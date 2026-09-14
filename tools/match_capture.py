#!/usr/bin/env python3
"""Match decrypted tshark PDML to application IDs. Presentation frames are not byte boundaries."""
import argparse
import hashlib
import json
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zlib
from collections import defaultdict
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit, parse_qs


def _json_payloads(raw):
    """Yield JSON message bytes from plain data or concatenated WebSocket frames."""
    if not raw:
        return
    try:
        text = raw.decode('utf-8')
        decoder = json.JSONDecoder()
        offset = 0
        found = False
        while offset < len(text):
            start = text.find('{', offset)
            if start < 0:
                break
            try:
                _, end = decoder.raw_decode(text, start)
            except ValueError:
                offset = start + 1
                continue
            found = True
            yield text[start:end].encode('utf-8')
            offset = end
        if found:
            return
    except UnicodeDecodeError:
        pass

    # Fallback for raw, concatenated WebSocket frames exposed as data.data.
    offset = 0
    while offset + 2 <= len(raw):
        first, second = raw[offset], raw[offset + 1]
        opcode = first & 0x0f
        if opcode not in (0, 1, 2, 8, 9, 10):
            break
        length = second & 0x7f
        header = 2
        if length == 126:
            if offset + 4 > len(raw): break
            length = int.from_bytes(raw[offset + 2:offset + 4], 'big'); header = 4
        elif length == 127:
            if offset + 10 > len(raw): break
            length = int.from_bytes(raw[offset + 2:offset + 10], 'big'); header = 10
        masked = bool(second & 0x80)
        if masked: header += 4
        end = offset + header + length
        if end > len(raw): break
        payload_start = offset + header
        payload = bytearray(raw[payload_start:end])
        if masked:
            key = raw[payload_start - 4:payload_start]
            for i in range(len(payload)): payload[i] ^= key[i % 4]
        if opcode in (1, 2, 0):
            candidate = bytes(payload)
            try:
                json.loads(candidate)
                yield candidate
            except (ValueError, UnicodeDecodeError):
                try:
                    inflated = zlib.decompress(candidate + b'\x00\x00\xff\xff', -15)
                    json.loads(inflated)
                    yield inflated
                except (zlib.error, ValueError, UnicodeDecodeError):
                    pass
        offset = end


def parse_capture(path, progress=False, progress_label=None):
    markets, orders = defaultdict(list), defaultdict(list)
    diagnostics = defaultdict(int)
    fragments = defaultdict(bytearray)
    direct_market_records = set()
    generic_market_records = set()
    from progress import ProgressFile
    label = progress_label or Path(path).name
    with ProgressFile(path, f"{label} parse", progress) as source:
      for _, packet in ET.iterparse(source, events=('end',)):
        if packet.tag != 'packet':
            continue
        fields = defaultdict(list)
        for field in packet.iter('field'):
            fields[field.get('name')].append(field)
        def show(name):
            return fields[name][0].get('show') if fields[name] else None
        observation = {'presentation_frame': show('frame.number'),
                       'presentation_time_epoch': show('frame.time_epoch'),
                       'tcp_stream': show('tcp.stream'),
                       'tcp_reassembly_frames': [f.get('show') for f in fields['tcp.segment']],
                       'tls_reassembly_frames': [f.get('show') for f in fields['tls.segment']]}
        websocket_fields = (fields['websocket.payload.text'] +
                            fields['websocket.payload.continue'] +
                            fields['websocket.payload.unknown'])
        # Some dissections expose only the generic payload field.  Use it as
        # a fallback, never alongside a more specific child field, to avoid
        # decoding the same frame twice.
        if not websocket_fields:
            websocket_fields = fields['websocket.payload']
        if websocket_fields:
            diagnostics['websocket_payload_fields'] += len(websocket_fields)
        if fields['websocket.payload.text']:
            diagnostics['websocket_text_frames'] += 1
        if fields['websocket.payload.continue']:
            diagnostics['websocket_continuation_frames'] += 1
        if fields['websocket.payload.unknown']:
            diagnostics['websocket_binary_or_unknown_frames'] += 1
        if fields['websocket.rsv'] and any(f.get('show', '0') not in ('0', '0x00') for f in fields['websocket.rsv']):
            diagnostics['websocket_reserved_bit_frames'] += 1

        # Prefer Wireshark's already reassembled text field.  If it is not
        # available, reconstruct a fragmented message from continuation
        # payloads in capture order for this TCP direction.
        payload_field = (fields['websocket.payload.text'] or
                         fields['websocket.payload.continue'] or
                         fields['websocket.payload.unknown'] or
                         fields['websocket.payload'])
        payload = None
        if payload_field:
            raw = payload_field[0].get('value')
            if raw:
                try:
                    fragment = bytes.fromhex(raw)
                except ValueError:
                    fragment = b''
                direction = (show('tcp.stream'), show('ip.src') or show('ipv6.src'),
                             show('tcp.srcport'), show('ip.dst') or show('ipv6.dst'),
                             show('tcp.dstport'))
                opcode = show('websocket.opcode')
                fin = show('websocket.fin') in ('1', 'True', 'true')
                if opcode in ('1', '0x1') and not fin:
                    fragments[direction] = bytearray(fragment)
                    diagnostics['websocket_fragment_starts'] += 1
                elif opcode in ('0', '0x0') and direction in fragments:
                    fragments[direction].extend(fragment)
                    diagnostics['websocket_fragment_continuations'] += 1
                    if fin:
                        payload = bytes(fragments.pop(direction))
                elif fields['websocket.payload.text']:
                    payload = fragment
                elif fin and direction in fragments:
                    fragments[direction].extend(fragment)
                    payload = bytes(fragments.pop(direction))
                elif fin:
                    payload = fragment
                if not fin and opcode not in ('1', '0x1'):
                    diagnostics['websocket_unfinished_messages'] += 1
        def record_market(payload, generic=False):
            if not payload:
                return
            digest = hashlib.sha256(payload).hexdigest()
            record_key = (observation['presentation_frame'], digest)
            if generic and (record_key in direct_market_records or record_key in generic_market_records):
                return
            try:
                body = json.loads(payload)
            except (ValueError, UnicodeDecodeError):
                diagnostics['websocket_json_decode_failures'] += 1
                return
            data = body.get('data', body) if isinstance(body, dict) else {}
            if isinstance(data, dict) and 'lastUpdateId' in data:
                (generic_market_records if generic else direct_market_records).add(record_key)
                diagnostics['decoded_market_messages'] += 1
                markets[digest].append(dict(observation,
                    stream=body.get('stream'), book_update_id=data['lastUpdateId']))
        record_market(payload)
        # A single dissected packet can contain multiple WebSocket text
        # payload fields.  They are independent messages and must all be
        # considered; the first one was handled above for fragment support.
        for field in fields['websocket.payload.text'][1:]:
            raw = field.get('value')
            if raw:
                try:
                    record_market(bytes.fromhex(raw))
                except ValueError:
                    diagnostics['websocket_json_decode_failures'] += 1
        # If Wireshark did not classify the decrypted bytes as WebSocket, try
        # generic data/reassembled fields as a conservative fallback.
        for name in ('data.data', 'tcp.reassembled.data'):
            for field in fields[name]:
                raw = field.get('value')
                if not raw:
                    continue
                try:
                    for candidate in _json_payloads(bytes.fromhex(raw)):
                        diagnostics['generic_payload_candidates'] += 1
                        record_market(candidate, generic=True)
                except ValueError:
                    diagnostics['generic_payload_decode_failures'] += 1
        for field in fields['http.request.uri']:
            uri = field.get('show', '')
            parsed = urlsplit(uri)
            if parsed.path != '/api/v3/order' or show('http.request.method') != 'POST':
                continue
            query = parse_qs(parsed.query)
            client = query.get('newClientOrderId', [None])[0]
            if client:
                orders[client].append(dict(observation,
                    symbol=query.get('symbol', [None])[0], side=query.get('side', [None])[0]))
        packet.clear()
    return markets, orders, dict(diagnostics)


def match(pdml, log, first_leg_only=False, progress=False, progress_label=None):
    markets, orders, diagnostics = parse_capture(pdml, progress, progress_label)
    identities, reports, report_lines = {}, {}, {}
    # Keep the small set of order latency records in the match result.  The
    # caller can then reuse this scan instead of parsing a large JSONL log a
    # second time.
    with Path(log).open() as source:
        for line_number, line in enumerate(source, 1):
            row = json.loads(line)
            f = row.get('fields', {})
            if row.get('event') == 'capture_market_identity':
                identities[f['receive_sequence']] = f['payload_sha256']
            if row.get('event') == 'execution_order_write':
                reports[f['client_id']] = f.get('latency', {})
                report_lines.setdefault(f['client_id'], line_number)
            elif row.get('event') == 'execution_order_latency':
                client = f['client_id']
                report_lines.setdefault(client, line_number)
                if client in reports:
                    write_latency = reports[client]
                    report_latency = f.get('latency', {})
                    write_latency.setdefault('market_received_realtime_ns', report_latency.get('market_received_realtime_ns'))
                    write_latency.setdefault('kernel_rx', report_latency.get('kernel_rx', {}))
                    write_latency.setdefault('transport', {}).setdefault('kernel_tx', report_latency.get('transport', {}).get('kernel_tx', {}))
                else:
                    reports[client] = f.get('latency', {})
    results = []
    decoded_hashes = set(markets)
    diagnostics['log_market_identity_count'] = len(identities)
    diagnostics['identity_hashes_present_in_capture'] = sum(
        1 for fingerprint in identities.values() if fingerprint in decoded_hashes
    )
    for client, latency in reports.items():
        if first_leg_only and latency.get('leg') != 0:
            continue
        sequence = latency.get('trigger_receive_sequence')
        market = markets.get(identities.get(sequence), [])
        order = orders.get(client, [])
        results.append({'client_id': client, 'cycle_id': latency.get('cycle_id'),
            'leg': latency.get('leg'), 'trigger_receive_sequence': sequence,
            'market_candidates': market, 'order_candidates': order,
            'latency': latency, 'log_line': report_lines.get(client),
            'match_status': 'unique' if len(market) == len(order) == 1 else 'missing_or_ambiguous',
            'exact_wire_latency_available': False})
    return {'boundary': 'tshark presentation frames; inspect TCP/TLS byte provenance for wire latency',
            'decoded_market_payloads': sum(map(len, markets.values())),
            'decoded_order_requests': sum(map(len, orders.values())),
            'market_decode_diagnostics': diagnostics, 'orders': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--pcap', type=Path)
    source.add_argument('--pdml', type=Path)
    parser.add_argument('--keylog', type=Path)
    parser.add_argument('--log', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--first-leg-only', action='store_true')
    parser.add_argument('--verify-boundaries', action='store_true')
    parser.add_argument('--progress', action='store_true')
    parser.add_argument('--progress-label', default=None, help='Label shown in progress output')
    parser.add_argument('--tls-port', action='append', type=int, default=[])
    args = parser.parse_args()
    if args.pcap and not args.keylog:
        parser.error('--pcap requires --keylog')
    with tempfile.TemporaryDirectory() as directory:
        pdml = args.pdml
        if args.pcap:
            pdml = Path(directory) / 'decoded.pdml'
            command = ['tshark', '-n', '-2', '-r', str(args.pcap),
                '-o', f'tls.keylog_file:{args.keylog.resolve()}',
                '-o', 'tcp.desegment_tcp_streams:TRUE',
                '-o', 'tcp.reassemble_out_of_order:TRUE',
                '-o', 'tls.desegment_ssl_records:TRUE',
                '-o', 'tls.desegment_ssl_application_data:TRUE']
            for port in sorted(set([443, 9443] + args.tls_port)):
                command += ['-d', f'tcp.port=={port},tls']
            with pdml.open('wb') as output:
                subprocess.run(command + ['-T', 'pdml'], stdout=output, check=True)
        result = match(pdml, args.log, args.first_leg_only, args.progress, args.progress_label)
        if args.verify_boundaries:
            from capture_boundaries import validate_matches
            validate_matches(pdml, result, args.progress, args.progress_label)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(f"decoded markets={result['decoded_market_payloads']}, orders={result['decoded_order_requests']}; "
          f"unique matches={sum(r['match_status'] == 'unique' for r in result['orders'])}")


if __name__ == '__main__':
    main()
