#!/usr/bin/env python3
"""Match decrypted tshark PDML to application IDs. Presentation frames are not byte boundaries."""
import argparse
import hashlib
import json
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit, parse_qs


def parse_capture(path):
    markets, orders = defaultdict(list), defaultdict(list)
    for _, packet in ET.iterparse(path, events=('end',)):
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
        for field in fields['websocket.payload.text']:
            # PDML value is raw bytes, unlike show which can escape/pretty-print text.
            raw = field.get('value')
            if not raw:
                continue
            try:
                payload = bytes.fromhex(raw)
                body = json.loads(payload)
            except (ValueError, UnicodeDecodeError):
                continue
            data = body.get('data', body) if isinstance(body, dict) else {}
            if isinstance(data, dict) and 'lastUpdateId' in data:
                markets[hashlib.sha256(payload).hexdigest()].append(dict(observation,
                    stream=body.get('stream'), book_update_id=data['lastUpdateId']))
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
    return markets, orders


def match(pdml, log, first_leg_only=False):
    markets, orders = parse_capture(pdml)
    identities, reports = {}, {}
    for line in Path(log).read_text().splitlines():
        row = json.loads(line)
        f = row.get('fields', {})
        if row.get('event') == 'capture_market_identity':
            identities[f['receive_sequence']] = f['payload_sha256']
        if row.get('event') == 'execution_order_write':
            reports[f['client_id']] = f.get('latency', {})
        elif row.get('event') == 'execution_order_latency':
            reports.setdefault(f['client_id'], f.get('latency', {}))
    results = []
    for client, latency in reports.items():
        if first_leg_only and latency.get('leg') != 0:
            continue
        sequence = latency.get('trigger_receive_sequence')
        market = markets.get(identities.get(sequence), [])
        order = orders.get(client, [])
        results.append({'client_id': client, 'cycle_id': latency.get('cycle_id'),
            'leg': latency.get('leg'), 'trigger_receive_sequence': sequence,
            'market_candidates': market, 'order_candidates': order,
            'match_status': 'unique' if len(market) == len(order) == 1 else 'missing_or_ambiguous',
            'exact_wire_latency_available': False})
    return {'boundary': 'tshark presentation frames; inspect TCP/TLS byte provenance for wire latency',
            'decoded_market_payloads': sum(map(len, markets.values())),
            'decoded_order_requests': sum(map(len, orders.values())), 'orders': results}


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
                '-o', 'tls.desegment_ssl_records:TRUE',
                '-o', 'tls.desegment_ssl_application_data:TRUE']
            for port in sorted(set([443, 9443] + args.tls_port)):
                command += ['-d', f'tcp.port=={port},tls']
            with pdml.open('wb') as output:
                subprocess.run(command + ['-T', 'pdml'], stdout=output, check=True)
        result = match(pdml, args.log, args.first_leg_only)
        if args.verify_boundaries:
            from capture_boundaries import validate_matches
            validate_matches(pdml, result)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(f"decoded markets={result['decoded_market_payloads']}, orders={result['decoded_order_requests']}; "
          f"unique matches={sum(r['match_status'] == 'unique' for r in result['orders'])}")


if __name__ == '__main__':
    main()
