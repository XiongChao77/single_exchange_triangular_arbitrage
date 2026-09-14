"""Conservative PDML TCP/TLS boundary proof. Never exports payloads or secrets."""
from collections import defaultdict
from decimal import Decimal
from bisect import bisect_left, bisect_right
import xml.etree.ElementTree as ET


def fields(node, name):
    return [f for f in node.iter('field') if f.get('name') == name]


def value(node, name):
    fs = fields(node, name)
    return fs[0].get('show') if len(fs) == 1 else None


def raw(field):
    return bytes.fromhex(field.get('value', ''))


def coverage(packets, start, data, presorted=False):
    """First-observed coverage of [start,end); detect conflicting overlaps, not just flags."""
    end = start + len(data)
    seen = bytearray(len(data))
    contributing = []
    complete = None
    last_time = None
    ordered = packets if presorted else sorted(packets, key=lambda p: p['frame'])
    for p in ordered:
        lo, hi = max(start, p['seq']), min(end, p['seq'] + len(p['data']))
        if lo >= hi:
            continue
        now = Decimal(p['time'])
        if last_time is not None and now < last_time:
            raise ValueError('non_monotonic_capture_timestamps')
        last_time = now
        if p['data'][lo-p['seq']:hi-p['seq']] != data[lo-start:hi-start]:
            raise ValueError('conflicting_tcp_overlap')
        if p['truncated']:
            raise ValueError('truncated_capture_frame')
        if 0 in seen[lo-start:hi-start]:
            seen[lo-start:hi-start] = b'\x01' * (hi-lo)
            contributing.append({'frame': p['frame'], 'tcp_seq_start': lo,
                'tcp_seq_end_exclusive': hi, 'time_epoch': p['time']})
            if complete is None and all(seen):
                complete = p
    if complete is None:
        raise ValueError('missing_tcp_bytes')
    return {'tcp_seq_start': start, 'tcp_seq_end_exclusive': end,
        'record_bytes': len(data), 'contributing_frames': contributing,
        'completion_frame': complete['frame'], 'completion_time_epoch': complete['time']}


class BoundaryIndex:
    def __init__(self, path, target_frames, progress=False, progress_label=None):
        self.packets = {}
        self.streams = defaultdict(list)
        self.sequence_index = {}
        self.targets = {}
        # Keep XML only for business targets and TLS reassembly dependencies.
        self.path = path
        needed = set(map(int, target_frames))
        from progress import ProgressFile
        label = progress_label or path.name
        with ProgressFile(path, f"{label} index", progress) as source:
          for _, packet in ET.iterparse(source, events=('end',)):
            if packet.tag != 'packet':
                continue
            number = value(packet, 'frame.number')
            if number is None:
                packet.clear()
                continue
            number = int(number)
            payload = fields(packet, 'tcp.payload')
            if len(payload) == 1:
                try:
                    direction = (value(packet, 'tcp.stream'), value(packet, 'ip.src') or value(packet, 'ipv6.src'),
                        value(packet, 'tcp.srcport'), value(packet, 'ip.dst') or value(packet, 'ipv6.dst'), value(packet, 'tcp.dstport'))
                    if any(x is None for x in direction):
                        raise ValueError('missing_direction')
                    p = {'frame': number, 'seq': int(value(packet, 'tcp.seq')),
                        'time': value(packet, 'frame.time_epoch'), 'data': raw(payload[0]),
                        'payload_pos': int(payload[0].get('pos')), 'direction': direction,
                        'truncated': value(packet, 'frame.cap_len') != value(packet, 'frame.len')}
                    self.packets[number] = p
                    self.streams[direction].append(p)
                except (ValueError, TypeError):
                    pass
            if number in needed:
                self.targets[number] = ET.fromstring(ET.tostring(packet))
            packet.clear()
        # Dependencies may precede a target, so load those in a second streaming pass.
        # Build a sequence interval index once.  Boundary verification can then
        # inspect only segments overlapping the target TLS record instead of
        # rescanning every earlier segment in the TCP stream.
        for direction, packets in self.streams.items():
            by_sequence = sorted(packets, key=lambda p: (p['seq'], p['frame']))
            prefix_end = []
            highest_end = 0
            for packet in by_sequence:
                highest_end = max(highest_end, packet['seq'] + len(packet['data']))
                prefix_end.append(highest_end)
            self.sequence_index[direction] = (by_sequence, [q['seq'] for q in by_sequence], prefix_end)

        deps = {int(f.get('show')) for packet in self.targets.values()
                for f in fields(packet, 'tls.segment') if f.get('show', '').isdigit()}
        missing = deps - self.targets.keys()
        if missing:
            with ProgressFile(path, f"{label} deps", progress) as source:
              for _, packet in ET.iterparse(source, events=('end',)):
                if packet.tag != 'packet':
                    continue
                n = value(packet, 'frame.number')
                if n and int(n) in missing:
                    self.targets[int(n)] = ET.fromstring(ET.tostring(packet))
                packet.clear()

    def record(self, frame):
        packet = self.targets.get(frame)
        if packet is None or frame not in self.packets:
            raise ValueError('missing_frame_metadata')
        records = fields(packet, 'tls.record')
        if len(records) != 1:
            raise ValueError('multiple_or_missing_tls_records')
        record = records[0]
        if len(fields(record, 'tls.app_data')) != 1:
            raise ValueError('not_tls_application_data')
        p = self.packets[frame]
        size = int(value(record, 'tls.record.length')) + 5
        assembled = fields(packet, 'tcp.reassembled.data')
        if assembled:
            if len(assembled) != 1 or int(record.get('pos')) != 0:
                raise ValueError('ambiguous_reassembly_buffer')
            data = raw(assembled[0])
            if len(data) != size:
                raise ValueError('record_not_entire_reassembly_buffer')
            starts = set()
            segments = fields(packet, 'tcp.segment')
            if not segments:
                raise ValueError('missing_segment_provenance')
            for segment in segments:
                original = self.packets.get(int(segment.get('show')))
                fragment = raw(segment)
                offset = int(segment.get('pos'))
                if original is None or original['direction'] != p['direction'] or not fragment:
                    raise ValueError('missing_or_wrong_direction_segment')
                location = original['data'].find(fragment)
                if location < 0 or original['data'].find(fragment, location+1) >= 0:
                    raise ValueError('ambiguous_segment_bytes')
                if data[offset:offset+len(fragment)] != fragment:
                    raise ValueError('segment_reassembly_mismatch')
                starts.add(original['seq'] + location - offset)
            if len(starts) != 1:
                raise ValueError('inconsistent_tcp_sequence_mapping')
            start = starts.pop()
        else:
            offset = int(record.get('pos')) - p['payload_pos']
            if offset < 0 or offset+size > len(p['data']):
                raise ValueError('record_not_contained_in_packet')
            data = p['data'][offset:offset+size]
            start = p['seq']+offset
        if len(data) != size or data[0] != 23 or data[1:3] != b'\x03\x03' or int.from_bytes(data[3:5], 'big')+5 != size:
            raise ValueError('tls_header_mismatch')
        if raw(fields(record, 'tls.app_data')[0]) != data[5:]:
            raise ValueError('tls_ciphertext_mismatch')
        by_sequence, starts, prefix_end = self.sequence_index.get(p['direction'], ([], [], []))
        end = start + len(data)
        # prefix_end is monotonic, so this finds the first sequence-indexed
        # segment whose byte range can reach the record.  The upper bound
        # excludes segments beginning at or after the record's end.
        first = bisect_right(prefix_end, start)
        last = bisect_left(starts, end)
        candidates = [q for q in by_sequence[first:last] if q['frame'] <= frame]
        candidates.sort(key=lambda q: q['frame'])
        proof = coverage(candidates, start, data, presorted=True)
        proof.update(tcp_stream=p['direction'][0], source=p['direction'][1:3], destination=p['direction'][3:],
            presentation_frame=frame)
        return proof

    def verify(self, observation, kind):
        try:
            frame = int(observation['presentation_frame'])
            packet = self.targets.get(frame)
            if packet is None:
                raise ValueError('missing_presentation_frame')
            if kind == 'market':
                # A fragmented/compressed WS message needs a more detailed plaintext mapping.
                for proto in packet.iter('proto'):
                    if proto.get('name') != 'websocket':
                        continue
                    if value(proto, 'websocket.fin') not in ('True', '1') or value(proto, 'websocket.opcode') not in ('1',):
                        raise ValueError('unsupported_websocket_fragment_or_opcode')
                    for flag in ('websocket.rsv', 'websocket.rsv1', 'websocket.rsv2', 'websocket.rsv3'):
                        if value(proto, flag) not in (None, '0', 'False', '0x00'):
                            raise ValueError('unsupported_websocket_compression')
                if not fields(packet, 'websocket.payload.text'):
                    raise ValueError('missing_decoded_websocket')
            else:
                if len(fields(packet, 'http.request.method')) != 1:
                    raise ValueError('ambiguous_http_request')
                # Support bodyless requests only; don't mistake a decoded header for completion.
                if fields(packet, 'http.transfer_encoding') or value(packet, 'http.content_length') not in (None, '0'):
                    raise ValueError('http_body_boundary_not_supported')
                if not fields(packet, 'http.request.version'):
                    raise ValueError('incomplete_http_request')
            current = self.record(frame)
            dependencies = []
            for dep in sorted({int(f.get('show')) for f in fields(packet, 'tls.segment')} - {frame}):
                proof = self.record(dep)
                if proof['tcp_stream'] != current['tcp_stream'] or proof['source'] != current['source']:
                    raise ValueError('wrong_direction_tls_dependency')
                if Decimal(proof['completion_time_epoch']) > Decimal(current['completion_time_epoch']):
                    raise ValueError('late_tls_dependency')
                dependencies.append(proof)
            return {'status': 'verified', 'scope': 'capture_packet_tls_record_completion',
                'completion_frame': current['completion_frame'],
                'completion_time_epoch': current['completion_time_epoch'],
                'record': current, 'earlier_tls_dependencies': dependencies,
                'message_mapping': 'single_record_dissection_with_earlier_dependencies',
                'resolution': 'packet_timestamp; shared TLS records share completion boundary'}
        except (ValueError, TypeError, KeyError, IndexError) as error:
            return {'status': 'unverified', 'reason': str(error)}


def validate_matches(path, matches, progress=False, progress_label=None):
    frames = {c['presentation_frame'] for row in matches['orders']
              for key in ('market_candidates', 'order_candidates') for c in row[key]}
    index = BoundaryIndex(path, frames, progress, progress_label)
    for row in matches['orders']:
        row['boundary_validation'] = 'unverified'
        row['verified_capture_latency_us'] = None
        if row['match_status'] != 'unique':
            row['boundary_reason'] = 'missing_or_ambiguous_business_match'
            continue
        rx = index.verify(row['market_candidates'][0], 'market')
        tx = index.verify(row['order_candidates'][0], 'order')
        row['market_boundary'], row['order_boundary'] = rx, tx
        if rx['status'] == tx['status'] == 'verified':
            gap = (Decimal(tx['completion_time_epoch'])-Decimal(rx['completion_time_epoch']))*1_000_000
            if gap >= 0:
                row['boundary_validation'] = 'verified'
                row['verified_capture_latency_us'] = float(gap)
            else:
                row['boundary_reason'] = 'negative_capture_latency'
