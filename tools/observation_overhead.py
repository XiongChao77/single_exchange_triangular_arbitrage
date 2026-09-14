#!/usr/bin/env python3
"""Estimate capture A/B observation overhead from clean TCP data/ACK pairs."""
import hashlib
import json
import subprocess
from bisect import bisect_right
from collections import Counter, defaultdict
from decimal import Decimal


FIELDS = ['frame.number', 'frame.time_epoch', 'ip.src', 'ip.dst', 'tcp.srcport',
          'tcp.dstport', 'tcp.seq', 'tcp.ack', 'tcp.len', 'tcp.flags',
          'tcp.payload', 'tcp.analysis.retransmission', 'tcp.analysis.out_of_order',
          'tcp.analysis.lost_segment']


def read_packets(pcap):
    command = ['tshark', '-n', '-r', str(pcap), '-Y', 'tcp', '-T', 'fields', '-E', 'separator=\t',
               '-E', 'occurrence=f']
    for field in FIELDS:
        command += ['-e', field]
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    packets = []
    for line in output.splitlines():
        values = line.split('\t')
        if len(values) != len(FIELDS):
            continue
        d = dict(zip(FIELDS, values))
        try:
            d.update(frame=int(d['frame.number']), time=Decimal(d['frame.time_epoch']),
                     src=(d['ip.src'], d['tcp.srcport']), dst=(d['ip.dst'], d['tcp.dstport']),
                     seq=int(d['tcp.seq']), ack=int(d['tcp.ack']), length=int(d['tcp.len']),
                     flags=int(d['tcp.flags'], 16))
        except (ValueError, TypeError):
            continue
        d['payload_hash'] = hashlib.sha256(bytes.fromhex(d['tcp.payload']) if d['tcp.payload'] else b'').hexdigest()
        packets.append(d)
    return packets


def clean_samples(packets):
    by_direction = defaultdict(list)
    for p in packets:
        by_direction[(p['src'], p['dst'])].append(p)

    # Build the indexes once.  The previous implementation scanned the full
    # reverse direction for every data segment, which made large captures
    # quadratic.  The indexes preserve the original frame-order semantics.
    duplicate_segments = {}
    for direction, direction_packets in by_direction.items():
        counts = Counter((p['seq'], p['length']) for p in direction_packets)
        duplicate_segments[direction] = {key for key, count in counts.items() if count > 1}
    ack_index = {}
    ack_frame_index = {}
    data_frame_index = {}
    for direction, direction_packets in by_direction.items():
        ack_by_number = defaultdict(list)
        data_frames = []
        for packet in direction_packets:
            if packet['length'] == 0 and (packet['flags'] & 0x10) and not (packet['flags'] & 0x07):
                ack_by_number[packet['ack']].append(packet)
            if packet['length'] > 0:
                data_frames.append(packet['frame'])
        for entries in ack_by_number.values():
            entries.sort(key=lambda packet: packet['frame'])
        ack_index[direction] = ack_by_number
        ack_frame_index[direction] = {
            number: [packet['frame'] for packet in entries]
            for number, entries in ack_by_number.items()
        }
        data_frame_index[direction] = sorted(data_frames)

    samples = []
    for direction, data_packets in by_direction.items():
        if direction[0][1] == '0' or direction[1][1] == '0':
            continue
        for index, data in enumerate(data_packets):
            if data['length'] <= 0 or data['tcp.analysis.retransmission'] or data['tcp.analysis.out_of_order'] or data['tcp.analysis.lost_segment']:
                continue
            # Require the next packet in this direction to be the same segment (no retransmission).
            if (data['seq'], data['length']) in duplicate_segments[direction]:
                continue
            # A clean ACK is pure TCP ACK, confirms exactly this segment, and follows it.
            reverse_direction = (data['dst'], data['src'])
            ack_number = data['seq'] + data['length']
            candidates = ack_index.get(reverse_direction, {}).get(ack_number, [])
            ack_frames = ack_frame_index.get(reverse_direction, {}).get(ack_number, [])
            first = bisect_right(ack_frames, data['frame'])
            if first == len(candidates):
                continue
            ack = candidates[first]
            reverse_data_frames = data_frame_index.get((data['dst'], data['src']), [])
            next_data = bisect_right(reverse_data_frames, data['frame'])
            if next_data < len(reverse_data_frames) and reverse_data_frames[next_data] < ack['frame']:
                continue
            samples.append({'key': (data['src'], data['dst'], data['seq'], data['length'], data['payload_hash']),
                            'data_frame': data['frame'], 'ack_frame': ack['frame'],
                            'data_time': data['time'], 'ack_time': ack['time'],
                            'delay_us': (ack['time']-data['time'])*Decimal(1_000_000)})
    return samples


def summarize(local_pcap, external_pcap):
    local = {s['key']: s for s in clean_samples(read_packets(local_pcap))}
    external = {s['key']: s for s in clean_samples(read_packets(external_pcap))}
    values = []
    for key in local.keys() & external.keys():
        # A and B are not assumed to share clock origin; compare per-segment ACK turnaround.
        value = external[key]['delay_us'] - local[key]['delay_us']
        if value >= 0:
            values.append(value)
    values.sort()
    def quantile(p):
        if not values:
            return None
        index = max(0, min(len(values)-1, int((len(values)*p + 0.999999999999) - 1)))
        return float(values[index])
    return {'method': 'clean_inbound_tcp_segment_to_exact_pure_ack',
            'samples_local': len(local), 'samples_external': len(external),
            'paired_samples': len(values), 'excluded_nonpaired_or_invalid':
            len(local.keys() ^ external.keys()),
            'units': 'microseconds',
            'p50': quantile(.50), 'p95': quantile(.95), 'p99': quantile(.99),
            'minimum': float(values[0]) if values else None,
            'maximum': float(values[-1]) if values else None}
