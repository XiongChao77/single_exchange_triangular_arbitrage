#!/usr/bin/env python3
"""Estimate capture A/B observation overhead from clean TCP data/ACK pairs."""
import hashlib
import json
import subprocess
from collections import defaultdict
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
    samples = []
    for direction, data_packets in by_direction.items():
        if direction[0][1] == '0' or direction[1][1] == '0':
            continue
        for index, data in enumerate(data_packets):
            if data['length'] <= 0 or data['tcp.analysis.retransmission'] or data['tcp.analysis.out_of_order'] or data['tcp.analysis.lost_segment']:
                continue
            # Require the next packet in this direction to be the same segment (no retransmission).
            if any(q['seq'] == data['seq'] and q['length'] == data['length']
                   and q['frame'] != data['frame'] for q in data_packets):
                continue
            reverse = by_direction.get((data['dst'], data['src']), [])
            # A clean ACK is pure TCP ACK, confirms exactly this segment, and follows it.
            candidates = [ack for ack in reverse if ack['frame'] > data['frame'] and
                          ack['length'] == 0 and ack['ack'] == data['seq'] + data['length'] and
                          (ack['flags'] & 0x10) and not (ack['flags'] & 0x07)]
            if not candidates:
                continue
            ack = min(candidates, key=lambda q: q['frame'])
            if any(q['frame'] > data['frame'] and q['frame'] < ack['frame'] and q['length'] > 0
                   for q in reverse):
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
