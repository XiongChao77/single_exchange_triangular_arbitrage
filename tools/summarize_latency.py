#!/usr/bin/env python3
"""Summarize local latency events. RX batch observations are not exact message timestamps."""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


def summarize(path):
    metrics = defaultdict(list)
    counts = defaultdict(int)
    callback_times = {}
    for line in Path(path).open():
        row = json.loads(line)
        if row.get('event') == 'market_latency':
            counts['market_messages'] += 1
            window = row['fields']['kernel_rx']
            if window.get('no_new_transport_read'):
                counts['rx_read_ahead_messages'] += 1
            if any(b.get('kernel_rx_realtime_ns') for b in window.get('batches', [])):
                counts['messages_with_rx_batch_observation'] += 1
            continue
        if row.get('event') == 'execution_order_report':
            fields = row['fields']
            start = callback_times.get(fields.get('client_id'))
            end = fields.get('report_received_ns')
            if start and end and end >= start:
                metrics['callback_to_executor_report'].append((end-start)/1000)
            continue
        if row.get('event') != 'execution_order_latency':
            continue
        counts['order_reports'] += 1
        fields = row['fields']
        latency = fields['latency']
        callback_times[fields['client_id']] = latency.get('callback_received_ns')
        transport = latency.get('transport', {})
        points = transport.get('timepoints', {})
        tx = transport.get('kernel_tx', {})
        if latency.get('leg') == 0:
            counts['first_leg_reports'] += 1
        if tx.get('final_byte_timestamp_present'):
            counts['orders_with_final_tx_software'] += 1
        elif tx.get('enabled'):
            counts['orders_missing_final_tx_software'] += 1

        def delta(name, end, start):
            if end and start and end >= start:
                metrics[name].append((end - start) / 1000)

        if latency.get('leg') == 0:
            delta('receiver_queue', latency.get('market_processed_ns'), latency.get('market_received_ns'))
            delta('parse_update_scan', latency.get('edge_found_ns'), latency.get('market_processed_ns'))
        delta('gateway_queue', latency.get('worker_begin_ns'), latency.get('gateway_enqueued_ns'))
        delta('sign', latency.get('sign_end_ns'), latency.get('sign_begin_ns'))
        delta('worker_build_before_sign', latency.get('sign_begin_ns'), latency.get('worker_begin_ns'))
        delta('signed_to_request', points.get('request_begin_ns'), latency.get('sign_end_ns'))
        if latency.get('leg') == 0:
            delta('decision_preflight_sizing', latency.get('order_prepared_ns'), latency.get('cycle_started_ns'))
            delta('edge_log_admission', latency.get('cycle_started_ns'), latency.get('edge_found_ns'))
        delta('prepared_to_gateway_submit', latency.get('gateway_submit_ns'), latency.get('order_prepared_ns'))
        delta('response_parse_accounting', latency.get('report_ready_ns'), points.get('http_response_ns'))
        delta('request_prepare_connect', points.get('http_write_begin_ns'), points.get('request_begin_ns'))
        delta('tls_write', points.get('http_write_end_ns'), points.get('http_write_begin_ns'))
        delta('response_wait', points.get('http_response_ns'), points.get('http_write_end_ns'))
        delta('callback_queue', latency.get('callback_received_ns'), latency.get('report_ready_ns'))
        if latency.get('leg') == 0:
            delta('edge_to_gateway_submit', latency.get('gateway_submit_ns'), latency.get('edge_found_ns'))
            delta('user_market_to_http_write_end', points.get('http_write_end_ns'), latency.get('market_received_ns'))
        else:
            delta('previous_report_to_gateway_submit', latency.get('gateway_submit_ns'), latency.get('previous_report_ns'))

        # Software kernel timestamps use CLOCK_REALTIME. Check conversion consistency
        # before comparing them with userspace markers; never subtract clock domains directly.
        rx_real, rx_mono = latency.get('market_received_realtime_ns'), latency.get('market_received_ns')
        write_real, write_mono = points.get('http_write_begin_realtime_ns'), points.get('http_write_begin_ns')
        if all((rx_real, rx_mono, write_real, write_mono)):
            if abs((rx_real-rx_mono)-(write_real-write_mono)) > 1_000_000:
                counts['clock_inconsistent_orders'] += 1
                continue
            tx_software = tx.get('kernel_tx_software_realtime_ns')
            delta('send_begin_to_tx_sched', tx.get('kernel_tx_sched_realtime_ns'), write_real)
            delta('tx_sched_to_tx_software', tx_software, tx.get('kernel_tx_sched_realtime_ns'))
            if latency.get('leg') == 0:
                delta('user_market_to_tx_software_clock_checked', tx_software, rx_real)
                window = latency.get('kernel_rx', {})
                stamps = [b['kernel_rx_realtime_ns'] for b in window.get('batches', []) if b.get('kernel_rx_realtime_ns')]
                if stamps and not window.get('history_truncated'):
                    delta('observed_latest_rx_batch_to_tx_software', tx_software, max(stamps))
                    delta('observed_earliest_rx_batch_to_tx_software', tx_software, min(stamps))

    def percentile(values, fraction):
        values = sorted(values)
        return values[max(0, math.ceil(len(values)*fraction)-1)]

    lines = [f'Latency sample: `{Path(path).name}`', '',
             'All durations are microseconds. RX batch observations are not exact per-message end-to-end latency.', '',
             '| Metric | Samples | p50 | p95 | p99 | Max |', '|---|---:|---:|---:|---:|---:|']
    for name, values in sorted(metrics.items()):
        lines.append(f'| {name} | {len(values)} | {percentile(values,.5):.3f} | {percentile(values,.95):.3f} | {percentile(values,.99):.3f} | {max(values):.3f} |')
    lines.extend(['', 'Coverage: ' + ', '.join(f'{key}={value}' for key,value in sorted(counts.items())), ''])
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log')
    parser.add_argument('--append-report', help='Append a result to a Markdown report')
    args = parser.parse_args()
    result = summarize(args.log)
    print(result)
    if args.append_report:
        with Path(args.append_report).open('a') as report:
            report.write('\n\n' + result)
