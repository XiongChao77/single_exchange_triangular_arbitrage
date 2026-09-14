#!/usr/bin/env python3
"""First-leg submission latency; ends at request write, before any response."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

from observation_overhead import summarize as summarize_observation_overhead

STAGES = [
    ("receiver_wait", "market_received_ns", "market_processed_ns"),
    ("parse_update_scan", "market_processed_ns", "edge_found_ns"),
    ("edge_to_cycle", "edge_found_ns", "cycle_started_ns"),
    ("first_leg_prepare", "cycle_started_ns", "order_prepared_ns"),
    ("prepare_state_init", "cycle_started_ns", "cycle_post_begin_ns"),
    ("prepare_post_wait", "cycle_post_begin_ns", "cycle_begin_ns"),
    ("prepare_begin_guard", "cycle_begin_ns", "balance_read_begin_ns"),
    ("prepare_balance_read", "balance_read_begin_ns", "balance_read_end_ns"),
    ("prepare_balance_check_setup", "balance_read_end_ns", "snapshot_begin_ns"),
    ("prepare_snapshot", "snapshot_begin_ns", "snapshot_end_ns"),
    ("prepare_preflight_checks", "snapshot_end_ns", "preflight_end_ns"),
    ("prepare_next_leg", "preflight_end_ns", "first_order_begin_ns"),
    ("prepare_order_build", "first_order_begin_ns", "first_order_end_ns"),
    ("prepare_metadata", "first_order_end_ns", "order_prepared_ns"),
    ("prepared_to_enqueue", "order_prepared_ns", "gateway_enqueued_ns"),
    ("gateway_queue", "gateway_enqueued_ns", "worker_begin_ns"),
    ("worker_before_sign", "worker_begin_ns", "sign_begin_ns"),
    ("sign", "sign_begin_ns", "sign_end_ns"),
    ("signed_to_write", "sign_end_ns", "http_write_begin_ns"),
    ("http_write", "http_write_begin_ns", "http_write_end_ns"),
    ("user_trigger_to_write_end", "market_received_ns", "http_write_end_ns"),
]


def analyze(log, matches):
    reports = {}
    # New match files carry the latency records collected by match_capture,
    # avoiding a second full pass over a potentially multi-gigabyte JSONL log.
    # Keep the fallback for older match files and direct callers.
    embedded = any("latency" in row for row in matches.get("orders", []))
    if not embedded:
        with Path(log).open() as source:
            for number, line in enumerate(source, 1):
                row = json.loads(line)
                if row.get("event") == "execution_order_write":
                    reports[row["fields"]["client_id"]] = (number, row["fields"])
                elif row.get("event") == "execution_order_latency":
                    client = row["fields"]["client_id"]
                    if client in reports:
                        write_fields = reports[client][1]
                        write_latency = write_fields.setdefault("latency", {})
                        report_latency = row["fields"].get("latency", {})
                        write_latency.setdefault("market_received_realtime_ns", report_latency.get("market_received_realtime_ns"))
                        write_latency.setdefault("kernel_rx", report_latency.get("kernel_rx", {}))
                        write_latency.setdefault("transport", {}).setdefault("kernel_tx", report_latency.get("transport", {}).get("kernel_tx", {}))
                    else:
                        reports[client] = (number, row["fields"])
    result = []
    for match in matches["orders"]:
        if embedded:
            number = match.get("log_line")
            latency = match.get("latency", {})
        else:
            number, fields = reports[match["client_id"]]
            latency = fields.get("latency", {})
        if latency.get("leg") != 0:
            continue
        points = dict(latency)
        points.update(latency.get("transport", {}).get("timepoints", {}))
        durations, issues = {}, []
        for name, start, end in STAGES:
            a, b = points.get(start), points.get(end)
            if not isinstance(a, int) or not isinstance(b, int) or a <= 0 or b <= 0:
                durations[name] = None
            elif b < a:
                durations[name] = None
                issues.append("non_monotonic:" + name)
            else:
                durations[name] = (b - a) / 1000
        gap = None
        if match["match_status"] == "unique":
            a = match["market_candidates"][0].get("presentation_time_epoch")
            b = match["order_candidates"][0].get("presentation_time_epoch")
            if a and b:
                gap = float((Decimal(b) - Decimal(a)) * 1_000_000)
                if gap < 0:
                    issues.append("negative_capture_gap")
                    gap = None
        residual = None
        if gap is not None and durations.get("user_trigger_to_write_end") is not None:
            residual = gap - durations["user_trigger_to_write_end"]
        transport_breakdown = {
            "kernel_rx_to_tx_software_us": None,
            "app_rx_realtime_to_tx_software_us": None,
            "app_write_end_realtime_to_tx_software_us": None,
            "rx_mapping": "unavailable",
        }
        tx = latency.get("transport", {}).get("kernel_tx", {})
        tx_time = tx.get("kernel_tx_software_realtime_ns")
        if isinstance(tx_time, int):
            app_rx = latency.get("market_received_realtime_ns")
            write_end = latency.get("transport", {}).get("timepoints", {}).get("http_write_end_realtime_ns")
            if isinstance(app_rx, int):
                transport_breakdown["app_rx_realtime_to_tx_software_us"] = (tx_time - app_rx) / 1000
            if isinstance(write_end, int):
                transport_breakdown["app_write_end_realtime_to_tx_software_us"] = (tx_time - write_end) / 1000
            window = latency.get("kernel_rx", {})
            stamps = [b.get("kernel_rx_realtime_ns") for b in window.get("batches", []) if isinstance(b.get("kernel_rx_realtime_ns"), int)]
            if stamps and not window.get("history_truncated") and not window.get("exact_message_mapping", True):
                transport_breakdown["kernel_rx_to_tx_software_us"] = (tx_time - max(stamps)) / 1000
                transport_breakdown["rx_mapping"] = "latest_read_batch_upper_bound"
            elif stamps and not window.get("history_truncated"):
                transport_breakdown["kernel_rx_to_tx_software_us"] = (tx_time - max(stamps)) / 1000
                transport_breakdown["rx_mapping"] = "message_mapped"
        result.append(
            dict(
                match,
                log_line=number,
                durations_us=durations,
                presentation_gap_us=gap,
                unassigned_boundary_difference_us=residual,
                transport_breakdown=transport_breakdown,
                issues=issues,
                boundary_validation=match.get("boundary_validation", "not_automatically_verified"),
            )
        )
    return {
        "units": "microseconds",
        "scope": "first_leg_submission_only",
        "first_leg_samples": len(result),
        "notes": [
            "verified_capture_latency_us uses proven TLS-record packet boundaries; presentation_gap_us remains an estimate.",
            "Boundary differences are unassigned; do not interpret as network-stack latency.",
            "No absolute timestamps are subtracted across capture and host clocks.",
            "Only leg=0 is analyzed, ending at HTTP request write / capture TX record completion.",
            "When --local-pcap is supplied, local_verified_capture_latency_us is from the trading-host capture.",
        ],
        "decoded_market_payloads": matches["decoded_market_payloads"],
        "decoded_order_requests": matches["decoded_order_requests"],
        "market_decode_diagnostics": matches.get("market_decode_diagnostics", {}),
        "orders": result,
    }


def render(result, log):
    lines = [
        "# First-Leg Order Submission Latency",
        "",
        "Unit: μs. Verified latency is emitted only when complete TLS-record packet boundaries are proven.",
        "",
        "Latency decomposition:",
        "",
        "`external capture = A/B capture-boundary overhead + on-host capture`",
        "",
        "`on-host capture = application processing + local residual`",
        "",
        "## End-to-End Decomposition",
        "",
        "| client_id | Match | Market frame → order frame | External capture | A/B boundary overhead | On-host capture | Application | Local residual |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]

    def fmt(value):
        return "N/A" if value is None else f"{value:.3f}"

    for row in result["orders"]:
        frames = "N/A"
        if row["match_status"] == "unique":
            frames = row["market_candidates"][0]["presentation_frame"] + " → " + row["order_candidates"][0]["presentation_frame"]
        local_gap = row.get("local_verified_capture_latency_us")
        app_gap = row["durations_us"].get("user_trigger_to_write_end")
        external_gap = row.get("verified_capture_latency_us")
        boundary_overhead = external_gap - local_gap if external_gap is not None and local_gap is not None else None
        local_residual = local_gap - app_gap if local_gap is not None and app_gap is not None else None
        lines.append(
            f"| {row['client_id']} | {row['match_status']} | {frames} | "
            f"{fmt(external_gap)} | {fmt(boundary_overhead)} | {fmt(local_gap)} | "
            f"{fmt(app_gap)} | {fmt(local_residual)} |"
        )
    lines += [
        "",
        "The A/B boundary overhead is `external capture - on-host capture`. "
        "It covers the two capture-point boundaries and the bidirectional A–B path; "
        "it is not application latency.",
        "",
        "The local residual is `on-host capture - application processing`. " "It includes uninstrumented host-side boundary work and timestamp uncertainty.",
        "",
        "## Observable Host-Side Boundary Breakdown",
        "",
        "| client_id | Latest kernel RX → TX software time | Application RX → TX software time | Application write end → TX software time | RX mapping |",
        "|---|---:|---:|---:|---|",
    ]
    for row in result["orders"]:
        b = row["transport_breakdown"]
        lines.append(
            f"| {row['client_id']} | {fmt(b['kernel_rx_to_tx_software_us'])} | {fmt(b['app_rx_realtime_to_tx_software_us'])} | {fmt(b['app_write_end_realtime_to_tx_software_us'])} | {b['rx_mapping']} |"
        )
    lines += [
        "",
        "## TCP/TLS Boundary Verification",
        "",
        "| client_id | Verification | Record-complete RX frame → TX frame | Verified latency | Reason |",
        "|---|---|---|---:|---|",
    ]
    for row in result["orders"]:
        rx, tx = row.get("market_boundary", {}), row.get("order_boundary", {})
        reason = row.get("boundary_reason") or "; ".join(b.get("reason", "") for b in (rx, tx) if b.get("reason"))
        lines.append(
            f"| {row['client_id']} | {row['boundary_validation']} | {rx.get('completion_frame', 'N/A')} → {tx.get('completion_frame', 'N/A')} | {fmt(row.get('verified_capture_latency_us'))} | {reason} |"
        )
    lines += [
        "",
        "Residual values must not be interpreted directly as network-stack latency. "
        "Only the first leg is analyzed, ending when the complete order request is submitted.",
        "",
        "## Per-Order Application Breakdown",
        "",
    ]
    lines += ["| Stage | " + " | ".join(row["client_id"] for row in result["orders"]) + " |", "|---|" + "---:|" * len(result["orders"])]
    for name, _, _ in STAGES:
        lines.append("| " + name + " | " + " | ".join(fmt(row["durations_us"].get(name)) for row in result["orders"]) + " |")
    lines += [
        "",
        "## Application Stage Summary",
        "",
        "| Stage | Samples | Minimum | P50 | P95 | P99 | Maximum |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, _, _ in STAGES:
        values = sorted(row["durations_us"][name] for row in result["orders"] if row["durations_us"].get(name) is not None)
        if values:
            percentile = lambda p: values[max(0, math.ceil(len(values) * p) - 1)]
            p50 = percentile(0.5) if len(values) >= 2 else None
            p95 = percentile(0.95) if len(values) >= 20 else None
            p99 = percentile(0.99) if len(values) >= 100 else None
            lines.append(f"| {name} | {len(values)} | {fmt(values[0])} | {fmt(p50)} | " f"{fmt(p95)} | {fmt(p99)} | {fmt(values[-1])} |")
    lines += [
        "",
        "P95 is shown with at least 20 samples and P99 with at least 100 samples.",
    ]
    overhead = result.get("observation_overhead")
    if overhead:
        lines += [
            "",
            "## A/B Capture-Boundary Overhead Baseline",
            "",
            "A is the on-host capture and B is the external capture. Each sample uses one inbound TCP data segment followed by one unambiguous pure ACK.",
            "",
            "| A samples | B samples | Paired samples | P50 | P95 | P99 | Minimum | Maximum |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        lines.append(
            "| {samples_local} | {samples_external} | {paired_samples} | {p50} | {p95} | {p99} | {minimum} | {maximum} |".format(
                samples_local=overhead["samples_local"],
                samples_external=overhead["samples_external"],
                paired_samples=overhead["paired_samples"],
                **{k: fmt(overhead[k]) for k in ("p50", "p95", "p99", "minimum", "maximum")},
            )
        )
        lines += [
            "",
            "Compare this baseline with the per-order `A/B boundary overhead` column, " "not with the total external residual. For each order:",
            "",
            "`external residual = A/B boundary overhead + local residual`",
        ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--pcap", required=True, type=Path)
    parser.add_argument("--local-pcap", type=Path, help="Optional pcap captured on the trading host")
    parser.add_argument("--keys", "--keylog", dest="keys", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="JSON output; Markdown written alongside it")
    parser.add_argument("--tls-port", action="append", type=int, default=[])
    parser.add_argument("--progress", action="store_true", help="Show capture parsing progress and ETA")
    args = parser.parse_args()
    inputs = [args.log, args.pcap, args.keys] + ([args.local_pcap] if args.local_pcap else [])
    for path in inputs:
        if not path.is_file():
            parser.error(f"Input file not found: {path}")
    if args.output.suffix != ".json":
        parser.error("--output must end with .json")
    markdown = args.output.with_suffix(".md")
    if any(p.resolve() in [q.resolve() for q in inputs] for p in [args.output, markdown]):
        parser.error("Output must not overwrite an input")

    def run_match_capture(pcap, output, progress_label):
        command = [
            sys.executable,
            str(Path(__file__).with_name("match_capture.py")),
            "--verify-boundaries",
            "--first-leg-only",
            "--log",
            str(args.log),
            "--pcap",
            str(pcap),
            "--keylog",
            str(args.keys),
            "--output",
            str(output),
        ]
        for port in args.tls_port:
            command += ["--tls-port", str(port)]
        if args.progress:
            command += ["--progress"]
            command += ["--progress-label", progress_label]
            print(f"[capture] decoding {pcap} ...", file=sys.stderr, flush=True)
        started = time.monotonic()
        completed = subprocess.run(command, check=True)
        if args.progress:
            elapsed = time.monotonic() - started
            print(f"[capture] finished {pcap} in {elapsed:.1f}s", file=sys.stderr, flush=True)
        return completed

    with tempfile.TemporaryDirectory() as directory:
        matches = Path(directory) / "matches.json"
        local_matches = Path(directory) / "local-matches.json" if args.local_pcap else None

        if local_matches:
            # The two captures are independent: each gets its own tshark and
            # PDML stream.  Run them concurrently, then merge their results
            # after both boundary-validation jobs have completed.
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="capture-match") as pool:
                external_future = pool.submit(run_match_capture, args.pcap, matches, "external")
                local_future = pool.submit(run_match_capture, args.local_pcap, local_matches, "local")
                external_future.result()
                local_future.result()
        else:
            run_match_capture(args.pcap, matches, "external")

        result = analyze(args.log, json.loads(matches.read_text()))
        if args.local_pcap:
            local_match_result = json.loads(local_matches.read_text())
            local_by_client = {row["client_id"]: row for row in local_match_result["orders"]}
            result["local_market_decode_diagnostics"] = local_match_result.get("market_decode_diagnostics", {})
            for row in result["orders"]:
                local = local_by_client.get(row["client_id"])
                if local:
                    row["local_boundary_validation"] = local.get("boundary_validation")
                    row["local_verified_capture_latency_us"] = local.get("verified_capture_latency_us")
                    app_latency = row["durations_us"].get("user_trigger_to_write_end")
                    local_latency = local.get("verified_capture_latency_us")
                    external_latency = row.get("verified_capture_latency_us")
                    row["capture_boundary_overhead_us"] = (
                        external_latency - local_latency if external_latency is not None and local_latency is not None else None
                    )
                    row["local_unassigned_difference_us"] = local_latency - app_latency if local_latency is not None and app_latency is not None else None
                    row["local_market_boundary"] = local.get("market_boundary")
                    row["local_order_boundary"] = local.get("order_boundary")
                else:
                    row["local_boundary_validation"] = "missing_or_ambiguous"
                    row["local_verified_capture_latency_us"] = None
                    row["capture_boundary_overhead_us"] = None
                    row["local_unassigned_difference_us"] = None
            result["observation_overhead"] = summarize_observation_overhead(args.local_pcap, args.pcap)
    report = render(result, args.log)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    markdown.write_text(report + "\n")
    print(f"JSON: {args.output}\nMarkdown: {markdown}")


if __name__ == "__main__":
    main()
