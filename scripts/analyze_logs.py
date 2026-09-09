"""Summarize receiver/consumer and asynchronous writer measurements from JSONL."""

import argparse
from collections import Counter
import json
import math
from pathlib import Path


def distribution(values):
    if not values:
        return None
    values = sorted(values)
    return {"count": len(values), "mean": sum(values) / len(values),
            "p50": values[math.ceil(len(values) * .50) - 1],
            "p95": values[math.ceil(len(values) * .95) - 1],
            "p99": values[math.ceil(len(values) * .99) - 1], "max": values[-1]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", nargs="?", type=Path, help="Defaults to the latest logs/*.jsonl")
    args = parser.parse_args()
    path = args.file or max(Path("logs").glob("*.jsonl"), default=None)
    if path is None:
        parser.error("No log file found")
    received, consumed = Counter(), Counter()
    ages, exchange_ages, writes, serialization = [], [], [], []
    final, logger, started, ended = None, None, None, None
    truncated = 0
    with path.open() as source:
        for line in source:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                truncated += 1
                continue
            event, fields = row["event"], row["fields"]
            if event == "run_started":
                started = row["time_us"]
            elif event == "receiver_stopped":
                ended = row["time_us"]
            elif event == "receiver_quote":
                received[fields["symbol"]] += 1
                exchange_ages.append(fields["exchange_to_receive_us"])
            elif event == "consumer_quote":
                consumed[fields["symbol"]] += 1
                ages.append(fields["receive_to_consume_ns"] / 1000)
            elif event == "logger_batch" and fields["batch_records"]:
                writes.append(fields["last_write_us"])
                serialization.append(fields["serialize_us"])
            elif event == "pipeline_final":
                final = fields
            elif event == "logger_final":
                logger = fields
    duration = (ended - started) / 1e6 if started is not None and ended is not None else None
    result = {"file": str(path), "duration_seconds": duration, "receiver_log_counts": dict(received),
              "consumer_log_counts": dict(consumed), "receive_to_consume_us": distribution(ages),
              "exchange_to_receive_us_clock_dependent": distribution(exchange_ages),
              "batch_write_flush_us": distribution(writes), "batch_serialization_us": distribution(serialization),
              "pipeline_final": final, "logger_final": logger, "malformed_lines": truncated}
    if final and logger:
        q = final["quote_queue"]
        result["accounting_ok"] = (q["received"] == q["consumed"] + q["dropped"] + q["queued"] and
            logger["submitted"] == logger["accepted"] + logger["dropped"] and
            logger["accepted"] == logger["written"] + logger["write_lost"])
        result["complete_run_without_local_loss"] = (result["accounting_ok"] and q["queued"] == 0 and
            q["dropped"] == logger["dropped"] == logger["write_lost"] == truncated == 0 and
            not logger["failed"] and not final["consumer"]["failed"] and
            final["receiver"]["invalid"] == 0 and final["consumer"]["processed"] == q["consumed"] and
            sum(received.values()) == q["received"] and sum(consumed.values()) == q["consumed"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
