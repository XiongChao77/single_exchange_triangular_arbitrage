# Local replay measurement

`replay_benchmark.py` records public Binance 20-level partial-depth messages and market metadata. It runs a local TLS REST/WebSocket server, replays one recording to each receiver sequentially, and saves every sender timestamp, receiver log, missing sequence, and latency distribution. Orders are disabled.

The main and lock-free branches contain identical `replay_decision` instrumentation in `receiver.cpp`. It is enabled only by `TRIANGULAR_REPLAY=1` (set by the runner). Python `time.monotonic_ns()` and C++ `steady_clock` must use the same clock epoch on the same Linux host; the runner checks timestamp ordering. Do not copy timing fields to another host.

Build each branch in Release in its own worktree. If dependencies are local, pass `BOOST_INCLUDE_DIR` and `JSON_INCLUDE_DIR` to CMake. Specify the actual two binaries explicitly; directory names alone do not identify the implementation.

```bash
python3 tools/replay_benchmark.py record \
  --output benchmarks/recording-new --seconds 15 --max-symbols 180

python3 tools/replay_benchmark.py benchmark \
  --dataset benchmarks/recording-new \
  --output benchmarks/results-new \
  --main /absolute/path/to/main/build-release/json_receiver \
  --lock-free /absolute/path/to/lock-free/build-release/json_receiver \
  --speeds 1 10 50 --bursts 500 --repeats 2
```

The dataset saves the current configuration's triangles, then adds active triangles from `binance_triangles.json` within the symbol budget. No API keys are recorded. Production configuration is not edited. Local server certificates are generated only for this test.

A fixed monotonic schedule is assigned before sending. Sender `send_start_ns` and `send_end_ns` are recorded separately. The primary `scheduled_to_decision_us` includes lateness and send-side backpressure, avoiding omission of delayed sends. `send_start_to_receive_us`, `send_start_to_decision_us`, and `receive_to_decision_us` locate waiting, while `sender_lateness_us` identifies generator delays. High lateness means requested speed was not achieved; report it, rather than claiming throughput at that speed.

Burst mode emits up to N messages every 100 ms. It preserves recording order and recorded prices; it does not simulate exchange matching or claim new profitable opportunities. Normal mode preserves observed arrival intervals divided by speed, not unknown exchange event timestamps.

The connection stays open after the last message. The runner waits for decisions or a drain timeout and then sends SIGINT. The final analysis checks uniqueness/order and reports missing sequences alongside queue/invalid/logger statistics. Loss must be considered before comparing latency. Per-message timing logging adds measurement overhead to both variants.

Results include `report.md`, `results.json`, and per-run `metrics.json`, `sender.jsonl`, `configs/binance.json`, `console.txt`, and receiver JSONL logs. CPU time covers startup, scheduled replay, and drain. This measures finite replay workloads, not production exchange-to-decision latency or maximum sustainable throughput.
