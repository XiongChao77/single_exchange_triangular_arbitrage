# Binance Spot JSON receiver

C++20 market-data ingestion for two triangles:

| Triangle | Binance symbols |
| --- | --- |
| ETH / BTC / USDT | ETHBTC, BTCUSDT, ETHUSDT |
| USDT / BTC / BNB | BTCUSDT, BNBBTC, BNBUSDT |

The shared BTCUSDT stream is subscribed once. All five symbols use
`<symbol>@depth20@100ms` on one TLS WebSocket connection. Each message replaces the
local 20-level snapshot for that symbol; edge scanning reads only level one. Symbols are read directly
from `triangles` in the configuration. Startup assigns each unique symbol a
shared slot in first-appearance order. Stream
names are lowercased when building the subscription URL; received symbol names
are kept as provided by Binance.

## Build and run

Dependencies: CMake 3.20+, a C++20 compiler, Boost 1.74+ headers, OpenSSL 1.1.1+
development files, nlohmann/json headers, and Python 3 for integration tests.
On Ubuntu these are provided by `cmake g++ libboost-dev libssl-dev
nlohmann-json3-dev python3`; the tests also use the `openssl` command.

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build-release -j2
```
# Debug version
cd single_exchange_triangular_arbitrage
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j2

If dependencies live outside system include paths, supply
`-DBOOST_INCLUDE_DIR=/path/to/include -DJSON_INCLUDE_DIR=/path/to/include`.
In the current workspace the headers are available in the project-local `.deps` directory:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DBOOST_INCLUDE_DIR="$PWD/.deps/usr/include" \
  -DJSON_INCLUDE_DIR="$PWD/.deps/usr/include"
```

The default config path is `configs/binance.json`, defined directly in the code
and resolved relative to the current working directory. Run from the project
root to use this default. Paths inside the config, such as `ca_file`,
resolve relative to the config file. Runtime settings are constants at the top
of `src/main.cpp`: `kConfigPath` and `kRunSeconds = 10`.
Edit these constants and rebuild to change them. Set `kRunSeconds = 0` to run
until Ctrl+C or SIGTERM. There are no command-line options. Start with:

```bash
./build/json_receiver
```

# latency analysis
sudo ethtool -K <iface> gro off lro off
sudo tcpdump -i <iface> -nn -s 0 -B 32768 -U \
  -w local.pcap 'tcp and (port 443 or port 9443)'

python3 tools/analyze_capture_latency.py \
  --local-pcap /home/chao/work/single_exchange_triangular_arbitrage/local.pcap \
  --pcap /home/chao/work/single_exchange_triangular_arbitrage/logs/1789332734363617.pcapng \
  --log /home/chao/work/single_exchange_triangular_arbitrage/logs/json-1789332734363617.jsonl \
  --keys /home/chao/work/single_exchange_triangular_arbitrage/logs/capture-session.keys \
  --output /home/chao/work/single_exchange_triangular_arbitrage/logs/latency-analysis-1789332734363617.json

The default endpoint is `wss://stream.binance.com:9443`; set `port` to `"443"`
in `configs/binance.json` to use `wss://stream.binance.com:443`.
Public market streams require no API key. The client subscribes through
`/stream?streams=ethbtc@depth20@100ms/...` and decodes JSON text messages
(including the combined-stream `stream`/`data` envelope).

Prices and quantities are parsed from decimal strings into integer mantissas
and shared bid/ask exponents without floating-point conversion. Invalid JSON fields and
integer overflow are rejected.
Spot partial depth payloads do not include an exchange event timestamp, so
`event_time_us` and `exchange_to_receive_us` are logged as `null`.
Receiver, latest orderbook store and logger statistics are available in `scripts/analyze_logs.py`.

Protocol reference: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

## Triangle configuration and latest orderbooks

The only configuration file is `configs/binance.json`:

```json
{
  "host": "stream.binance.com",
  "port": "9443",
  "triangles": [
    ["ETHBTC", "ETHUSDT", "BTCUSDT"],
    ["BTCUSDT", "BNBBTC", "BNBUSDT"]
  ]
}
```

Startup requires a non-empty list of groups, each with three distinct symbols.
Symbols are uppercased for lookup. The resulting unique symbol list is
`[ETHBTC, ETHUSDT, BTCUSDT, BNBBTC, BNBUSDT]`; `triangle_indices` is
`[[0, 1, 2], [2, 3, 4]]`. The order of each input group is preserved.

Before subscribing, one public HTTPS `exchangeInfo` request retrieves the actual
base/quote assets and trading status. Every symbol must permit Spot trading and
be in TRADING status. Each group must connect exactly three distinct assets,
with two edges incident on each asset. This checks a closed triangle regardless
of the order of its three pairs; it does not choose trade direction or calculate
profitability. Invalid configurations or failed metadata requests stop startup.
The REST request has a 15-second deadline. Optional `rest_host` and `rest_port`
in the same config default to `api.binance.com` and `443`. Optional `ca_file`
applies to both HTTPS and WebSocket TLS verification.

`OrderBook` stores the latest 20 bid and 20 ask levels from each partial depth snapshot.
`scan_edge` intentionally reads only the duplicated best bid and ask fields.
`OrderBookManager` stores one optional orderbook per symbol in a fixed-size vector.
The receiver performs one symbol-to-index lookup, then overwrites that slot.
Readers use precomputed indices with `get(index)`, or copy all slots under one
lock with `snapshot()` to obtain a consistent local view. Empty slots mean no
orderbook has arrived yet. Slot replacement retains arrival order, not a historical
queue. Order books are retained across reconnects; readers must check their receive
timestamps before using them. A dedicated network thread reads complete WebSocket messages
and publishes them to a preallocated, bounded SPSC ring (1024 slots, 16 KiB per message).
The main thread busy-polls this lock-free queue in batches of at most 32, parses JSON,
updates books, scans edges, and services executor callbacks through its own `io_context`.
Queue publication uses release/acquire atomics; message storage is not reused until consumption
completes. A full queue drops the new snapshot; oversized messages are also dropped.
`queue_full_total`, `oversized_total`, `queue_depth`, and `queue_high_watermark` expose overload.
Receive sequence numbers include dropped messages. `queue_wait_ns` measures publication-to-parse
waiting, and `receive_to_decision_ns` includes queue waiting and processing. Book freshness
continues to use the original network receive timestamp. Busy polling consumes a CPU core.
Shutdown joins the network thread and consumes remaining queued snapshots before final statistics.
The asynchronous logger retains its own queue for disk I/O. Shutdown logs one `latest_orderbook` per
populated slot, including its index.

Metadata reference: [Binance exchangeInfo](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information).

Arbitrage scanning loads `configs/binance.json` at startup, falling back to
`/root/work/single_exchange_triangular_arbitrage/configs/binance.json` when needed. It uses
exchangeInfo base/quote data to generate both closed paths for each triangle.
The initial balance is assumed to be USDT. Each group must contain two markets quoted in USDT,
which buy the first leg and eventually sell back to USDT. For ETH/BTC/USDT the paths are
`USDT -> ETH -> BTC -> USDT` and `USDT -> BTC -> ETH -> USDT`, regardless of configuration order.
`validate_arbitrage` returns `TradeGroup` objects stored in `Config::trading_groups` before creating the order book manager.
Each group stores three market indices and two paths of three legs. Each market update calls
`scan_edge(index, opportunity)`, which checks both paths in every group containing that market.
A market may belong to several groups; any path above the threshold returns true.
Buys use ask prices and sells use bids. Each leg applies `1 - commission_taker`; the net return must
strictly exceed `edge_threshold`, and the `arbitrage_edge` event is recorded. Both values are ratios;
for example, `0.0001` means 0.01%.
Missing quotes or non-positive top-level prices and quantities skip a path. The top-level scan limits
the initial USDT input with `max_arbitrage_usdt`, which defaults to 100. Best-level quantities reduce
the input further and fees are deducted from the asset received on each leg.
The returned `ArbitrageOpportunity` contains the path, quote/base prices, base quantities before fees,
book update IDs, receive times, estimated input/output/profit, and net return. No opportunity clears
the output parameters. The first qualifying path in configuration order is returned; maximum profit is not guaranteed.
The scan computes theoretical return from three bid/ask levels and configured fees. It does not round
tick/step sizes or simulate balances and dust; the execution module performs those checks before orders.
The independent execution module is described in
[execution_plan.md](execution_plan.md)。

Order book log serialization lives in `orderbook_logging.hpp/.cpp`. Events use
`first_orderbook`, `receiver_orderbook`, and `latest_orderbook`; statistics use
`orderbook_slots` and `orderbook_store`. The analysis script expects this naming
for newly generated logs; older logs retain their original field names.


Execution lives in `include/triangular/execution/` and `src/execution/` (`market_execution`).
`execution_mode` accepts `disabled` (scan only, default when omitted), `paper` (the checked-in
configuration), or `live`. Paper mode uses a simulated USDT wallet seeded with `max_arbitrage_usdt`,
consumes top-of-book liquidity locally, and never calls private Binance APIs. It does not
model queue position, network latency or hidden depth and must not be treated as a live PnL estimate.

Market parsing, scanning, executor state changes, and gateway result callbacks run on the
main thread; market WebSocket callbacks run on a dedicated network `io_context` thread. One
`ArbitrageExecutor` admits only one cycle globally; shared symbols across groups are allowed,
and busy-time opportunities are discarded. The executable does not clear account holdings or dust
at startup or between cycles. Initial balances, including BNB, remain untouched by cleanup.
The three arbitrage legs run sequentially
as LIMIT/FOK orders. Prices come from the accepted opportunity; later legs do not read the order
book or recalculate the edge. Actual net fills determine each following quantity.

The executor emits `execution_state` / `execution_final`. `execution_timeout_ms` limits the total
elapsed time of the complete execution. A timeout or unknown outcome enters `HALTED`; a rejected
or incomplete leg ends the cycle without cleanup orders. There is no intent journal, restart
recovery, or abnormal-position cleanup in the current implementation.

`Gateway` is the submit/query/cancel boundary. `BinanceGateway` implements signed Binance Spot
REST calls on a private worker thread and posts normalized reports back to the main io_context.
Reports must be
normalized cumulative order snapshots with monotonic revisions and complete per-asset fees.
A raw `executionReport` has per-fill commission fields and cannot be passed through unchanged.
It submits normal legs as LIMIT/FOK and initial-balance clearing orders as LIMIT/IOC. The startup
`/api/v3/account` response seeds a local balance cache; FULL order responses update the cache
from cumulative fills and commissions, avoiding account REST calls between legs. Query and cancel
remain gateway operations, but the current low-latency executor does not invoke them.
The gateway reuses its verified TLS connection with HTTP keep-alive, including the connection
opened for the initial account snapshot. Requests execute sequentially on the REST worker.
A server `Connection: close` response or transport error discards the connection; the next
request reconnects. An order POST is never automatically replayed after a write/read failure.
Transport diagnostics include `connection_reused`, `connection_id` and establishment timings.

Live mode requires `hmac_api_key_file` and `hmac_secret_key_file`; credential contents are never logged.
The `max_cycles` configuration limits submitted arbitrage cycles in both live and paper mode;
`0` means unlimited. The checked-in configuration uses `2`. When omitted, the limit defaults
to two with `live_test_mode: true`, otherwise unlimited. `max_cycles` uses `submitted_cycles`, incremented once when the first
arbitrage `Gateway::submit` returns successfully. Startup IOC clearing orders are counted
separately in `startup_orders_submitted` and do not consume `max_cycles`. Balance and
preflight failures before submission do not consume the allowance. Submitted orders that are
later rejected or have an unknown outcome still consume it; this counter records gateway
submission, not proof of transmission from the NIC. `accepted` continues to count admitted
validation attempts. Restarting the process resets both counters.

`execution_state` and `execution_final` include a structured `rejection` object on balance or
initial-preflight failures. It identifies the stage and code; per-leg failures include a zero-based
leg index, symbol and side. Diagnostics include book age/limit, rounded price and quantity,
market limits, liquidity and accepted price as applicable.
For example, `stale_orderbook`, `notional_below_min`, and `price_worsened` distinguish
failures previously reported as `Initial preflight failed`. Profit threshold filtering happens
in `scan_edge`, before an opportunity reaches the executor.

Automatic startup account cleanup is disabled. The executable does not invoke
`start_initial_cleanup()` and emits no `startup_cleanup_*` events. The library retains the explicit
cleanup API for callers that deliberately invoke it; its diagnostic results are omitted from recurring
`execution_state` events. Ordinary arbitrage cycles use only their USDT budget and net leg fills.

The first normal arbitrage leg emits dedicated events (initial IOC clearing and later legs do
not emit these events):

- `arbitrage_first_leg_submit_attempt`: order prepared, immediately before `Gateway::submit`.
- `arbitrage_first_leg_submitted`: the gateway accepted the submission call; this is not a
  network transmission or exchange acceptance confirmation.
- `arbitrage_first_leg_submit_failed`: submission threw synchronously, with the error.
- `arbitrage_first_leg_report`: a processed order report with status (`FILLED`, `REJECTED`,
  `UNKNOWN`, etc.), and fill/commission values when the report is valid and known.

Events carry cycle/client IDs, group, zero-based leg, symbol, side, LIMIT/FOK parameters,
budget and execution mode. `paper` reports are simulated. Duplicate known revisions are
ignored. These records let a log distinguish first-leg execution from balance clearing.

Gateway failures additionally emit `execution_order_failed` for any pending order, including
initial clearing and later legs. Its `failure` object is retained as `order_failure` in execution
state/final records and included in first-leg reports. It includes the failed stage, origin,
method and endpoint (without query), elapsed time, completed DNS/connect/TLS/write/read
timings, request-write flags, and whether the outcome is uncertain. Transport failures carry
an error category/code/message; HTTP failures carry status and Binance `code`/`msg` when
available. An invalid response identifies `response_parse` or `response_decode`.
API keys, secrets, signed targets and signature values are redacted; response bodies and
request headers are not logged. Error text is limited to 1024 bytes.

Failures before writing the order request are known not to have sent that request. Once
writing begins, transport failures remain `UNKNOWN`; HTTP 5xx and Binance -1006/-1007 are
also treated as uncertain, in accordance with the
[Binance error documentation](https://developers.binance.com/en/docs/products/spot/errors).
A complete HTTP response is processed even if subsequent TLS shutdown reports an error.
No automatic order retry is added.

Example live-only fields:

```json
{
  "execution_mode": "live",
  "live_test_mode": true,
  "max_cycles": 2,
  "hmac_api_key_file": "secrets/binance_api_key",
  "hmac_secret_key_file": "secrets/binance_secret_key"
}
```

The current live adapter intentionally serializes private REST requests. It does not yet maintain a Binance user-data WebSocket, implement automatic
clock offset correction, rate-limit scheduling, or automatic cross-process recovery. These limits
make `live_test_mode` suitable for bounded integration testing, not unattended production trading.

Linux local latency measurement is enabled with `"latency_timestamps": true` (set false for a
measurement-overhead baseline). It needs no privileges for socket software timestamps and makes
no extra exchange REST requests. `kernel_timestamp_capability` records socket setup success;
missing timestamps stay null. The executable continues to perform no startup balance cleanup.

`market_latency` records userspace message receipt, processing, decoding, book update and scanning
markers. `execution_order_write` records request write completion without waiting for the HTTP
response. `execution_order_latency` records all order legs, including failures and late reports,
with gateway queue, signing, connect, TLS write, response and callback markers. These events share
`client_id` and carry the cycle/leg, triggering `receive_sequence`, all three book receive sequences
and book update IDs. Subsequent legs also carry the previous report-received time.

The transport adapter reads RX ancillary data below TLS via `recvmsg`. RX observations describe
TCP reads performed during a WebSocket read, **not exact per-message kernel receipt**. TCP/TLS and
WebSocket buffering can split or combine messages or read ahead; `exact_message_mapping` is
always false. A callback with no new TCP read gets no fabricated RX timestamp. Exact message
correlation still needs a TCP/TLS-record/WebSocket byte-range mapping implementation; alternatively
use packet/eBPF tracing with socket identity and TCP sequence ranges to validate transport timing.
A recvmsg can itself combine packets, so its ancillary timestamp is not a complete packet history.

TX uses `TX_SCHED`, `TX_SOFTWARE`, `OPT_ID`, `OPT_ID_TCP` and `OPT_TSONLY`. The adapter counts
ciphertext bytes below TLS, drains `MSG_ERRQUEUE`, filters the request byte range and reports a
final-byte timestamp only when the matching TCP ID arrives. IDs are scoped to `connection_id`;
TLS handshakes and earlier requests are excluded. These times represent the scheduler and driver
software boundaries, not physical NIC transmission. Missing final IDs, truncated controls and drain
limits are reported. Data-path read/write timestamping is optional; no traffic is replayed for it.

Application `*_ns` markers use the monotonic clock. Kernel software times and explicit
`*_realtime_ns` markers use CLOCK_REALTIME. The summarizer checks real/monotonic offset consistency
(with a 1 ms tolerance) before cross-boundary comparisons; this is a clock-jump filter, not a precision
clock calibration. It never subtracts a realtime stamp directly from a monotonic stamp. RX-batch
spans are reported as observations, never as exact per-message end-to-end latency. NIC buffering and
RX processing before the kernel timestamp remain unmeasurable from these software stamps.

Run `python3 tools/summarize_latency.py logs/<run>.jsonl --append-report latency_report.md` to
summarize p50/p95/p99/max and coverage and append a run to the common report. Compare enabled and
disabled runs under the same load to quantify added recvmsg, timestamp and logging overhead.
