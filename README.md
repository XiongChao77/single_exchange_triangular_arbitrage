# Binance Spot JSON receiver

C++20 market-data ingestion for two triangles:

| Triangle | Binance symbols |
| --- | --- |
| ETH / BTC / USDT | ETHBTC, BTCUSDT, ETHUSDT |
| USDT / BTC / BNB | BTCUSDT, BNBBTC, BNBUSDT |

The shared BTCUSDT stream is subscribed once. All five symbols use
`<symbol>@bookTicker` on one TLS WebSocket connection. Symbols are read directly
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
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
ctest --test-dir build --output-on-failure
./build/json_receiver
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

The default endpoint is `wss://stream.binance.com:9443`; set `port` to `"443"`
in `configs/binance.json` to use `wss://stream.binance.com:443`.
Public market streams require no API key. The client subscribes through
`/stream?streams=ethbtc@bookTicker/...` and decodes JSON text messages
(including the combined-stream `stream`/`data` envelope).

Prices and quantities are parsed from decimal strings into integer mantissas
and shared bid/ask exponents without floating-point conversion. Invalid JSON fields and
integer overflow are rejected.
Spot `bookTicker` does not include an exchange event timestamp, so
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

`OrderBook` is currently simplified to the best bid and ask; full depth is not yet implemented.
`OrderBookManager` stores one optional orderbook per symbol in a fixed-size vector.
The receiver performs one symbol-to-index lookup, then overwrites that slot.
Readers use precomputed indices with `get(index)`, or copy all slots under one
lock with `snapshot()` to obtain a consistent local view. Empty slots mean no
orderbook has arrived yet. Slot replacement retains arrival order, not a historical
queue. Order books are retained across reconnects; readers must check their receive
timestamps before using them. There is no orderbook consumer thread. The asynchronous
logger retains its own queue for disk I/O. Shutdown logs one `latest_orderbook` per
populated slot, including its index.

Metadata reference: [Binance exchangeInfo](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#exchange-information).

套利扫描：启动时读取 `configs/binance.json`（当前目录不存在该文件时，读取
`/root/work/single_exchange_triangular_arbitrage/configs/binance.json`），并通过
`exchangeInfo` 的 base/quote 信息为每组三角组合生成正反两条闭环路径。
假设初始仅持有 USDT，每组必须包含两个以 USDT 为 quote 的交易对；
分别通过这两个交易对买入作为第一腿，最终卖出回到 USDT。
例如 ETH/BTC/USDT 的路径为 `USDT → ETH → BTC → USDT` 和
`USDT → BTC → ETH → USDT`，与配置中交易对的排列顺序无关。
`validate_arbitrage` 返回 `TradeGroup` 列表，存入 `Config::trading_groups` 后创建订单簿管理器。
每个 group 保存三个交易对的索引和两条 path，每条 path 包含三条交易腿。
每次行情更新调用 `scan_edge(index, opportunity)`，遍历 groups，检查包含该交易对的组内两条路径。
同一交易对可能属于多个 group；找到任意超过阈值的路径即可返回 true。
买入按 ask 换算，卖出按 bid 换算，每条腿的所得乘以
`1 - commission_taker`；最终净收益率严格大于 `edge_threshold` 才成立，
并记录 `arbitrage_edge` 日志。两个配置值均为比例，例如 `0.0001` 为 0.01%。
缺失报价或所用一档价格、数量非正时跳过该路径。这是顶层报价收益率扫描，
`max_arbitrage_usdt` 默认为 100，限制单次三角套利的初始 USDT 投入。
扫描根据三条腿的最优档数量进一步缩小投入，手续费按每腿收到的资产扣除。
返回 `ArbitrageOpportunity`，其中 `path` 是三条交易腿，`prices` 为 quote/base 价格，
`quantities` 为每腿下单的 base 数量（买卖均相同、扣手续费前），并附上
订单簿更新 ID、接收时间、预计投入/回款/利润和净收益率。无机会时清空输出参数。
当前返回配置顺序中第一个合格机会，不保证利润最大。
金额与数量是浮点预估，尚未处理交易所过滤器、数量取整、实际费用币种和报价过期；
扫描本身不提交订单。独立执行模块只在一轮开始前验证报价、按十进制 tick/step 取整，并按真实网关回报
推进订单。可选择本地模拟网关或 Binance Spot live REST 网关。实现说明见
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

All receiver/executor/gateway callbacks run on the same single `io_context` thread. One
`ArbitrageExecutor` admits only one cycle globally; shared symbols across groups are allowed,
and busy-time opportunities are discarded. Before a cycle, related non-USDT balances are sold
through the group's USDT markets with IOC orders. The three arbitrage legs then run sequentially
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

Live mode requires `api_key_file` and `secret_key_file`; credential contents are never logged.
With `live_test_mode: true`, the process accepts at most 10 arbitrage cycles. Failed or rejected
cycles count toward this conservative cap. One cycle can create three normal orders and additional
orders to clear initial balances, so the cap is ten triangular attempts, not ten individual orders.
Restarting the process starts a new ten-cycle allowance. The checked-in
configuration remains `paper`, so building or running the default project cannot submit live orders.

Example live-only fields:

```json
{
  "execution_mode": "live",
  "live_test_mode": true,
  "api_key_file": "secrets/binance_api_key",
  "secret_key_file": "secrets/binance_secret_key"
}
```

The current live adapter intentionally serializes private REST requests. It does not yet maintain a Binance user-data WebSocket, implement automatic
clock offset correction, rate-limit scheduling, or automatic cross-process recovery. These limits
make `live_test_mode` suitable for bounded integration testing, not unattended production trading.
