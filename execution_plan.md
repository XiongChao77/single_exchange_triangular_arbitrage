# Triangular Arbitrage Execution Module

`OrderBookManager` scans two USDT round-trip paths for each group. `Receiver` passes opportunities to the single
global `ArbitrageExecutor`. While it is active, other groups are rejected, so groups may share symbols such as
`BTCUSDT` while only one triangle executes at a time.

## Execution Flow

After a cycle is accepted, the executor performs these preflight checks once:

1. Confirm sufficient USDT in the Gateway balance cache.
2. Sell existing non-USDT balances with a LIMIT/IOC order on the matching `asset/USDT` market; balances below
   exchange filters are recorded as dust.
3. Read all three order books once and validate age, top-level liquidity, prices, exchange filters, and profit
   after rounding.
4. Send three LIMIT/FOK orders in sequence.

After the first leg is sent, the executor does not reread books, recalculate the edge, or query balances. Later
prices use accepted `ArbitrageOpportunity::prices`; quantities use the previous fill and actual fee.

## State and Failure

The main state path is `IDLE -> VALIDATING -> LEG_PENDING (legs 1/2/3) -> IDLE`, with initial position clearing
before the legs when required. Only one cycle may execute at a time. A rejected, expired, canceled, or incomplete
leg fails the cycle and returns to `IDLE` without an emergency cleanup order. An Unknown result or a cycle
exceeding `execution_timeout_ms` enters `HALTED`; the current version does not query, cancel, recover, or log intent.

`execution_timeout_ms` covers accepting an opportunity through cleanup and all three legs. The current value is
1000 ms; valid values are 1 to 60000 ms.

## Gateway

`PaperGateway` simulates fills and balances at the best level. `BinanceGateway` signs HTTPS in a worker thread
and posts callbacks to the main `io_context`. A live gateway calls `/api/v3/account` once to build its balance
cache; cumulative fills and fees from each `newOrderRespType=FULL` response update the cache, so no extra
account request is made between legs.

Normal arbitrage orders use LIMIT/FOK; preflight cleanup uses LIMIT/IOC. The Gateway interface retains
query/cancel for future extensions, but the low-latency path does not call them.

`live_test_mode=true` accepts at most 10 cycles per process. Failed cycles and failed preflight checks count
against the limit. Key files are read only in live mode and are never written to logs.

## Current Scope

The implementation targets the normal fill path. It has no order-intent log, cross-process recovery, user-data
WebSocket, automatic abnormal-position cleanup, clock-offset calibration, or full rate-limit scheduler. The
default configuration remains paper mode and sends no real orders.
