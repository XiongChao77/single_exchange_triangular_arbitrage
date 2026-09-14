# Triangular Arbitrage Execution Module

`OrderBookManager` scans two USDT round-trip paths for each group. `Receiver` passes opportunities to the single
global `ArbitrageExecutor`. While it is active, other groups are rejected, so groups may share symbols such as
`BTCUSDT` while only one triangle executes at a time.

## Execution Flow

Startup position clearing, when invoked, runs separately from ordinary cycles and uses LIMIT/IOC orders; USDT and BNB are protected and balances below exchange filters are recorded as dust. After a cycle is accepted:

1. Confirm sufficient USDT in the Gateway balance cache.
2. Read all three order books once and validate age, top-level liquidity, prices, exchange filters, and price deterioration against the accepted opportunity, with fee-adjusted simulated proceeds.
3. Prepare and send LIMIT/FOK orders sequentially. The current temporary test exits after the first-leg terminal response, before advancing to the second leg.

After the first leg is sent, the executor does not reread books, recalculate the edge, or query balances. Later
prices use accepted `ArbitrageOpportunity::prices`; quantities use the previous fill and actual fee.

## State and Failure

The main state path is `IDLE -> VALIDATING -> LEG_PENDING (legs 1/2/3) -> IDLE`, with initial position clearing
before the legs when required. Only one cycle may execute at a time. A rejected, expired, canceled, or incomplete
leg fails the current cycle and returns to `IDLE` without an emergency cleanup order, allowing a later opportunity
to start a new cycle. The current version does not query, cancel, recover, or log intent for late or ambiguous orders.

`execution_timeout_ms` starts when a candidate is accepted, before the posted `begin()` callback and balance/preflight checks. Valid values are 1 to 60000 ms. On timeout, an outstanding gateway task keeps the executor in `LEG_PENDING` with `waiting_gateway=true`; new cycles remain blocked until the callback returns. The late result is handled without advancing to another leg, and the slot is then released. Timeout does not cancel an exchange order or abort the REST worker.

The temporary first-leg response stop uses `fail_cycle("First-leg response test stop")`, so it increments the failure counter rather than the completed-three-leg counter. The [retained latency report](latency-analysis1789373319293622.md) records 100 submitted first legs. Full-cycle tests expecting three fills are incompatible with this temporary branch.

## Gateway

`PaperGateway` simulates fills and balances at the best level. `BinanceGateway` signs HTTPS in a worker thread
and posts callbacks to the main `io_context`. A live gateway calls `/api/v3/account` once to build its balance
cache; cumulative fills and fees from each `newOrderRespType=FULL` response update the cache, so no extra
account request is made between legs.

Normal arbitrage orders use LIMIT/FOK; preflight cleanup uses LIMIT/IOC. The Gateway interface retains
query/cancel for future extensions, but the low-latency path does not call them.

`live_test_mode=true` defaults the process limit to two submitted cycles unless `max_cycles` is explicitly
configured. The limit counts cycles whose first-leg request was accepted by `gateway.submit()`; failed preflight checks do not consume it. This boundary is queue acceptance, not HTTP write completion. Key files are read only in live
mode and are never written to logs.

## Current Scope

The implementation targets the normal fill path. It has no order-intent log, cross-process recovery, user-data
WebSocket, automatic abnormal-position cleanup, clock-offset calibration, or full rate-limit scheduler. Check `execution_mode` in the local configuration before running; `live_test_mode` only changes the default cycle limit and does not simulate orders.

The executor favors bounded, explicit cycle failure over automatic retries. An ambiguous order result can leave an
unknown exchange position; the cycle fails; if its gateway task is still outstanding, executor availability waits for that task to return. External reconciliation is still required for any unknown exchange position.
