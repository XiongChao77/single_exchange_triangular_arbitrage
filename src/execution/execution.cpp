#include "triangular/execution/execution.hpp"
#include "triangular/logger.hpp"
#include "triangular/timestamp_stream.hpp"
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <algorithm>
#include <sstream>
#include <set>

namespace triangular::execution {
namespace {
Decimal positive_decimal(const nlohmann::json& j, const char* key) {
    Decimal n(j.at(key).get<std::string>());
    if (!boost::math::isfinite(n) || n < 0) throw std::runtime_error("Invalid market filter");
    return n;
}
Decimal price_of(const OrderBook& book, bool buy) {
    return Decimal(buy ? book.ask_price : book.bid_price) * pow(Decimal(10), int(book.price_exponent));
}
Decimal quantity_of(const OrderBook& book, bool buy) {
    return Decimal(buy ? book.ask_qty : book.bid_qty) * pow(Decimal(10), int(book.qty_exponent));
}
Decimal down(const Decimal& value, const Decimal& step) { return floor(value / step) * step; }
bool terminal(OrderStatus status) {
    return status == OrderStatus::Filled || status == OrderStatus::Canceled ||
           status == OrderStatus::Expired || status == OrderStatus::Rejected;
}
nlohmann::json balance_json(const Balances& balances) {
    auto result = nlohmann::json::object();
    for (const auto& [asset, amount] : balances) result[asset] = decimal_text(amount);
    return result;
}
}

std::string decimal_text(const Decimal& value) {
    auto text = value.str(40, std::ios_base::fixed);
    while (text.size() > 1 && text.back() == '0') text.pop_back();
    if (text.back() == '.') text.pop_back();
    return text;
}

std::vector<Market> load_markets(const Config& config, const nlohmann::json& info) {
    std::vector<Market> result(config.symbols.size());
    for (const auto& item : info.at("symbols")) {
        const auto index = config.symbol_indices.find(item.at("symbol").get<std::string>());
        if (index == config.symbol_indices.end()) continue;
        Market market{};
        market.symbol = index->first;
        market.base = item.at("baseAsset").get<std::string>();
        market.quote = item.at("quoteAsset").get<std::string>();
        if (item.at("status") != "TRADING" || !item.at("isSpotTradingAllowed").get<bool>())
            throw std::runtime_error("Execution market unavailable");
        for (const auto& filter : item.at("filters")) {
            const auto type = filter.at("filterType").get<std::string>();
            if (type == "PRICE_FILTER") {
                market.tick = positive_decimal(filter, "tickSize");
                market.min_price = positive_decimal(filter, "minPrice");
                market.max_price = positive_decimal(filter, "maxPrice");
            } else if (type == "LOT_SIZE") {
                market.step = positive_decimal(filter, "stepSize");
                market.min_qty = positive_decimal(filter, "minQty");
                market.max_qty = positive_decimal(filter, "maxQty");
            } else if (type == "MIN_NOTIONAL") {
                market.min_notional = std::max(market.min_notional, positive_decimal(filter, "minNotional"));
            } else if (type == "NOTIONAL") {
                market.min_notional = std::max(market.min_notional, positive_decimal(filter, "minNotional"));
                market.max_notional = positive_decimal(filter, "maxNotional");
            }
        }
        if (market.tick <= 0 || market.step <= 0)
            throw std::runtime_error("Execution requires positive PRICE_FILTER/LOT_SIZE");
        result.at(index->second) = std::move(market);
    }
    for (const auto& market : result)
        if (market.symbol.empty()) throw std::runtime_error("Missing execution market");
    return result;
}

const char* state_name(State state) {
    switch (state) {
    case State::Idle: return "IDLE";
    case State::Validating: return "VALIDATING";
    case State::LegPending: return "LEG_PENDING";
    case State::ClearingInitialPosition: return "CLEARING_INITIAL_POSITION";
    case State::Halted: return "HALTED";
    }
    return "HALTED";
}

struct ArbitrageExecutor::Impl : std::enable_shared_from_this<Impl> {
    boost::asio::io_context& io;
    Config config;
    OrderBookManager& books;
    std::vector<Market> markets;
    Gateway& gateway;
    Options options;
    Observer observer;
    EventObserver event_observer;
    boost::asio::steady_timer timeout, cleanup_wait;
    bool cleanup_started = false, cleanup_done = false;
    std::int64_t cleanup_deadline_ns = 0;
    std::set<std::string> cleanup_processed;
    std::uint64_t startup_orders_submitted = 0;
    nlohmann::json cleanup_results = nlohmann::json::object();
    State state = State::Idle;
    bool stopping = false, failed = false, cycle_submitted = false;
    bool gateway_busy = false, waiting_gateway = false;
    nlohmann::json rejection = nlohmann::json::object();
    nlohmann::json order_failure = nlohmann::json::object();
    std::string reason, cycle;
    std::uint64_t next_cycle = 0, accepted = 0, submitted_cycles = 0, completed = 0, failures = 0;
    std::uint64_t rejected_busy = 0, rejected_limit = 0;
    unsigned order_number = 0, clear_orders = 0;
    std::size_t leg_number = 0;
    ArbitrageOpportunity opportunity;
    Balances holdings, dust;
    Decimal budget = 0;
    std::int64_t cycle_started_ns = 0, previous_report_ns = 0;
    struct PreparationTiming {
        std::int64_t post_begin = 0, begin = 0, balance_begin = 0, balance_end = 0;
        std::int64_t snapshot_begin = 0, snapshot_end = 0, preflight_end = 0, order_begin = 0, order_end = 0;
    } preparation;
    std::optional<OrderRequest> pending;
    OrderReport accounted;

    Impl(boost::asio::io_context& context, const Config& c, OrderBookManager& manager,
         std::vector<Market> market_list, Gateway& order_gateway, Options execution_options, Observer obs, EventObserver events)
        : io(context), config(c), books(manager), markets(std::move(market_list)), gateway(order_gateway),
          options(std::move(execution_options)), observer(std::move(obs)), event_observer(std::move(events)), timeout(io), cleanup_wait(io) {
        if (markets.size() != config.symbols.size() || options.max_book_age.count() <= 0 ||
            options.execution_timeout.count() <= 0 || options.initial_cleanup_wait.count() <= 0 || !options.max_initial_clear_orders)
            throw std::runtime_error("Invalid execution options");
    }

    nlohmann::json status() const {
        const bool limit_reached = options.max_cycles != 0 && submitted_cycles >= options.max_cycles;
        const std::string stop_reason = limit_reached ? "max_cycles_reached" :
            (state == State::Halted ? "halted" :
             (stopping ? "stopped" : (failed ? "cycle_failed" : "")));
        nlohmann::json result = {{"state", state_name(state)}, {"cycle_id", cycle}, {"reason", reason},
            {"gateway_busy", gateway_busy}, {"waiting_gateway", waiting_gateway},
            {"accepted", accepted}, {"submitted_cycles", submitted_cycles},
            {"cycle_submitted", cycle_submitted}, {"rejection", rejection}, {"order_failure", order_failure}, {"completed", completed}, {"failed", failures},
            {"rejected_busy", rejected_busy}, {"rejected_limit", rejected_limit},
            {"max_cycles", options.max_cycles}, {"holdings", balance_json(holdings)},
            {"limit_reached", limit_reached}, {"stop_reason", stop_reason},
            {"dust", balance_json(dust)}, {"budget_usdt", decimal_text(budget)},
            {"leg", leg_number}, {"stopping", stopping},
            {"startup_cleanup_started", cleanup_started}, {"startup_cleanup_done", cleanup_done},
            {"startup_orders_submitted", startup_orders_submitted}, {"startup_cleanup_results", cleanup_results}};
        if (pending) result["pending"] = {{"client_id", pending->client_id},
            {"symbol_index", pending->symbol_index}, {"buy", pending->buy},
            {"price", pending->price}, {"quantity", pending->quantity},
            {"initial_clear", pending->initial_clear}, {"filled_qty", decimal_text(accounted.filled_qty)},
            {"filled_quote", decimal_text(accounted.filled_quote)}};
        return result;
    }

    void publish() const {
        if (!observer) return;
        auto fields = status();
        fields.erase("startup_cleanup_results");
        fields.erase("dust");
        observer(fields);
    }
    void halt(std::string why) {
        timeout.cancel(); cleanup_wait.cancel(); state = State::Halted; reason = std::move(why); ++failures; publish();
    }
    // A failed cycle cannot release its slot while its REST task is still running.
    void fail_cycle(std::string why) {
        timeout.cancel(); cleanup_wait.cancel(); failed = true; reason = std::move(why);
        if (!waiting_gateway) ++failures;
        waiting_gateway = gateway_busy;
        if (!waiting_gateway) { pending.reset(); state = State::Idle; }
        publish();
    }
    bool fresh(const OrderBook& book) const {
        const auto age = steady_time_ns() - book.received_steady_ns;
        return book.received_steady_ns > 0 && age >= 0 &&
            age <= std::chrono::duration_cast<std::chrono::nanoseconds>(options.max_book_age).count();
    }
    bool valid(const Market& market, const Decimal& price, const Decimal& quantity,
               nlohmann::json* detail = nullptr) const {
        const char* code = nullptr;
        if (price <= 0) code = "nonpositive_price";
        else if (quantity <= 0) code = "nonpositive_quantity";
        else if (market.min_price != 0 && price < market.min_price) code = "price_below_min";
        else if (market.max_price != 0 && price > market.max_price) code = "price_above_max";
        else if (quantity < market.min_qty) code = "quantity_below_min";
        else if (market.max_qty != 0 && quantity > market.max_qty) code = "quantity_above_max";
        else if (price * quantity < market.min_notional) code = "notional_below_min";
        else if (market.max_notional != 0 && price * quantity > market.max_notional) code = "notional_above_max";
        if (code && detail) {
            (*detail)["code"] = code;
            (*detail)["min_price"] = decimal_text(market.min_price);
            (*detail)["max_price"] = decimal_text(market.max_price);
            (*detail)["min_quantity"] = decimal_text(market.min_qty);
            (*detail)["max_quantity"] = decimal_text(market.max_qty);
            (*detail)["min_notional"] = decimal_text(market.min_notional);
            (*detail)["max_notional"] = decimal_text(market.max_notional);
        }
        return code == nullptr;
    }

    std::optional<OrderRequest> prepare_from_book(const TradeLeg& leg, const Decimal& amount,
            bool initial_clear, const std::vector<std::optional<OrderBook>>& snapshot,
            nlohmann::json* detail = nullptr) const {
        const auto& book = snapshot.at(leg.index);
        if (!book || !fresh(*book)) {
            if (detail) {
                (*detail)["code"] = book ? "stale_orderbook" : "missing_orderbook";
                (*detail)["max_book_age_ms"] = options.max_book_age.count();
                if (book) {
                    (*detail)["book_age_ns"] = steady_time_ns() - book->received_steady_ns;
                    (*detail)["book_update_id"] = book->book_update_id;
                }
            }
            return {};
        }
        const auto& market = markets.at(leg.index);
        const Decimal raw_price = price_of(*book, leg.buy), liquidity = quantity_of(*book, leg.buy);
        const Decimal price = leg.buy ? Decimal(ceil(raw_price / market.tick) * market.tick)
                                      : down(raw_price, market.tick);
        Decimal quantity = down(leg.buy ? amount / price : amount, market.step);
        if (initial_clear) quantity = down(std::min(quantity, liquidity), market.step);
        if (detail) {
            (*detail)["price"] = decimal_text(price);
            (*detail)["quantity"] = decimal_text(quantity);
            (*detail)["notional"] = decimal_text(price * quantity);
            (*detail)["liquidity"] = decimal_text(liquidity);
            (*detail)["input_amount"] = decimal_text(amount);
            (*detail)["tick_size"] = decimal_text(market.tick);
            (*detail)["step_size"] = decimal_text(market.step);
        }
        if (!valid(market, price, quantity, detail)) return {};
        if (quantity > liquidity) {
            if (detail) (*detail)["code"] = "insufficient_top_level_liquidity";
            return {};
        }
        return OrderRequest{"", leg.index, leg.buy, decimal_text(price), decimal_text(quantity), initial_clear};
    }

    std::optional<OrderRequest> prepare_fast(const TradeLeg& leg, const Decimal& amount, std::size_t path_leg) const {
        const auto& market = markets.at(leg.index);
        const Decimal scanned(opportunity.prices.at(path_leg));
        const Decimal price = floor(scanned / market.tick + Decimal("0.5")) * market.tick;
        const Decimal quantity = down(leg.buy ? amount / price : amount, market.step);
        if (!valid(market, price, quantity)) return {};
        return OrderRequest{"", leg.index, leg.buy, decimal_text(price), decimal_text(quantity), false};
    }

    bool preflight() {
        auto balances = holdings;
        if (config.latency_timestamps) preparation.snapshot_begin = steady_time_ns();
        const auto snapshot = books.snapshot();
        if (config.latency_timestamps) preparation.snapshot_end = steady_time_ns();
        const Decimal fee = Decimal(1) - Decimal(nlohmann::json(config.commission_taker).dump());
        for (std::size_t i = 0; i < opportunity.path.size(); ++i) {
            const auto& leg = opportunity.path[i];
            const auto& market = markets.at(leg.index);
            rejection = {{"stage", "initial_preflight"}, {"leg", i},
                {"symbol", market.symbol}, {"side", leg.buy ? "BUY" : "SELL"}};
            const auto order = prepare_from_book(leg, balances[leg.buy ? market.quote : market.base], false, snapshot, &rejection);
            if (!order) { reason = "Initial preflight failed: " + rejection.at("code").get<std::string>(); return false; }
            const Decimal price(order->price), quantity(order->quantity);
            const Decimal accepted_price = floor(Decimal(opportunity.prices[i]) / market.tick + Decimal("0.5")) * market.tick;
            if ((leg.buy && price > accepted_price) || (!leg.buy && price < accepted_price)) {
                rejection["code"] = "price_worsened";
                rejection["accepted_price"] = decimal_text(accepted_price);
                reason = "Initial preflight failed: price_worsened";
                return false;
            }
            if (leg.buy) {
                balances[market.quote] -= price * quantity;
                balances[market.base] += quantity * fee;
            } else {
                balances[market.base] -= quantity;
                balances[market.quote] += quantity * price * fee;
            }
        }
        rejection = nlohmann::json::object();
        return true;
    }

    void start_arbitrage() {
        holdings = {{"USDT", budget}};
        const bool ready = preflight();
        if (config.latency_timestamps) preparation.preflight_end = steady_time_ns();
        if (!ready) { failed = true; finish(); return; }
        leg_number = 0;
        next_leg();
    }

    void cleanup_event(std::string_view event, const nlohmann::json& fields) const {
        if (event_observer) event_observer(event, fields);
    }
    void arm_timeout() {
        timeout.expires_after(options.execution_timeout);
        timeout.async_wait([weak = weak_from_this()](const boost::system::error_code& error) {
            if (!error)
                if (auto self = weak.lock(); self && self->state != State::Idle && self->state != State::Halted) {
                    if (self->pending && self->pending->initial_clear) self->halt("Execution timeout");
                    else self->fail_cycle("Execution timeout");
                }
        });
    }
    void clear_initial_position() {
        if (stopping || state == State::Halted || cleanup_done || pending) return;
        const auto snapshot = books.snapshot();
        bool waiting = false;
        for (auto& [asset, amount] : holdings) {
            if (amount <= 0 || cleanup_processed.contains(asset)) continue;
            auto skip = [&](const char* code, nlohmann::json detail = nlohmann::json::object()) {
                cleanup_processed.insert(asset);
                detail["code"] = code;
                detail["balance"] = decimal_text(amount);
                cleanup_results[asset] = detail;
                detail["asset"] = asset;
                cleanup_event("startup_cleanup_skipped", detail);
            };
            if (asset == "USDT" || asset == "BNB") { skip("protected_asset"); continue; }
            std::optional<TradeLeg> exit_leg;
            for (std::size_t index = 0; index < markets.size(); ++index) {
                const auto& candidate = markets[index];
                if (candidate.base == asset && candidate.quote == "USDT") { exit_leg = TradeLeg{index, false}; break; }
                if (candidate.base == "USDT" && candidate.quote == asset) exit_leg = TradeLeg{index, true};
            }
            if (!exit_leg) { skip("no_usdt_market"); continue; }
            const auto& market = markets.at(exit_leg->index);
            const auto& book = snapshot.at(exit_leg->index);
            if (!book || !fresh(*book)) {
                if (steady_time_ns() < cleanup_deadline_ns) { waiting = true; continue; }
                nlohmann::json detail = {{"symbol", market.symbol}};
                if (book) detail["book_age_ns"] = steady_time_ns() - book->received_steady_ns;
                skip(book ? "stale_orderbook" : "missing_orderbook", detail);
                continue;
            }
            const Decimal price = price_of(*book, exit_leg->buy);
            if (price <= 0) { skip("nonpositive_price", {{"symbol", market.symbol}}); continue; }
            const Decimal quantity = down(exit_leg->buy ? amount / price : amount, market.step);
            if (quantity == 0 || quantity < market.min_qty || price * quantity < market.min_notional) {
                dust[asset] = amount;
                skip("dust", {{"symbol", market.symbol}, {"quantity", decimal_text(quantity)},
                    {"notional", decimal_text(price * quantity)}, {"min_notional", decimal_text(market.min_notional)}});
                continue;
            }
            if (clear_orders >= options.max_initial_clear_orders) { skip("startup_order_limit"); continue; }
            nlohmann::json detail = {{"symbol", market.symbol}};
            auto order = prepare_from_book(*exit_leg, amount, true, snapshot, &detail);
            if (!order) { const auto code = detail.at("code").get<std::string>(); skip(code.c_str(), detail); continue; }
            cleanup_processed.insert(asset); // One submission attempt per initial asset; no remainder retries.
            ++clear_orders;
            cleanup_results[asset] = {{"code", "submit_attempt"}, {"initial_balance", decimal_text(amount)}};
            arm_timeout();
            send(*order);
            return;
        }
        if (waiting) {
            cleanup_wait.expires_after(std::chrono::milliseconds(50));
            cleanup_wait.async_wait([weak = weak_from_this()](const boost::system::error_code& error) {
                if (!error) if (auto self = weak.lock()) self->clear_initial_position();
            });
            return;
        }
        timeout.cancel(); cleanup_wait.cancel();
        cleanup_done = true; state = State::Idle; pending.reset(); holdings.clear();
        cleanup_event("startup_cleanup_finished", {{"startup_orders_submitted", startup_orders_submitted},
            {"results", cleanup_results}, {"dust", balance_json(dust)}});
        publish();
    }
    void start_initial_cleanup() {
        if (cleanup_started || stopping || state != State::Idle) return;
        cleanup_started = true; state = State::ClearingInitialPosition;
        cycle = "startup-" + std::to_string(wall_time_us());
        holdings = gateway.balances(); // Snapshot once; later arbitrage cycles never sweep balances.
        cleanup_deadline_ns = steady_time_ns() +
            std::chrono::duration_cast<std::chrono::nanoseconds>(options.initial_cleanup_wait).count();
        cleanup_event("startup_cleanup_started", {{"cycle_id", cycle}, {"protected_assets", {"USDT", "BNB"}},
            {"wait_ms", options.initial_cleanup_wait.count()}});
        publish();
        clear_initial_position();
    }
    void begin() {
        if (config.latency_timestamps) preparation.begin = steady_time_ns();
        if (state != State::Validating || stopping) return;
        if (config.latency_timestamps) preparation.balance_begin = steady_time_ns();
        const auto account = gateway.balances();
        if (config.latency_timestamps) preparation.balance_end = steady_time_ns();
        if (!account.contains("USDT") || account.at("USDT") < budget) {
            rejection = {{"stage", "balance_check"}, {"code", "insufficient_usdt"},
                {"available_usdt", decimal_text(account.contains("USDT") ? account.at("USDT") : Decimal(0))},
                {"required_usdt", decimal_text(budget)}};
            failed = true; reason = "Insufficient USDT"; finish(); return;
        }
        start_arbitrage();
    }

    void next_leg() {
        if (leg_number == opportunity.path.size()) { finish(); return; }
        if (config.latency_timestamps && leg_number == 0) preparation.order_begin = steady_time_ns();
        const auto& leg = opportunity.path[leg_number];
        const auto& market = markets.at(leg.index);
        const auto order = prepare_fast(leg, holdings[leg.buy ? market.quote : market.base], leg_number);
        if (config.latency_timestamps && leg_number == 0) preparation.order_end = steady_time_ns();
        if (!order) { failed = true; reason = "Cannot size next leg"; finish(); return; }
        send(*order);
    }

    Gateway::Callback callback() {
        return [weak = weak_from_this()](OrderReport report) {
            if (auto self = weak.lock()) {
                if (!self->pending || report.client_id != self->pending->client_id) return;
                self->gateway_busy = false;
                self->report(report);
            }
        };
    }
    bool is_first_leg() const {
        return pending && !pending->initial_clear && leg_number == 0;
    }
    void first_leg_event(std::string_view event, nlohmann::json extra = nlohmann::json::object()) const {
        if (!event_observer || !is_first_leg()) return;
        const auto& market = markets.at(pending->symbol_index);
        nlohmann::json fields = {{"cycle_id", cycle}, {"client_id", pending->client_id},
            {"group_index", opportunity.group_index}, {"leg", 0}, {"symbol", market.symbol},
            {"side", pending->buy ? "BUY" : "SELL"}, {"type", "LIMIT"}, {"time_in_force", "FOK"},
            {"price", pending->price}, {"quantity", pending->quantity},
            {"execution_mode", config.execution_mode}, {"budget_usdt", decimal_text(budget)},
            {"submitted_cycles", submitted_cycles}};
        fields.update(extra);
        event_observer(event, fields);
    }
    void send(OrderRequest order) {
        order.client_id = cycle + "-" + std::to_string(++order_number);
        if(config.latency_timestamps) order.latency = {
            {"cycle_id",cycle},{"leg",leg_number},{"initial_clear",order.initial_clear},
            {"trigger_receive_sequence",opportunity.trigger_receive_sequence},
            {"trigger_symbol_index",opportunity.trigger_symbol_index},
            {"book_receive_sequences",opportunity.receive_sequences},{"book_update_ids",opportunity.book_update_ids},
            {"book_received_ns",opportunity.received_steady_ns},{"market_received_ns",opportunity.market_received_ns},
            {"market_received_realtime_ns",opportunity.market_received_realtime_ns},
            {"market_processed_ns",opportunity.market_processed_ns},{"edge_found_ns",opportunity.edge_found_ns},
            {"kernel_rx",opportunity.kernel_rx},{"cycle_started_ns",cycle_started_ns},
            {"previous_report_ns",previous_report_ns}};
        if (config.latency_timestamps) {
            if (!order.initial_clear && leg_number == 0) {
                order.latency.update({{"cycle_post_begin_ns",preparation.post_begin},
                    {"cycle_begin_ns",preparation.begin}, {"balance_read_begin_ns",preparation.balance_begin},
                    {"balance_read_end_ns",preparation.balance_end}, {"snapshot_begin_ns",preparation.snapshot_begin},
                    {"snapshot_end_ns",preparation.snapshot_end}, {"preflight_end_ns",preparation.preflight_end},
                    {"first_order_begin_ns",preparation.order_begin}, {"first_order_end_ns",preparation.order_end}});
            }
            // Include latency metadata construction in the preparation boundary.
            order.latency["order_prepared_ns"] = steady_time_ns();
        }
        pending = std::move(order); accounted = {};
        state = pending->initial_clear ? State::ClearingInitialPosition : State::LegPending;
        publish();
        first_leg_event("arbitrage_first_leg_submit_attempt");
        try {
            if(config.latency_timestamps) {
                pending->latency["gateway_submit_ns"]=steady_time_ns();
                pending->latency["gateway_submit_realtime_ns"]=realtime_ns();
            }
            gateway_busy = true;
            gateway.submit(*pending, callback());
            if (pending->initial_clear) ++startup_orders_submitted;
            else if (!cycle_submitted) { cycle_submitted = true; ++submitted_cycles; }
            first_leg_event("arbitrage_first_leg_submitted", {{"submission_boundary", "gateway"}});
            publish();
        }
        catch (const std::exception& error) {
            gateway_busy = false;
            first_leg_event("arbitrage_first_leg_submit_failed", {{"error", error.what()}});
            const auto message = std::string("Order submission failed: ") + error.what();
            if (pending && pending->initial_clear) halt(message);
            else fail_cycle(message);
        }
    }

    void report(const OrderReport& value) {
        if (!pending || value.client_id != pending->client_id || state == State::Halted) return;
        if (value.revision <= accounted.revision) return;
        previous_report_ns=steady_time_ns();
        if(config.latency_timestamps && event_observer) event_observer("execution_order_report", {
            {"cycle_id",cycle},{"client_id",value.client_id},{"leg",leg_number},
            {"report_received_ns",previous_report_ns},{"status_code",static_cast<int>(value.status)},
            {"filled_qty",decimal_text(value.filled_qty)},{"filled_quote",decimal_text(value.filled_quote)},
            {"failure",value.failure}});
        if (!value.failure.empty()) {
            order_failure = value.failure;
            if (event_observer) event_observer("execution_order_failed", {
                {"cycle_id", cycle}, {"client_id", pending->client_id}, {"leg", leg_number},
                {"symbol", markets.at(pending->symbol_index).symbol}, {"side", pending->buy ? "BUY" : "SELL"},
                {"price", pending->price}, {"quantity", pending->quantity}, {"initial_clear", pending->initial_clear},
                {"execution_mode", config.execution_mode},
                {"order_status", value.status == OrderStatus::Unknown ? "UNKNOWN" : "REJECTED"},
                {"failure", order_failure}});
        }
        if (value.status == OrderStatus::Unknown) {
            first_leg_event("arbitrage_first_leg_report", {{"order_status", "UNKNOWN"}, {"revision", value.revision}, {"failure", value.failure}});
            if (pending->initial_clear) halt("Order outcome unknown");
            else fail_cycle("Order outcome unknown");
            return;
        }
        const Decimal quantity_limit(pending->quantity);
        if (value.filled_qty < accounted.filled_qty || value.filled_qty > quantity_limit ||
            value.filled_quote < accounted.filled_quote) {
            if (pending->initial_clear) halt("Invalid cumulative fill");
            else fail_cycle("Invalid cumulative fill");
            return;
        }
        const Decimal filled_delta = value.filled_qty - accounted.filled_qty;
        const Decimal quote_delta = value.filled_quote - accounted.filled_quote;
        const auto& market = markets.at(pending->symbol_index);
        holdings[market.base] += pending->buy ? filled_delta : -filled_delta;
        holdings[market.quote] += pending->buy ? -quote_delta : quote_delta;
        for (const auto& [asset, commission] : value.commissions)
            holdings[asset] -= commission - accounted.commissions[asset];
        accounted = value;
        const char* report_status = "UNKNOWN";
        switch (value.status) {
        case OrderStatus::Open: report_status = "OPEN"; break;
        case OrderStatus::PartiallyFilled: report_status = "PARTIALLY_FILLED"; break;
        case OrderStatus::Filled: report_status = "FILLED"; break;
        case OrderStatus::Canceled: report_status = "CANCELED"; break;
        case OrderStatus::Expired: report_status = "EXPIRED"; break;
        case OrderStatus::Rejected: report_status = "REJECTED"; break;
        case OrderStatus::Unknown: break;
        }
        first_leg_event("arbitrage_first_leg_report", {{"order_status", report_status},
            {"revision", value.revision}, {"filled_qty", decimal_text(value.filled_qty)},
            {"filled_quote", decimal_text(value.filled_quote)}, {"commissions", balance_json(value.commissions)},
            {"accounting_complete", value.accounting_complete}, {"failure", value.failure}});
        publish();
        if (waiting_gateway) {
            // The gateway has completed its request and balance update. Keep
            // the timeout result, but never advance a timed-out cycle.
            waiting_gateway = false;
            pending.reset(); state = State::Idle; publish(); return;
        }
        if (!terminal(value.status)) return;
        if (!value.accounting_complete) {
            if (pending->initial_clear) halt("Incomplete fill accounting");
            else fail_cycle("Incomplete fill accounting");
            return;
        }
        const bool initial_clear = pending->initial_clear;
        const bool filled = value.status == OrderStatus::Filled && value.filled_qty == quantity_limit;
        const auto asset = pending->buy ? market.quote : market.base;
        const auto client_id = pending->client_id;
        pending.reset();
        if (initial_clear) {
            timeout.cancel();
            cleanup_results[asset].update({{"code", value.filled_qty > 0 ? "filled" : "not_filled"},
                {"client_id", client_id}, {"symbol", market.symbol}, {"order_status", report_status},
                {"filled_qty", decimal_text(value.filled_qty)}, {"filled_quote", decimal_text(value.filled_quote)},
                {"remaining_balance", decimal_text(holdings[asset])}, {"failure", value.failure}});
            cleanup_event("startup_cleanup_order_report", {{"asset", asset}, {"result", cleanup_results[asset]}});
            clear_initial_position(); return;
        }
        if (!filled) { failed = true; reason = "Arbitrage leg did not fill completely"; finish(); return; }
        // Temporary test: stop after the first-leg terminal response.
        if (leg_number == 0) {
            fail_cycle("First-leg response test stop");
            return;
        }
        ++leg_number;
        next_leg();
    }

    void finish() {
        timeout.cancel(); pending.reset();
        if (failed) ++failures;
        else if (leg_number == opportunity.path.size()) ++completed;
        state = State::Idle;
        publish();
    }

    bool start(const ArbitrageOpportunity& candidate) {
        if (state != State::Idle || stopping) { ++rejected_busy; return false; }
        if (options.max_cycles != 0 && submitted_cycles >= options.max_cycles) {
            ++rejected_limit; publish(); return false;
        }
        if (candidate.group_index >= config.trading_groups.size() || candidate.input_usdt <= 0 ||
            candidate.input_usdt > config.max_arbitrage_usdt) return false;
        const auto& group = config.trading_groups[candidate.group_index];
        const bool known = std::any_of(group.paths.begin(), group.paths.end(), [&](const TradePath& path) {
            for (std::size_t i = 0; i < path.size(); ++i)
                if (path[i].index != candidate.path[i].index || path[i].buy != candidate.path[i].buy) return false;
            return true;
        });
        if (!known) return false;
        cycle_started_ns=steady_time_ns(); previous_report_ns=0;
        preparation = {};
        state = State::Validating; opportunity = candidate; budget = Decimal(candidate.input_usdt);
        failed = false; cycle_submitted = false; reason.clear(); rejection = nlohmann::json::object(); order_failure = nlohmann::json::object(); pending.reset(); holdings.clear();
        leg_number = 0; order_number = 0;
        cycle = std::to_string(wall_time_us()) + "-" + std::to_string(++next_cycle);
        ++accepted;
        publish();
        arm_timeout();
        if (config.latency_timestamps) preparation.post_begin = steady_time_ns();
        boost::asio::post(io, [weak = weak_from_this()] { if (auto self = weak.lock()) self->begin(); });
        return true;
    }

    void stop() {
        stopping = true; cleanup_wait.cancel();
        if (state != State::Idle && state != State::Halted) halt("Stopped during execution");
    }
};

ArbitrageExecutor::ArbitrageExecutor(boost::asio::io_context& io, const Config& config,
    OrderBookManager& books, std::vector<Market> markets, Gateway& gateway, Options options, Observer observer, EventObserver events)
    : impl_(std::make_shared<Impl>(io, config, books, std::move(markets), gateway,
                                  std::move(options), std::move(observer), std::move(events))) {}
ArbitrageExecutor::~ArbitrageExecutor() { impl_->timeout.cancel(); impl_->cleanup_wait.cancel(); }
void ArbitrageExecutor::start_initial_cleanup() { impl_->start_initial_cleanup(); }
bool ArbitrageExecutor::try_start(const ArbitrageOpportunity& opportunity) { return impl_->start(opportunity); }
void ArbitrageExecutor::on_report(const OrderReport& report) { impl_->report(report); }
void ArbitrageExecutor::stop() { impl_->stop(); }
bool ArbitrageExecutor::busy() const { return impl_->state != State::Idle; }
State ArbitrageExecutor::state() const { return impl_->state; }
nlohmann::json ArbitrageExecutor::stats() const { return impl_->status(); }
} // namespace triangular::execution
