#include "triangular/execution/execution.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <algorithm>
#include <sstream>

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
    boost::asio::steady_timer timeout;
    State state = State::Idle;
    bool stopping = false, failed = false;
    std::string reason, cycle;
    std::uint64_t next_cycle = 0, accepted = 0, completed = 0, failures = 0;
    std::uint64_t rejected_busy = 0, rejected_limit = 0;
    unsigned order_number = 0, clear_orders = 0;
    std::size_t leg_number = 0;
    ArbitrageOpportunity opportunity;
    Balances holdings, dust;
    Decimal budget = 0;
    std::optional<OrderRequest> pending;
    OrderReport accounted;

    Impl(boost::asio::io_context& context, const Config& c, OrderBookManager& manager,
         std::vector<Market> market_list, Gateway& order_gateway, Options execution_options, Observer obs)
        : io(context), config(c), books(manager), markets(std::move(market_list)), gateway(order_gateway),
          options(std::move(execution_options)), observer(std::move(obs)), timeout(io) {
        if (markets.size() != config.symbols.size() || options.max_book_age.count() <= 0 ||
            options.execution_timeout.count() <= 0 || !options.max_initial_clear_orders)
            throw std::runtime_error("Invalid execution options");
    }

    nlohmann::json status() const {
        nlohmann::json result = {{"state", state_name(state)}, {"cycle_id", cycle}, {"reason", reason},
            {"accepted", accepted}, {"completed", completed}, {"failed", failures},
            {"rejected_busy", rejected_busy}, {"rejected_limit", rejected_limit},
            {"max_cycles", options.max_cycles}, {"holdings", balance_json(holdings)},
            {"dust", balance_json(dust)}, {"budget_usdt", decimal_text(budget)},
            {"leg", leg_number}, {"stopping", stopping}};
        if (pending) result["pending"] = {{"client_id", pending->client_id},
            {"symbol_index", pending->symbol_index}, {"buy", pending->buy},
            {"price", pending->price}, {"quantity", pending->quantity},
            {"initial_clear", pending->initial_clear}, {"filled_qty", decimal_text(accounted.filled_qty)},
            {"filled_quote", decimal_text(accounted.filled_quote)}};
        return result;
    }

    void publish() const { if (observer) observer(status()); }
    void halt(std::string why) {
        timeout.cancel(); state = State::Halted; reason = std::move(why); ++failures; publish();
    }
    bool fresh(const OrderBook& book) const {
        const auto age = steady_time_ns() - book.received_steady_ns;
        return book.received_steady_ns > 0 && age >= 0 &&
            age <= std::chrono::duration_cast<std::chrono::nanoseconds>(options.max_book_age).count();
    }
    bool valid(const Market& market, const Decimal& price, const Decimal& quantity) const {
        return price > 0 && quantity > 0 && (market.min_price == 0 || price >= market.min_price) &&
            (market.max_price == 0 || price <= market.max_price) && quantity >= market.min_qty &&
            (market.max_qty == 0 || quantity <= market.max_qty) && price * quantity >= market.min_notional &&
            (market.max_notional == 0 || price * quantity <= market.max_notional);
    }

    std::optional<OrderRequest> prepare_from_book(const TradeLeg& leg, const Decimal& amount,
            bool initial_clear, const std::vector<std::optional<OrderBook>>& snapshot) const {
        const auto& book = snapshot.at(leg.index);
        if (!book || !fresh(*book)) return {};
        const auto& market = markets.at(leg.index);
        const Decimal raw_price = price_of(*book, leg.buy), liquidity = quantity_of(*book, leg.buy);
        const Decimal price = leg.buy ? Decimal(ceil(raw_price / market.tick) * market.tick)
                                      : down(raw_price, market.tick);
        Decimal quantity = down(leg.buy ? amount / price : amount, market.step);
        if (initial_clear) quantity = down(std::min(quantity, liquidity), market.step);
        if (!valid(market, price, quantity) || quantity > liquidity) return {};
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

    bool preflight() const {
        auto balances = holdings;
        const auto snapshot = books.snapshot();
        const Decimal fee = Decimal(1) - Decimal(config.commission_taker);
        for (std::size_t i = 0; i < opportunity.path.size(); ++i) {
            const auto& leg = opportunity.path[i];
            const auto& market = markets.at(leg.index);
            const auto order = prepare_from_book(leg, balances[leg.buy ? market.quote : market.base], false, snapshot);
            if (!order) return false;
            const Decimal price(order->price), quantity(order->quantity);
            const Decimal accepted_price = floor(Decimal(opportunity.prices[i]) / market.tick + Decimal("0.5")) * market.tick;
            if ((leg.buy && price > accepted_price) || (!leg.buy && price < accepted_price)) return false;
            if (leg.buy) {
                balances[market.quote] -= price * quantity;
                balances[market.base] += quantity * fee;
            } else {
                balances[market.base] -= quantity;
                balances[market.quote] += quantity * price * fee;
            }
        }
        return balances["USDT"] > budget * (Decimal(1) + Decimal(config.edge_threshold));
    }

    void start_arbitrage() {
        holdings = {{"USDT", budget}};
        if (!preflight()) { failed = true; reason = "Initial preflight failed"; finish(); return; }
        leg_number = 0;
        next_leg();
    }

    void clear_initial_position() {
        state = State::ClearingInitialPosition;
        const auto snapshot = books.snapshot();
        for (auto& [asset, amount] : holdings) {
            if (asset == "USDT" || amount <= 0) continue;
            std::optional<std::size_t> exit_index;
            for (const auto index : config.trading_groups.at(opportunity.group_index).symbol_indices)
                if (markets[index].base == asset && markets[index].quote == "USDT") exit_index = index;
            if (!exit_index) { halt("No USDT market for initial asset"); return; }
            const auto& market = markets.at(*exit_index);
            const auto& book = snapshot.at(*exit_index);
            if (!book || !fresh(*book)) { halt("No fresh book for initial position clear"); return; }
            const Decimal quantity = down(amount, market.step), price = price_of(*book, false);
            if (quantity == 0 || quantity < market.min_qty || price * quantity < market.min_notional) {
                dust[asset] += amount; amount = 0; continue;
            }
            if (++clear_orders > options.max_initial_clear_orders) { halt("Initial position clear limit reached"); return; }
            auto order = prepare_from_book({*exit_index, false}, amount, true, snapshot);
            if (!order) { halt("Cannot clear initial position"); return; }
            send(*order); return;
        }
        start_arbitrage();
    }

    void begin() {
        if (state != State::Validating || stopping) return;
        const auto account = gateway.balances();
        if (!account.contains("USDT") || account.at("USDT") < budget) {
            failed = true; reason = "Insufficient USDT"; finish(); return;
        }
        holdings = {{"USDT", budget}};
        for (const auto index : config.trading_groups.at(opportunity.group_index).symbol_indices)
            for (const auto& asset : {markets[index].base, markets[index].quote})
                if (asset != "USDT" && account.contains(asset) && account.at(asset) > 0)
                    holdings[asset] = account.at(asset);
        const bool has_position = std::any_of(holdings.begin(), holdings.end(), [](const auto& item) {
            return item.first != "USDT" && item.second > 0;
        });
        if (has_position) clear_initial_position(); else start_arbitrage();
    }

    void next_leg() {
        if (leg_number == opportunity.path.size()) { finish(); return; }
        const auto& leg = opportunity.path[leg_number];
        const auto& market = markets.at(leg.index);
        const auto order = prepare_fast(leg, holdings[leg.buy ? market.quote : market.base], leg_number);
        if (!order) { failed = true; reason = "Cannot size next leg"; finish(); return; }
        send(*order);
    }

    Gateway::Callback callback() {
        return [weak = weak_from_this()](OrderReport report) {
            if (auto self = weak.lock()) self->report(report);
        };
    }
    void send(OrderRequest order) {
        order.client_id = cycle + "-" + std::to_string(++order_number);
        pending = std::move(order); accounted = {};
        state = pending->initial_clear ? State::ClearingInitialPosition : State::LegPending;
        publish();
        try { gateway.submit(*pending, callback()); }
        catch (const std::exception& error) { halt(std::string("Order submission failed: ") + error.what()); }
    }

    void report(const OrderReport& value) {
        if (!pending || value.client_id != pending->client_id || state == State::Halted) return;
        if (value.status == OrderStatus::Unknown) { halt("Order outcome unknown"); return; }
        if (value.revision <= accounted.revision) return;
        const Decimal quantity_limit(pending->quantity);
        if (value.filled_qty < accounted.filled_qty || value.filled_qty > quantity_limit ||
            value.filled_quote < accounted.filled_quote) { halt("Invalid cumulative fill"); return; }
        const Decimal filled_delta = value.filled_qty - accounted.filled_qty;
        const Decimal quote_delta = value.filled_quote - accounted.filled_quote;
        const auto& market = markets.at(pending->symbol_index);
        holdings[market.base] += pending->buy ? filled_delta : -filled_delta;
        holdings[market.quote] += pending->buy ? -quote_delta : quote_delta;
        for (const auto& [asset, commission] : value.commissions)
            holdings[asset] -= commission - accounted.commissions[asset];
        accounted = value;
        publish();
        if (!terminal(value.status)) return;
        if (!value.accounting_complete) { halt("Incomplete fill accounting"); return; }
        const bool initial_clear = pending->initial_clear;
        const bool filled = value.status == OrderStatus::Filled && value.filled_qty == quantity_limit;
        const bool made_progress = value.filled_qty > 0;
        pending.reset();
        if (initial_clear) {
            if (!made_progress) { halt("Initial position clear did not fill"); return; }
            clear_initial_position(); return;
        }
        if (!filled) { failed = true; reason = "Arbitrage leg did not fill completely"; finish(); return; }
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
        if (options.max_cycles != 0 && accepted >= options.max_cycles) {
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
        state = State::Validating; opportunity = candidate; budget = Decimal(candidate.input_usdt);
        failed = false; reason.clear(); pending.reset(); holdings.clear();
        leg_number = 0; order_number = 0; clear_orders = 0;
        cycle = std::to_string(wall_time_us()) + "-" + std::to_string(++next_cycle);
        ++accepted;
        publish();
        timeout.expires_after(options.execution_timeout);
        timeout.async_wait([weak = weak_from_this()](const boost::system::error_code& error) {
            if (!error)
                if (auto self = weak.lock(); self && self->state != State::Idle && self->state != State::Halted)
                    self->halt("Execution timeout");
        });
        boost::asio::post(io, [weak = weak_from_this()] { if (auto self = weak.lock()) self->begin(); });
        return true;
    }

    void stop() {
        stopping = true;
        if (state != State::Idle && state != State::Halted) halt("Stopped during execution");
    }
};

ArbitrageExecutor::ArbitrageExecutor(boost::asio::io_context& io, const Config& config,
    OrderBookManager& books, std::vector<Market> markets, Gateway& gateway, Options options, Observer observer)
    : impl_(std::make_shared<Impl>(io, config, books, std::move(markets), gateway,
                                  std::move(options), std::move(observer))) {}
ArbitrageExecutor::~ArbitrageExecutor() { impl_->timeout.cancel(); }
bool ArbitrageExecutor::try_start(const ArbitrageOpportunity& opportunity) { return impl_->start(opportunity); }
void ArbitrageExecutor::on_report(const OrderReport& report) { impl_->report(report); }
void ArbitrageExecutor::stop() { impl_->stop(); }
bool ArbitrageExecutor::busy() const { return impl_->state != State::Idle; }
State ArbitrageExecutor::state() const { return impl_->state; }
nlohmann::json ArbitrageExecutor::stats() const { return impl_->status(); }
} // namespace triangular::execution
