#pragma once

#include "triangular/orderbook.hpp"
#include <boost/asio/io_context.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <functional>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>

namespace triangular::execution {
using Decimal = boost::multiprecision::cpp_dec_float_50;
using Balances = std::map<std::string, Decimal>;

// Wire quantities are decimal strings; rounding uses exchange tick/step sizes.
std::string decimal_text(const Decimal& value);
struct Market {
    std::string symbol, base, quote;
    Decimal tick, step, min_qty, max_qty, min_price, max_price, min_notional, max_notional;
};
std::vector<Market> load_markets(const Config&, const nlohmann::json& exchange_info);

struct Options {
    std::chrono::milliseconds max_book_age{500};
    std::chrono::milliseconds execution_timeout{1000};
    unsigned max_initial_clear_orders = 6;
    std::size_t max_cycles = 0; // Zero means unlimited; live test mode sets this to 10.
};

enum class OrderStatus { Open, PartiallyFilled, Filled, Canceled, Expired, Rejected, Unknown };
struct OrderRequest {
    std::string client_id;
    std::size_t symbol_index{};
    bool buy{};
    std::string price, quantity;
    bool initial_clear{}; // Normal orders use LIMIT FOK; startup position clearing uses LIMIT IOC.
};
struct OrderReport {
    std::string client_id;
    std::uint64_t revision{}; // Monotonic per order, normalized by the gateway.
    OrderStatus status = OrderStatus::Unknown;
    Decimal filled_qty = 0, filled_quote = 0;
    Balances commissions; // CUMULATIVE per asset, not the last fill's commission.
    bool accounting_complete = true; // Terminal state may precede retrieval of all trades/fees.
};

// All methods/callbacks run on the SAME single io_context thread as the executor.
// Callbacks must be asynchronous. Reports combine query/stream fills with cumulative fees.
// An uncertain submission is Unknown, never Rejected. cancel/query target client_id.
class Gateway {
public:
    using Callback = std::function<void(OrderReport)>;
    virtual ~Gateway() = default;
    virtual Balances balances() const = 0; // Locally cached account view used before a cycle starts.
    virtual void submit(const OrderRequest&, Callback) = 0;
    virtual void query(const OrderRequest&, Callback) = 0;
    virtual void cancel(const OrderRequest&, Callback) = 0;
};

enum class State { Idle, Validating, LegPending, ClearingInitialPosition, Halted };
const char* state_name(State);

class ArbitrageExecutor {
public:
    using Observer = std::function<void(const nlohmann::json&)>;
    ArbitrageExecutor(boost::asio::io_context&, const Config&, OrderBookManager&,
                      std::vector<Market>, Gateway&, Options = {}, Observer = {});
    ~ArbitrageExecutor();
    ArbitrageExecutor(const ArbitrageExecutor&) = delete;
    ArbitrageExecutor& operator=(const ArbitrageExecutor&) = delete;
    // Call ONLY on the io_context thread. Occupies execution synchronously, then posts validation.
    bool try_start(const ArbitrageOpportunity&);
    void on_report(const OrderReport&); // Optional normalized user-data-stream reports.
    void stop(); // Reject new cycles; an active cycle is halted without automatic cleanup.
    bool busy() const;
    State state() const;
    nlohmann::json stats() const;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// Public order books + simulated wallet. Never sends private exchange requests.
class PaperGateway final : public Gateway {
public:
    PaperGateway(boost::asio::io_context&, OrderBookManager&, std::vector<Market>,
                 Decimal taker_fee, Balances initial_balances);
    ~PaperGateway() override;
    Balances balances() const override;
    void submit(const OrderRequest&, Callback) override;
    void query(const OrderRequest&, Callback) override;
    void cancel(const OrderRequest&, Callback) override;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// Signed Binance Spot REST adapter. Network work runs on a private worker;
// callbacks are posted back to the supplied io_context thread.
class BinanceGateway final : public Gateway {
public:
    BinanceGateway(boost::asio::io_context&, const Config&, std::vector<Market>);
    ~BinanceGateway() override;
    Balances balances() const override;
    void submit(const OrderRequest&, Callback) override;
    void query(const OrderRequest&, Callback) override;
    void cancel(const OrderRequest&, Callback) override;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// Exposed for deterministic verification against Binance's published HMAC vector.
std::string hmac_sha256_hex(const std::string& secret, const std::string& payload);
} // namespace triangular::execution
