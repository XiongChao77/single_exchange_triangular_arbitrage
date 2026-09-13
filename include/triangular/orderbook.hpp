#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <filesystem>
#include <mutex>
#include <optional>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace triangular {

struct TradeLeg {
    std::size_t index;
    bool buy; // Quote -> base at ask; otherwise base -> quote at bid.
};
using TradePath = std::array<TradeLeg, 3>;

struct TradeGroup {
    std::array<std::size_t, 3> symbol_indices;
    std::array<TradePath, 2> paths; // Both start and end in USDT.
};

// Indicative top-of-book plan, before exchange filters/rounding and actual fills.
struct ArbitrageOpportunity {
    std::size_t group_index{};
    TradePath path{};
    std::array<long double, 3> prices{}; // Quote asset per base asset.
    std::array<long double, 3> quantities{}; // Base asset, before commission, BUY and SELL alike.
    std::array<std::int64_t, 3> book_update_ids{};
    std::array<std::int64_t, 3> received_steady_ns{};
    long double input_usdt{};
    long double output_usdt{};
    long double profit_usdt{};
    long double net_return{};
    std::array<std::uint64_t,3> receive_sequences{};
    std::uint64_t trigger_receive_sequence{};
    std::size_t trigger_symbol_index{};
    std::int64_t market_received_ns{}, market_received_realtime_ns{}, market_processed_ns{}, edge_found_ns{};
    nlohmann::json kernel_rx = nlohmann::json::object();
};

struct Config {
    std::string execution_mode = "disabled"; // disabled, paper, or live.
    bool live_test_mode = false; // Defaults max_cycles to two when the limit is omitted.
    bool latency_timestamps = false; // Enable Linux software socket timestamps and latency events.
    std::filesystem::path tls_keylog_file; // Opt-in capture decryption; empty disables it.
    std::size_t max_cycles = 0; // Submitted arbitrage cycles per process; zero means unlimited.
    std::filesystem::path hmac_api_key_file;
    std::filesystem::path hmac_secret_key_file;
    std::uint64_t execution_timeout_ms = 1000;
    double max_arbitrage_usdt = 100.0; // Maximum initial USDT per triangle execution.
    double commission_taker = 0.0005;
    double edge_threshold = 0.0001; // Net return ratio, not percent.
    std::vector<TradeGroup> trading_groups;
    std::string host = "stream.binance.com";
    std::string port = "9443";
    std::filesystem::path ca_file;
    std::string rest_host = "api.binance.com";
    std::string rest_port = "443";
    std::vector<std::string> symbols; // Unique symbols in first-appearance order.
    std::unordered_map<std::string, std::size_t> symbol_indices;
    std::vector<std::array<std::size_t, 3>> triangle_indices;
};

Config load_config(const std::filesystem::path& path);
std::vector<TradeGroup> validate_arbitrage(const Config& config, const nlohmann::json& exchange_info);
std::string stream_target(const std::vector<std::string>& symbols);

// Latest 20-level partial depth snapshot. The duplicated best bid/ask fields are
// the hot path used by edge scanning and execution.
// Decimal value = mantissa * 10^exponent; no floating-point rounding on ingestion.
struct OrderBook {
    struct Level {
        std::int64_t price{};
        std::int64_t qty{};
        std::int8_t price_exponent{};
        std::int8_t qty_exponent{};
    };
    std::string symbol;
    std::optional<std::int64_t> event_time_us; // Unavailable in partial depth payloads.
    std::int64_t received_time_us{};
    std::int64_t received_steady_ns{};
    std::uint64_t receive_sequence{};
    std::int64_t book_update_id{};
    std::int8_t price_exponent{};
    std::int8_t qty_exponent{};
    std::int64_t bid_price{};
    std::int64_t bid_qty{};
    std::int64_t ask_price{};
    std::int64_t ask_qty{};
    std::array<Level, 20> bids{};
    std::array<Level, 20> asks{};
    std::size_t bid_levels{};
    std::size_t ask_levels{};
};

OrderBook decode_partial_depth(const nlohmann::json& message,
                               std::int64_t received_time_us);

struct OrderBookStats {
    std::size_t symbols;
    std::size_t populated;
    std::uint64_t received;
};

// Fixed slots after construction. Reads return copies protected by the same lock
// as updates, so readers never observe partially written orderbooks.
class OrderBookManager {
public:
    explicit OrderBookManager(const Config& config);
    std::size_t index_of(const std::string& symbol) const;
    void update(std::size_t index, OrderBook orderbook);
    // Scan both paths of each group containing the updated symbol index.
    // Returns the first qualifying path; clears result when no opportunity exists.
    bool scan_edge(std::size_t index, ArbitrageOpportunity& result);
    std::optional<OrderBook> get(std::size_t index) const;
    std::vector<std::optional<OrderBook>> snapshot() const;
    OrderBookStats stats() const;
private:
    std::unordered_map<std::string, std::size_t> indices_;
    mutable std::mutex mutex_;
    std::vector<std::optional<OrderBook>> orderbooks_;
    std::vector<TradeGroup> groups_;
    long double max_arbitrage_usdt_;
    long double fee_multiplier_;
    long double edge_threshold_;
    std::size_t populated_ = 0;
    std::uint64_t received_ = 0;
};

} // namespace triangular
