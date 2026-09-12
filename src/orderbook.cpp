#include "triangular/orderbook.hpp"

#include <array>
#include <cmath>
#include <limits>
#include <algorithm>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace triangular {
namespace {
struct Decimal {
    std::int64_t mantissa = 0;
    int scale = 0;
};
Decimal decimal(const nlohmann::json& value) {
    const auto& text = value.get_ref<const std::string&>();
    if (text.empty()) throw std::runtime_error("Empty decimal");
    Decimal result;
    bool dot = false;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (c == '.' && !dot && i > 0 && i + 1 < text.size()) { dot = true; continue; }
        if (c < '0' || c > '9') throw std::runtime_error("Invalid decimal");
        if (result.mantissa > (std::numeric_limits<std::int64_t>::max() - (c - '0')) / 10)
            throw std::runtime_error("Decimal mantissa overflow");
        result.mantissa = result.mantissa * 10 + (c - '0');
        if (dot && ++result.scale > 127) throw std::runtime_error("Decimal scale overflow");
    }
    return result;
}
struct ScaleFactor {
    std::int64_t multiplier;
    std::int64_t max_mantissa;
};
// Built at compile time: 10^0 through 10^18 fit in int64_t.
constexpr auto scale_factors = [] {
    std::array<ScaleFactor, 19> factors{};
    std::int64_t multiplier = 1;
    for (std::size_t i = 0; i < factors.size(); ++i) {
        factors[i] = {multiplier, std::numeric_limits<std::int64_t>::max() / multiplier};
        if (i + 1 < factors.size()) multiplier *= 10;
    }
    return factors;
}();
std::int64_t rescale(Decimal value, int scale) {
    const auto delta = static_cast<std::size_t>(scale - value.scale);
    if (delta == 0 || value.mantissa == 0) return value.mantissa;
    if (delta >= scale_factors.size()) throw std::runtime_error("Decimal rescaling overflow");
    const auto& factor = scale_factors[delta];
    if (value.mantissa > factor.max_mantissa) throw std::runtime_error("Decimal rescaling overflow");
    return value.mantissa * factor.multiplier;
}
void decimal_pair(const nlohmann::json& bid, const nlohmann::json& ask,
                  std::int64_t& bid_value, std::int64_t& ask_value, std::int8_t& exponent) {
    auto b = decimal(bid), a = decimal(ask);
    const auto scale = std::max(b.scale, a.scale);
    exponent = static_cast<std::int8_t>(-scale);
    bid_value = rescale(b, scale);
    ask_value = rescale(a, scale);
}
std::filesystem::path resolve(const std::filesystem::path& config, const std::string& value) {
    const std::filesystem::path path(value);
    return path.is_absolute() ? path : (config.parent_path() / path).lexically_normal();
}
} // namespace

Config load_config(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("Cannot open configuration file");
    nlohmann::json j;
    try { input >> j; } catch (...) { throw std::runtime_error("Invalid configuration JSON"); }
    Config c;
    c.execution_mode = j.value("execution_mode", c.execution_mode);
    if (c.execution_mode != "disabled" && c.execution_mode != "paper" && c.execution_mode != "live")
        throw std::runtime_error("execution_mode must be disabled, paper, or live");
    c.live_test_mode = j.value("live_test_mode", c.live_test_mode);
    if (j.contains("api_key_file")) c.api_key_file = resolve(path, j.at("api_key_file").get<std::string>());
    if (j.contains("secret_key_file")) c.secret_key_file = resolve(path, j.at("secret_key_file").get<std::string>());
    if (c.execution_mode == "live" && (c.api_key_file.empty() || c.secret_key_file.empty()))
        throw std::runtime_error("live execution requires api_key_file and secret_key_file");
    c.execution_timeout_ms = j.value("execution_timeout_ms", c.execution_timeout_ms);
    if (c.execution_timeout_ms == 0 || c.execution_timeout_ms > 60000)
        throw std::runtime_error("execution_timeout_ms must be in [1, 60000]");
    c.max_arbitrage_usdt = j.value("max_arbitrage_usdt", c.max_arbitrage_usdt);
    if (!std::isfinite(c.max_arbitrage_usdt) || c.max_arbitrage_usdt <= 0)
        throw std::runtime_error("max_arbitrage_usdt must be finite and positive");
    c.commission_taker = j.value("commission_taker", c.commission_taker);
    c.edge_threshold = j.value("edge_threshold", c.edge_threshold);
    if (!std::isfinite(c.commission_taker) || c.commission_taker < 0 || c.commission_taker >= 1)
        throw std::runtime_error("commission_taker must be in [0, 1)");
    if (!std::isfinite(c.edge_threshold) || c.edge_threshold < 0)
        throw std::runtime_error("edge_threshold must be finite and non-negative");
    const auto& groups = j.at("triangles");
    if (!groups.is_array() || groups.empty()) throw std::runtime_error("Expected a non-empty arbitrage list");
    for (std::size_t group = 0; group < groups.size(); ++group) {
        const auto& triangle = groups[group];
        const auto prefix = "Arbitrage group " + std::to_string(group) + ": ";
        if (!triangle.is_array() || triangle.size() != 3)
            throw std::runtime_error(prefix + "expected three symbols");
        std::array<std::size_t, 3> indices{};
        for (std::size_t leg = 0; leg < 3; ++leg) {
            if (!triangle[leg].is_string() || triangle[leg].get_ref<const std::string&>().empty())
                throw std::runtime_error(prefix + "expected non-empty symbol strings");
            auto symbol = triangle[leg].get<std::string>();
            for (auto& ch : symbol) if (ch >= 'a' && ch <= 'z') ch = static_cast<char>(ch - 'a' + 'A');
            const auto [entry, added] = c.symbol_indices.emplace(symbol, c.symbols.size());
            if (added) c.symbols.push_back(std::move(symbol));
            indices[leg] = entry->second;
        }
        if (indices[0] == indices[1] || indices[0] == indices[2] || indices[1] == indices[2])
            throw std::runtime_error(prefix + "symbols must be distinct");
        c.triangle_indices.push_back(indices);
    }
    if (c.symbols.size() > 1024) throw std::runtime_error("Too many subscribed streams");
    c.host = j.value("host", c.host);
    c.port = j.value("port", c.port);
    c.rest_host = j.value("rest_host", c.rest_host);
    c.rest_port = j.value("rest_port", c.rest_port);
    if (j.contains("ca_file")) c.ca_file = resolve(path, j.at("ca_file").get<std::string>());
    for (const auto& host : {c.host, c.rest_host})
        if (host.empty() || host.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-") != std::string::npos)
            throw std::runtime_error("Invalid connection host");
    for (const auto& port : {c.port, c.rest_port})
        if (port.empty() || port.size() > 5 || port.find_first_not_of("0123456789") != std::string::npos ||
            std::stoul(port) == 0 || std::stoul(port) > 65535) throw std::runtime_error("Invalid connection port");
    return c;
}

std::vector<TradeGroup> validate_arbitrage(const Config& config, const nlohmann::json& exchange_info) {
    std::unordered_map<std::string, std::pair<std::string, std::string>> markets;
    for (const auto& market : exchange_info.at("symbols")) {
        printf("symbol:%s,status:%s,baseAsset:%s,quoteAsset:%s \n",market.at("symbol").get_ref<const std::string&>().c_str(),market.at("status").get_ref<const std::string&>().c_str(),market.at("baseAsset").get_ref<const std::string&>().c_str(),market.at("quoteAsset").get_ref<const std::string&>().c_str());
        const auto symbol = market.at("symbol").get<std::string>();
        if (!config.symbol_indices.contains(symbol)) continue;
        if (market.at("status") != "TRADING" || !market.at("isSpotTradingAllowed").get<bool>())
            throw std::runtime_error("Symbol is not available for Spot trading: " + symbol);
        const auto base = market.at("baseAsset").get<std::string>();
        const auto quote = market.at("quoteAsset").get<std::string>();
        if (base.empty() || quote.empty() || base == quote)
            throw std::runtime_error("Invalid assets for symbol: " + symbol);
        markets.emplace(symbol, std::pair{base, quote});
    }
    for (const auto& symbol : config.symbols)
        if (!markets.contains(symbol)) throw std::runtime_error("Unknown symbol: " + symbol);
    std::vector<TradeGroup> groups;
    for (std::size_t group = 0; group < config.triangle_indices.size(); ++group) {
        std::unordered_map<std::string, unsigned> degree;
        for (const auto index : config.triangle_indices[group]) {
            const auto& [base, quote] = markets.at(config.symbols.at(index));
            ++degree[base];
            ++degree[quote];
        }
        if (degree.size() != 3 || std::any_of(degree.begin(), degree.end(), [](const auto& asset) { return asset.second != 2; }))
            throw std::runtime_error("Arbitrage group " + std::to_string(group) + " does not form a three-asset closed loop");
        const auto& indices = config.triangle_indices[group];
        // Only USDT is funded initially. Each USDT-quoted market is a
        // possible first purchase, giving the two directions of the triangle.
        TradeGroup trading_group{indices, {}};
        std::size_t path_count = 0;
        for (std::size_t first_leg = 0; first_leg < 3; ++first_leg) {
            const auto& [first_base, first_quote] = markets.at(config.symbols.at(indices[first_leg]));
            if (first_quote != "USDT") continue;
            TradePath path{};
            path[0] = {indices[first_leg], true};
            std::array<bool, 3> used{};
            used[first_leg] = true;
            auto current_asset = first_base;
            for (std::size_t leg = 1; leg < 3; ++leg) {
                bool found = false;
                for (std::size_t candidate = 0; candidate < 3; ++candidate) {
                    if (used[candidate]) continue;
                    const auto& [base, quote] = markets.at(config.symbols.at(indices[candidate]));
                    if (current_asset != base && current_asset != quote) continue;
                    const bool buy = current_asset == quote;
                    path[leg] = {indices[candidate], buy};
                    current_asset = buy ? base : quote;
                    used[candidate] = true;
                    found = true;
                    break;
                }
                if (!found) throw std::runtime_error("Cannot construct arbitrage path");
            }
            if (current_asset != "USDT") throw std::runtime_error("Arbitrage path is not closed");
            trading_group.paths.at(path_count++) = path;
        }
        if (path_count != trading_group.paths.size())
            throw std::runtime_error("Arbitrage group " + std::to_string(group) +
                                     " must contain two USDT-quoted markets");
        groups.push_back(trading_group);
    }
    return groups;
}

std::string stream_target(const std::vector<std::string>& symbols) {
    std::string target = "/stream?streams=";
    for (const auto& symbol : symbols) {
        if (target.back() != '=') target += '/';
        for (char c : symbol) target += (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
        target += "@bookTicker";
    }
    return target;
}

OrderBook decode_best_bid_ask(const nlohmann::json& message, std::int64_t received_time_us) {
    const bool combined = message.is_object() && message.contains("stream");
    const auto& data = combined ? message.at("data") : message;
    OrderBook orderbook;
    orderbook.symbol = data.at("s").get<std::string>();
    const auto& update = data.at("u");
    if (!update.is_number_integer() ||
        (update.is_number_unsigned() && update.get<std::uint64_t>() >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())))
        throw std::runtime_error("Invalid book update ID");
    orderbook.book_update_id = update.get<std::int64_t>();
    if (orderbook.book_update_id < 0) throw std::runtime_error("Negative book update ID");
    orderbook.received_time_us = received_time_us;
    decimal_pair(data.at("b"), data.at("a"), orderbook.bid_price, orderbook.ask_price, orderbook.price_exponent);
    decimal_pair(data.at("B"), data.at("A"), orderbook.bid_qty, orderbook.ask_qty, orderbook.qty_exponent);
    return orderbook;
}

OrderBookManager::OrderBookManager(const Config& config)
    : indices_(config.symbol_indices), orderbooks_(config.symbols.size()),
      groups_(config.trading_groups), max_arbitrage_usdt_(config.max_arbitrage_usdt),
      fee_multiplier_(1.0L - config.commission_taker), edge_threshold_(config.edge_threshold) {
    if (!std::isfinite(max_arbitrage_usdt_) || max_arbitrage_usdt_ <= 0)
        throw std::runtime_error("Invalid maximum arbitrage amount");
    if (!std::isfinite(fee_multiplier_) || fee_multiplier_ <= 0 || fee_multiplier_ > 1 ||
        !std::isfinite(edge_threshold_) || edge_threshold_ < 0)
        throw std::runtime_error("Invalid arbitrage fee or threshold");
    for (const auto& group : groups_) {
        for (const auto index : group.symbol_indices)
            if (index >= orderbooks_.size()) throw std::out_of_range("Invalid group symbol index");
        for (const auto& path : group.paths)
            for (const auto& leg : path)
                if (std::find(group.symbol_indices.begin(), group.symbol_indices.end(), leg.index) ==
                    group.symbol_indices.end())
                    throw std::runtime_error("Path symbol is not in its group");
    }
}
std::size_t OrderBookManager::index_of(const std::string& symbol) const {
    return indices_.at(symbol);
}
void OrderBookManager::update(std::size_t index, OrderBook orderbook) {
    std::lock_guard lock(mutex_);
    auto& slot = orderbooks_.at(index);
    if (!slot) ++populated_;
    slot = std::move(orderbook);
    ++received_;
}
bool OrderBookManager::scan_edge(std::size_t index, ArbitrageOpportunity& result) {
    result = {};
    std::lock_guard lock(mutex_);
    if (index >= orderbooks_.size()) throw std::out_of_range("Invalid symbol index");
    for (std::size_t group_index = 0; group_index < groups_.size(); ++group_index) {
        const auto& group = groups_[group_index];
        if (std::find(group.symbol_indices.begin(), group.symbol_indices.end(), index) ==
            group.symbol_indices.end()) continue;
        for (const auto& path : group.paths) {
            ArbitrageOpportunity candidate{};
            candidate.group_index = group_index;
            candidate.path = path;
            long double output = 1.0L; // Amount of the current asset per initial USDT.
            long double input_limit = max_arbitrage_usdt_;
            bool available = true;
            for (std::size_t i = 0; i < path.size(); ++i) {
                const auto& leg = path[i];
                const auto& slot = orderbooks_[leg.index];
                if (!slot) { available = false; break; }
                const auto price = leg.buy ? slot->ask_price : slot->bid_price;
                const auto qty = leg.buy ? slot->ask_qty : slot->bid_qty;
                if (price <= 0 || qty <= 0) { available = false; break; }
                const long double rate = static_cast<long double>(price) *
                                         std::pow(10.0L, slot->price_exponent);
                const long double liquidity = static_cast<long double>(qty) *
                                              std::pow(10.0L, slot->qty_exponent);
                const long double base_qty = leg.buy ? output / rate : output;
                if (!std::isfinite(rate) || rate <= 0 || !std::isfinite(liquidity) || liquidity <= 0 ||
                    !std::isfinite(base_qty) || base_qty <= 0) { available = false; break; }
                input_limit = std::min(input_limit, liquidity / base_qty);
                candidate.prices[i] = rate;
                candidate.quantities[i] = base_qty;
                candidate.book_update_ids[i] = slot->book_update_id;
                candidate.received_steady_ns[i] = slot->received_steady_ns;
                // Model commission as deducted from the asset received on each leg.
                output = (leg.buy ? base_qty : base_qty * rate) * fee_multiplier_;
            }
            if (!available || !std::isfinite(output) || output - 1.0L <= edge_threshold_ ||
                !std::isfinite(input_limit) || input_limit <= 0) continue;
            candidate.input_usdt = input_limit;
            candidate.output_usdt = input_limit * output;
            candidate.profit_usdt = candidate.output_usdt - input_limit;
            candidate.net_return = output - 1.0L;
            for (auto& qty : candidate.quantities) qty *= input_limit;
            if (!std::isfinite(candidate.output_usdt) ||
                std::any_of(candidate.quantities.begin(), candidate.quantities.end(),
                            [](long double qty) { return !std::isfinite(qty) || qty <= 0; })) continue;
            result = candidate;
            return true;
        }
    }
    return false;
}

std::optional<OrderBook> OrderBookManager::get(std::size_t index) const {
    std::lock_guard lock(mutex_);
    return orderbooks_.at(index);
}
std::vector<std::optional<OrderBook>> OrderBookManager::snapshot() const {
    std::lock_guard lock(mutex_);
    return orderbooks_;
}
OrderBookStats OrderBookManager::stats() const {
    std::lock_guard lock(mutex_);
    return {orderbooks_.size(), populated_, received_};
}
} // namespace triangular
