#include "triangular/core.hpp"

#include <bit>
#include <algorithm>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <unordered_set>

namespace triangular {
namespace {
std::uint16_t u16(std::span<const std::uint8_t> bytes, std::size_t offset) {
    return static_cast<std::uint16_t>(bytes[offset] | (bytes[offset + 1] << 8));
}
std::int64_t i64(std::span<const std::uint8_t> bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) value |= std::uint64_t{bytes[offset + i]} << (8 * i);
    return std::bit_cast<std::int64_t>(value);
}
std::filesystem::path resolve(const std::filesystem::path& config, const std::string& value) {
    const std::filesystem::path path(value);
    return path.is_absolute() ? path : (config.parent_path() / path).lexically_normal();
}
} // namespace

std::vector<std::string> normalize_symbols(const std::vector<std::string>& symbols) {
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    for (auto symbol : symbols) {
        if (symbol.empty() || symbol.size() > 255) throw std::runtime_error("Invalid symbol length");
        for (auto& c : symbol) {
            if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
            if (!((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')))
                throw std::runtime_error("Symbols must contain ASCII letters and digits only");
        }
        if (seen.insert(symbol).second) result.push_back(std::move(symbol));
    }
    if (result.empty() || result.size() > 1024) throw std::runtime_error("Expected 1 to 1024 unique symbols");
    return result;
}

Config load_config(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("Cannot open configuration file");
    nlohmann::json j;
    try { input >> j; } catch (...) { throw std::runtime_error("Invalid configuration JSON"); }
    Config c;
    c.host = j.value("host", c.host);
    c.port = j.value("port", c.port);
    if (c.host.empty() || c.host.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-") != std::string::npos)
        throw std::runtime_error("Invalid WebSocket host");
    if (c.port.empty() || c.port.size() > 5 || c.port.find_first_not_of("0123456789") != std::string::npos ||
        std::stoul(c.port) == 0 || std::stoul(c.port) > 65535) throw std::runtime_error("Invalid WebSocket port");
    c.api_key_file = resolve(path, j.at("api_key_file").get<std::string>());
    if (j.contains("ca_file")) c.ca_file = resolve(path, j.at("ca_file").get<std::string>());
    c.symbols = normalize_symbols(j.at("symbols").get<std::vector<std::string>>());
    return c;
}

std::string stream_target(const std::vector<std::string>& symbols) {
    std::string target = "/stream?streams=";
    for (const auto& symbol : normalize_symbols(symbols)) {
        if (target.back() != '=') target += '/';
        for (char c : symbol) target += (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
        target += "@bestBidAsk";
    }
    return target;
}

std::string load_api_key(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("Cannot open API key file");
    std::string key(4097, '\0');
    input.read(key.data(), static_cast<std::streamsize>(key.size()));
    key.resize(static_cast<std::size_t>(input.gcount()));
    if (key.size() > 4096) throw std::runtime_error("API key file is too large");
    const auto begin = key.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) throw std::runtime_error("API key is empty");
    key = key.substr(begin, key.find_last_not_of(" \t\r\n") - begin + 1);
    for (unsigned char c : key) if (c <= 32 || c >= 127) throw std::runtime_error("Invalid API key characters");
    return key;
}

BestBidAsk decode_best_bid_ask(std::span<const std::uint8_t> bytes, std::int64_t received_time_us) {
    // Binance stream_1_0.xml: 8-byte header, 50-byte root, varString8 symbol.
    if (bytes.size() < 8) throw std::runtime_error("Truncated SBE header");
    const auto block = u16(bytes, 0);
    if (u16(bytes, 4) != 1 || u16(bytes, 6) != 0) throw std::runtime_error("Unsupported SBE schema/version");
    if (u16(bytes, 2) != 10001) throw std::runtime_error("Unexpected SBE template");
    if (block != 50) throw std::runtime_error("Invalid SBE root block length");
    const std::size_t symbol_offset = 8 + block;
    if (bytes.size() <= symbol_offset) throw std::runtime_error("Truncated SBE root");
    const auto length = bytes[symbol_offset];
    if (length == 0 || bytes.size() != symbol_offset + 1 + length) throw std::runtime_error("Invalid SBE symbol length");
    BestBidAsk q;
    q.symbol.assign(reinterpret_cast<const char*>(bytes.data() + symbol_offset + 1), length);
    q.event_time_us = i64(bytes, 8);
    q.received_time_us = received_time_us;
    q.book_update_id = i64(bytes, 16);
    q.price_exponent = std::bit_cast<std::int8_t>(bytes[24]);
    q.qty_exponent = std::bit_cast<std::int8_t>(bytes[25]);
    q.bid_price = i64(bytes, 26);
    q.bid_qty = i64(bytes, 34);
    q.ask_price = i64(bytes, 42);
    q.ask_qty = i64(bytes, 50);
    return q;
}

QuoteQueue::QuoteQueue(std::size_t capacity) : slots_(capacity) {
    if (capacity == 0) throw std::invalid_argument("Queue capacity must be positive");
}
bool QuoteQueue::push(BestBidAsk quote) {
    std::unique_lock lock(mutex_);
    if (closed_) throw std::logic_error("Cannot push to a closed quote queue");
    const bool full = size_ == slots_.size();
    const auto index = (head_ + size_) % slots_.size();
    slots_[index] = std::move(quote);
    ++received_;
    if (full) { head_ = (head_ + 1) % slots_.size(); ++dropped_; }
    else ++size_;
    high_water_ = std::max(high_water_, size_);
    lock.unlock();
    ready_.notify_one();
    return full;
}
std::optional<BestBidAsk> QuoteQueue::wait_pop() {
    std::unique_lock lock(mutex_);
    ready_.wait(lock, [this] { return closed_ || size_ != 0; });
    if (size_ == 0) return std::nullopt;
    auto quote = std::move(slots_[head_]);
    slots_[head_].reset();
    head_ = (head_ + 1) % slots_.size();
    --size_;
    ++consumed_;
    return quote;
}
void QuoteQueue::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    ready_.notify_all();
}
QueueStats QuoteQueue::stats() const {
    std::lock_guard lock(mutex_);
    return {size_, slots_.size(), received_, dropped_, consumed_, high_water_};
}
std::vector<BestBidAsk> QuoteQueue::snapshot() const {
    std::lock_guard lock(mutex_);
    std::vector<BestBidAsk> copy;
    copy.reserve(size_);
    for (std::size_t i = 0; i < size_; ++i) copy.push_back(*slots_[(head_ + i) % slots_.size()]);
    return copy;
}
} // namespace triangular
