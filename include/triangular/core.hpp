#pragma once

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace triangular {

struct Config {
    std::string host = "stream-sbe.binance.com";
    std::string port = "443";
    std::filesystem::path api_key_file;
    std::filesystem::path ca_file;
    std::vector<std::string> symbols;
};

Config load_config(const std::filesystem::path& path);
std::vector<std::string> normalize_symbols(const std::vector<std::string>& symbols);
std::string stream_target(const std::vector<std::string>& symbols);
std::string load_api_key(const std::filesystem::path& path);

// Decimal value = mantissa * 10^exponent; no floating-point rounding on ingestion.
struct BestBidAsk {
    std::string symbol;
    std::int64_t event_time_us{};
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
};

BestBidAsk decode_best_bid_ask(std::span<const std::uint8_t> frame,
                             std::int64_t received_time_us);

struct QueueStats {
    std::size_t size;
    std::size_t capacity;
    std::uint64_t received;
    std::uint64_t dropped;
    std::uint64_t consumed;
    std::size_t high_water;
};

// A single global arrival-order ring shared by all subscribed symbols.
class QuoteQueue {
public:
    explicit QuoteQueue(std::size_t capacity);
    bool push(BestBidAsk quote); // True if the oldest entry was overwritten.
    std::optional<BestBidAsk> wait_pop(); // Null after close and complete draining.
    void close();
    QueueStats stats() const;
    std::vector<BestBidAsk> snapshot() const; // Non-destructive inspection only.
private:
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::vector<std::optional<BestBidAsk>> slots_;
    std::size_t head_ = 0;
    std::size_t size_ = 0;
    std::uint64_t received_ = 0;
    std::uint64_t dropped_ = 0;
    std::uint64_t consumed_ = 0;
    std::size_t high_water_ = 0;
    bool closed_ = false;
};

} // namespace triangular
