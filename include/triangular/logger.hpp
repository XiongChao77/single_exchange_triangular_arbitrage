#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

namespace triangular {

std::int64_t wall_time_us();
std::int64_t steady_time_ns();

struct LoggerOptions {
    std::size_t capacity = 65536;
    std::size_t batch_threshold = 256;
    std::chrono::milliseconds flush_interval{1000};
};

struct LoggerStats {
    std::size_t queued{}, in_flight{}, high_water{}, capacity{};
    std::uint64_t submitted{}, accepted{}, dropped{}, written{}, write_lost{}, batches{};
    std::uint64_t last_write_us{}, max_write_us{}, total_write_us{}, bytes_written{};
    std::uint64_t total_diagnostic_write_us{}, max_diagnostic_write_us{};
    std::uint64_t max_enqueue_ns{}, total_enqueue_ns{}, max_lock_wait_ns{};
    bool failed{};
};

// Producers perform no file I/O and never wait for queue space. A short mutex
// protects a pre-reserved vector; the writer swaps it out before doing I/O.
class AsyncLogger {
public:
    AsyncLogger(std::filesystem::path path, LoggerOptions options = {});
    ~AsyncLogger();
    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;
    bool log(std::string level, std::string event, nlohmann::json fields = nlohmann::json::object());
    LoggerStats stats() const;
    void stop(); // Stop producers first; drain all accepted records, then join.
    const std::filesystem::path& path() const { return path_; }
private:
    struct Record {
        std::uint64_t sequence;
        std::int64_t time_us;
        std::int64_t enqueue_steady_ns;
        std::string level, event;
        nlohmann::json fields;
    };
    void run() noexcept;
    void write_blob(const std::string& bytes);
    nlohmann::json stats_json(const LoggerStats& s) const;
    std::filesystem::path path_;
    LoggerOptions options_;
    std::ofstream file_;
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::vector<Record> pending_;
    LoggerStats stats_;
    bool stopping_ = false;
    std::thread worker_;
};

} // namespace triangular
