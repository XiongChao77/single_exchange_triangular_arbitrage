#include "triangular/logger.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace triangular {
std::int64_t wall_time_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
std::int64_t steady_time_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

AsyncLogger::AsyncLogger(std::filesystem::path path, LoggerOptions options)
    : path_(std::move(path)), options_(options) {
    if (!options.capacity || !options.batch_threshold || options.batch_threshold > options.capacity ||
        options.flush_interval.count() <= 0) throw std::invalid_argument("Invalid logger options");
    if (!path_.parent_path().empty()) std::filesystem::create_directories(path_.parent_path());
    file_.exceptions(std::ios::failbit | std::ios::badbit);
    file_.open(path_, std::ios::binary | std::ios::app);
    pending_.reserve(options.capacity);
    stats_.capacity = options.capacity;
    worker_ = std::thread([this] { run(); });
}
AsyncLogger::~AsyncLogger() { stop(); }

bool AsyncLogger::log(std::string level, std::string event, nlohmann::json fields) {
    const auto begin = steady_time_ns();
    const auto timestamp = wall_time_us();
    std::unique_lock lock(mutex_);
    const auto wait_ns = static_cast<std::uint64_t>(steady_time_ns() - begin);
    ++stats_.submitted;
    stats_.max_lock_wait_ns = std::max(stats_.max_lock_wait_ns, wait_ns);
    if (stopping_ || stats_.failed || pending_.size() >= options_.capacity) {
        ++stats_.dropped;
        return false;
    }
    pending_.push_back({stats_.accepted + 1, timestamp, begin, std::move(level), std::move(event), std::move(fields)});
    ++stats_.accepted;
    stats_.high_water = std::max(stats_.high_water, pending_.size());
    const bool wake = pending_.size() >= options_.batch_threshold;
    const auto elapsed = static_cast<std::uint64_t>(steady_time_ns() - begin);
    stats_.max_enqueue_ns = std::max(stats_.max_enqueue_ns, elapsed);
    stats_.total_enqueue_ns += elapsed;
    lock.unlock();
    if (wake) ready_.notify_one();
    return true;
}

LoggerStats AsyncLogger::stats() const {
    std::lock_guard lock(mutex_);
    auto s = stats_;
    s.queued = pending_.size();
    return s;
}
void AsyncLogger::stop() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    ready_.notify_one();
    if (worker_.joinable()) worker_.join();
}
void AsyncLogger::write_blob(const std::string& bytes) {
    file_.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    file_.flush(); // Flush to the OS; this is not an fsync durability guarantee.
}
nlohmann::json AsyncLogger::stats_json(const LoggerStats& s) const {
    return {{"queued", s.queued}, {"in_flight", s.in_flight}, {"capacity", s.capacity},
        {"queue_usage_percent", 100.0 * static_cast<double>(s.queued) / static_cast<double>(s.capacity)},
        {"high_water", s.high_water}, {"submitted", s.submitted}, {"accepted", s.accepted},
        {"dropped", s.dropped}, {"written", s.written}, {"write_lost", s.write_lost},
        {"batches", s.batches}, {"last_write_us", s.last_write_us}, {"max_write_us", s.max_write_us},
        {"total_write_us", s.total_write_us}, {"bytes_written", s.bytes_written},
        {"total_diagnostic_write_us", s.total_diagnostic_write_us}, {"max_diagnostic_write_us", s.max_diagnostic_write_us},
        {"max_enqueue_ns", s.max_enqueue_ns}, {"total_enqueue_ns", s.total_enqueue_ns},
        {"max_lock_wait_ns", s.max_lock_wait_ns}, {"failed", s.failed}};
}
void AsyncLogger::run() noexcept {
    try {
        std::vector<Record> batch;
        batch.reserve(options_.capacity);
        for (;;) {
            std::string trigger;
            {
                std::unique_lock lock(mutex_);
                ready_.wait_for(lock, options_.flush_interval, [this] {
                    return stopping_ || pending_.size() >= options_.batch_threshold;
                });
                trigger = stopping_ ? "shutdown" :
                    (pending_.size() >= options_.batch_threshold ? "threshold" : "interval");
                pending_.swap(batch); // Constant-time handoff; no file I/O under this lock.
                stats_.in_flight = batch.size();
                if (batch.empty() && stopping_) break;
            }
            const auto serialize_begin = steady_time_ns();
            std::int64_t max_queue_wait_us = 0;
            std::string payload;
            for (const auto& r : batch) {
                max_queue_wait_us = std::max(max_queue_wait_us, (serialize_begin - r.enqueue_steady_ns) / 1000);
                nlohmann::json row{{"time_us", r.time_us}, {"log_sequence", r.sequence},
                    {"level", r.level}, {"event", r.event}, {"fields", r.fields}};
                payload += row.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
                payload += '\n';
            }
            const auto serialize_us = (steady_time_ns() - serialize_begin) / 1000;
            if (!batch.empty()) {
                const auto write_begin = steady_time_ns();
                write_blob(payload);
                const auto elapsed = static_cast<std::uint64_t>((steady_time_ns() - write_begin) / 1000);
                std::lock_guard lock(mutex_);
                stats_.written += batch.size();
                stats_.in_flight = 0;
                ++stats_.batches;
                stats_.last_write_us = elapsed;
                stats_.max_write_us = std::max(stats_.max_write_us, elapsed);
                stats_.total_write_us += elapsed;
                stats_.bytes_written += payload.size();
            }
            // Writer diagnostics bypass the log queue to avoid recursive logging.
            const auto current = stats();
            auto fields = stats_json(current);
            fields["trigger"] = trigger;
            fields["batch_records"] = batch.size();
            fields["serialize_us"] = serialize_us;
            fields["max_batch_queue_wait_us"] = max_queue_wait_us;
            nlohmann::json diagnostic{{"time_us", wall_time_us()}, {"level", current.dropped ? "WARNING" : "INFO"},
                {"event", "logger_batch"}, {"fields", std::move(fields)}};
            const auto diagnostic_begin = steady_time_ns();
            write_blob(diagnostic.dump() + '\n');
            const auto diagnostic_us = static_cast<std::uint64_t>((steady_time_ns() - diagnostic_begin) / 1000);
            {
                std::lock_guard lock(mutex_);
                stats_.total_diagnostic_write_us += diagnostic_us;
                stats_.max_diagnostic_write_us = std::max(stats_.max_diagnostic_write_us, diagnostic_us);
            }
            batch.clear();
        }
        nlohmann::json final{{"time_us", wall_time_us()}, {"level", "INFO"},
            {"event", "logger_final"}, {"fields", stats_json(stats())}};
        write_blob(final.dump() + '\n');
    } catch (...) {
        {
            std::lock_guard lock(mutex_);
            stats_.failed = true;
            // The failing batch may be partially written. Treat it as unconfirmed.
            stats_.write_lost += stats_.in_flight + pending_.size();
            stats_.in_flight = 0;
            pending_.clear();
        }
        // Only the logger thread performs this fallback I/O, never a producer.
        std::cerr << "ERROR asynchronous log writer failed; pending logs are unconfirmed\n";
    }
}
} // namespace triangular
