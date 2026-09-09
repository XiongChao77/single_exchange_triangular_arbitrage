#include "triangular/receiver.hpp"
#include "triangular/consumer.hpp"

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <csignal>
#include <iostream>
#include <functional>

namespace {
const std::filesystem::path kConfigPath = "configs/binance.json";
constexpr std::size_t kRunSeconds = 10; // Zero runs until SIGINT or SIGTERM.
constexpr std::size_t kQuotesPerSymbol = 10;
const std::filesystem::path kLogDirectory = "logs";
constexpr std::size_t kLogQueueCapacity = 65536;
constexpr std::size_t kLogBatchThreshold = 256;
constexpr std::chrono::milliseconds kLogFlushInterval{1000};
constexpr std::chrono::seconds kStatisticsInterval{1};
static_assert(kQuotesPerSymbol > 0);
}

int main() {
    try {
        auto config = triangular::load_config(std::filesystem::absolute(kConfigPath));
        const std::size_t queue_capacity = config.symbols.size() * kQuotesPerSymbol;
        auto key = triangular::load_api_key(config.api_key_file);
        triangular::QuoteQueue queue(queue_capacity);
        const auto log_path = kLogDirectory / ("sbe-" + std::to_string(triangular::wall_time_us()) + ".jsonl");
        triangular::AsyncLogger logger(log_path, {kLogQueueCapacity, kLogBatchThreshold, kLogFlushInterval});
        logger.log("INFO", "run_started", {{"quote_capacity", queue_capacity}, {"symbols", config.symbols},
            {"run_seconds", kRunSeconds}, {"log_capacity", kLogQueueCapacity},
            {"log_batch_threshold", kLogBatchThreshold}, {"log_flush_interval_ms", kLogFlushInterval.count()}});
        std::cerr << "INFO log_file=" << log_path.string() << '\n';
        triangular::QuoteConsumer consumer(queue, logger);
        boost::asio::io_context io;
        triangular::Receiver receiver(io, std::move(config), std::move(key), queue, logger);
        boost::asio::signal_set signals(io, SIGINT, SIGTERM);
        boost::asio::steady_timer duration(io);
        boost::asio::steady_timer statistics(io);
        auto report = [&](const char* event) {
            const auto q = queue.stats();
            const auto l = logger.stats();
            logger.log("INFO", event, {{"quote_queue", {{"received", q.received}, {"consumed", q.consumed},
                    {"queued", q.size}, {"capacity", q.capacity}, {"high_water", q.high_water}, {"dropped", q.dropped}}},
                {"receiver", receiver.stats()}, {"consumer", consumer.stats()},
                {"logger", {{"queued", l.queued}, {"in_flight", l.in_flight}, {"high_water", l.high_water},
                    {"dropped", l.dropped}, {"written", l.written}, {"max_write_us", l.max_write_us},
                    {"max_enqueue_ns", l.max_enqueue_ns}, {"max_lock_wait_ns", l.max_lock_wait_ns}, {"failed", l.failed}}}});
        };
        std::function<void()> schedule_statistics;
        schedule_statistics = [&] {
            statistics.expires_after(kStatisticsInterval);
            statistics.async_wait([&](boost::system::error_code ec) {
                if (!ec) { report("pipeline_stats"); schedule_statistics(); }
            });
        };
        auto stop = [&] {
            receiver.stop();
            signals.cancel();
            duration.cancel();
            statistics.cancel();
        };
        signals.async_wait([&](boost::system::error_code ec, int) { if (!ec) stop(); });
        if (kRunSeconds) {
            duration.expires_after(std::chrono::seconds(kRunSeconds));
            duration.async_wait([&](boost::system::error_code ec) { if (!ec) stop(); });
        }
        receiver.start();
        schedule_statistics();
        io.run();
        consumer.stop(); // Drain accepted quotes before stopping the log writer.
        report("pipeline_final");
        logger.stop();
        const auto q = queue.stats();
        const auto l = logger.stats();
        std::cerr << "INFO finished received=" << q.received << " consumed=" << q.consumed
                  << " quote_dropped=" << q.dropped << " log_written=" << l.written
                  << " log_dropped=" << l.dropped << " log_write_lost=" << l.write_lost
                  << " logger_failed=" << l.failed << '\n';
        return l.failed || consumer.stats().at("failed").get<bool>() ? 1 : 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR " << error.what() << '\n';
        return 1;
    }
}
