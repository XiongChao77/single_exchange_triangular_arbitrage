#include "triangular/receiver.hpp"
#include "triangular/execution/execution.hpp"

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <csignal>
#include <iostream>
#include <functional>

namespace {
const std::filesystem::path kConfigPath = "configs/binance.json";
const std::filesystem::path kLogDirectory = "logs";
constexpr std::size_t kLogQueueCapacity = 65536;
constexpr std::size_t kLogBatchThreshold = 256;
constexpr std::chrono::milliseconds kLogFlushInterval{1000};
constexpr std::chrono::seconds kStatisticsInterval{1};
}

int main() {
    try {
        auto config = triangular::load_config(std::filesystem::exists(kConfigPath) ?
            std::filesystem::absolute(kConfigPath) :
            std::filesystem::path("/root/work/single_exchange_triangular_arbitrage/configs/binance.json"));
        const auto exchange_info = triangular::fetch_exchange_info(config);
        config.trading_groups = triangular::validate_arbitrage(config, exchange_info);
        triangular::OrderBookManager orderbooks(config);
        const auto log_path = kLogDirectory / ("json-" + std::to_string(triangular::wall_time_us()) + ".jsonl");
        triangular::AsyncLogger logger(log_path, {kLogQueueCapacity, kLogBatchThreshold, kLogFlushInterval});
        logger.log("INFO", "run_started", {{"orderbook_slots", orderbooks.stats().symbols}, {"symbols", config.symbols},
            {"triangle_indices", config.triangle_indices}, {"execution_mode", config.execution_mode},
            {"live_test_mode", config.live_test_mode},
            {"log_capacity", kLogQueueCapacity},
            {"log_batch_threshold", kLogBatchThreshold}, {"log_flush_interval_ms", kLogFlushInterval.count()}});
        std::cerr << "INFO log_file=" << log_path.string() << '\n';
        boost::asio::io_context io;
        std::unique_ptr<triangular::execution::Gateway> gateway;
        std::unique_ptr<triangular::execution::ArbitrageExecutor> executor;
        if (config.execution_mode == "paper" || config.execution_mode == "live") {
            auto markets = triangular::execution::load_markets(config, exchange_info);
            triangular::execution::Options options;
            options.execution_timeout = std::chrono::milliseconds(config.execution_timeout_ms);
            if (config.execution_mode == "paper") {
                gateway = std::make_unique<triangular::execution::PaperGateway>(io, orderbooks, markets,
                    triangular::execution::Decimal(config.commission_taker),
                    triangular::execution::Balances{{"USDT", triangular::execution::Decimal(config.max_arbitrage_usdt)}});
            } else {
                gateway = std::make_unique<triangular::execution::BinanceGateway>(io, config, markets);
                if (config.live_test_mode) options.max_cycles = 2;
            }
            executor = std::make_unique<triangular::execution::ArbitrageExecutor>(io, config, orderbooks,
                std::move(markets), *gateway, options, [&](const nlohmann::json& status) {
                    logger.log("INFO", "execution_state", status);
                });
        }
        triangular::Receiver receiver(io, std::move(config), orderbooks, logger,
            [&](const triangular::ArbitrageOpportunity& opportunity) {
                if (executor) executor->try_start(opportunity);
            });
        boost::asio::signal_set signals(io, SIGINT, SIGTERM);
        boost::asio::steady_timer duration(io);
        boost::asio::steady_timer statistics(io);
        auto report = [&](const char* event) {
            const auto orderbook_stats = orderbooks.stats();
            const auto l = logger.stats();
            logger.log("INFO", event, {{"orderbook_store", {{"received", orderbook_stats.received},
                    {"symbols", orderbook_stats.symbols}, {"populated", orderbook_stats.populated}}},
                {"receiver", receiver.stats()},
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
            if (executor) executor->stop();
            receiver.stop();
            signals.cancel();
            duration.cancel();
            statistics.cancel();
        };
        signals.async_wait([&](boost::system::error_code ec, int) { if (!ec) stop(); });
        receiver.start();
        schedule_statistics();
        io.run();
        const auto latest = orderbooks.snapshot();
        for (std::size_t index = 0; index < latest.size(); ++index) {
            if (!latest[index]) continue;
            const auto& orderbook = *latest[index];
            logger.log("INFO", "latest_orderbook", {
                {"receive_sequence", orderbook.receive_sequence},
                {"symbol", orderbook.symbol},
                {"event_time_us", orderbook.event_time_us ?
                    nlohmann::json(*orderbook.event_time_us) : nlohmann::json(nullptr)},
                {"received_time_us", orderbook.received_time_us},
                {"book_update_id", orderbook.book_update_id},
                {"price_exponent", orderbook.price_exponent},
                {"qty_exponent", orderbook.qty_exponent},
                {"bid_price", orderbook.bid_price},
                {"bid_qty", orderbook.bid_qty},
                {"ask_price", orderbook.ask_price},
                {"ask_qty", orderbook.ask_qty},
                {"index", index}});
        }
        if (executor) logger.log("INFO", "execution_final", executor->stats());
        report("pipeline_final");
        logger.stop();
        const auto orderbook_stats = orderbooks.stats();
        const auto l = logger.stats();
        std::cerr << "INFO finished received=" << orderbook_stats.received << " populated=" << orderbook_stats.populated << " log_written=" << l.written
                  << " log_dropped=" << l.dropped << " log_write_lost=" << l.write_lost
                  << " logger_failed=" << l.failed << '\n';
        return l.failed || (executor && executor->busy()) ? 1 : 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR " << error.what() << '\n';
        return 1;
    }
}
