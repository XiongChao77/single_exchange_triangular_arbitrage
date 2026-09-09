#include "triangular/consumer.hpp"

#include <algorithm>

namespace triangular {
nlohmann::json quote_fields(const BestBidAsk& q) {
    return {{"receive_sequence", q.receive_sequence}, {"symbol", q.symbol},
        {"event_time_us", q.event_time_us}, {"received_time_us", q.received_time_us},
        {"book_update_id", q.book_update_id}, {"price_exponent", q.price_exponent},
        {"qty_exponent", q.qty_exponent}, {"bid_price", q.bid_price}, {"bid_qty", q.bid_qty},
        {"ask_price", q.ask_price}, {"ask_qty", q.ask_qty}};
}
QuoteConsumer::QuoteConsumer(QuoteQueue& queue, AsyncLogger& logger)
    : queue_(queue), logger_(logger), worker_([this] { run(); }) {}
QuoteConsumer::~QuoteConsumer() { stop(); }
void QuoteConsumer::stop() {
    queue_.close();
    if (worker_.joinable()) worker_.join();
}
nlohmann::json QuoteConsumer::stats() const {
    return {{"processed", processed_.load()}, {"log_rejected", log_rejected_.load()},
        {"total_receive_to_consume_ns", total_age_ns_.load()}, {"max_receive_to_consume_ns", max_age_ns_.load()},
        {"total_work_ns", total_work_ns_.load()}, {"max_work_ns", max_work_ns_.load()}, {"failed", failed_.load()}};
}
void QuoteConsumer::run() noexcept {
    try {
        while (auto quote = queue_.wait_pop()) {
            const auto start = steady_time_ns();
            const auto age = static_cast<std::uint64_t>(std::max<std::int64_t>(0, start - quote->received_steady_ns));
            auto fields = quote_fields(*quote);
            fields["consumed_time_us"] = wall_time_us();
            fields["receive_to_consume_ns"] = age;
            if (!logger_.log("INFO", "consumer_quote", std::move(fields))) ++log_rejected_;
            const auto work = static_cast<std::uint64_t>(steady_time_ns() - start);
            total_age_ns_ += age;
            max_age_ns_ = std::max(max_age_ns_.load(), age);
            total_work_ns_ += work;
            max_work_ns_ = std::max(max_work_ns_.load(), work);
            ++processed_;
        }
    } catch (...) {
        failed_ = true;
        queue_.close();
        try { logger_.log("ERROR", "consumer_failed"); } catch (...) {}
    }
}
} // namespace triangular
