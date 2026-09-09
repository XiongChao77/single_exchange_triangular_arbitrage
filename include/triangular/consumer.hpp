#pragma once

#include "triangular/core.hpp"
#include "triangular/logger.hpp"
#include <atomic>
#include <thread>

namespace triangular {
nlohmann::json quote_fields(const BestBidAsk& quote);

// A deliberately light consumer: dequeue, measure age, submit one log record.
class QuoteConsumer {
public:
    QuoteConsumer(QuoteQueue& queue, AsyncLogger& logger);
    ~QuoteConsumer();
    void stop();
    nlohmann::json stats() const;
private:
    void run() noexcept;
    QuoteQueue& queue_;
    AsyncLogger& logger_;
    std::atomic<std::uint64_t> processed_{0}, log_rejected_{0}, total_age_ns_{0}, max_age_ns_{0};
    std::atomic<std::uint64_t> total_work_ns_{0}, max_work_ns_{0};
    std::atomic<bool> failed_{false};
    std::thread worker_;
};
} // namespace triangular
