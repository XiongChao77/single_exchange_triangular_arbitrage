#pragma once

#include "triangular/orderbook.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/io_context.hpp>
#include <memory>
#include <functional>

namespace triangular {

nlohmann::json fetch_exchange_info(const Config& config);

// Call start/process/stop/stats on the processing thread. Network reads use a dedicated thread.
class Receiver {
public:
    Receiver(Config config, OrderBookManager& orderbooks, AsyncLogger& logger,
             std::function<void(const ArbitrageOpportunity&)> on_opportunity = {});
    ~Receiver();
    Receiver(const Receiver&) = delete;
    Receiver& operator=(const Receiver&) = delete;
    void start();
    void stop();
    void process(std::size_t max_batch = 32); // Nonblocking SPSC consumption.
    nlohmann::json stats() const; // Read on the io_context thread.
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // namespace triangular
