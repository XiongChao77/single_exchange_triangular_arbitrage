#pragma once

#include "triangular/orderbook.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/io_context.hpp>
#include <memory>
#include <functional>

namespace triangular {

nlohmann::json fetch_exchange_info(const Config& config);

// Call start/stop on the io_context thread. OrderBookManager inspection is thread-safe.
class Receiver {
public:
    Receiver(boost::asio::io_context& io, Config config, OrderBookManager& orderbooks, AsyncLogger& logger,
             std::function<void(const ArbitrageOpportunity&)> on_opportunity = {});
    ~Receiver();
    Receiver(const Receiver&) = delete;
    Receiver& operator=(const Receiver&) = delete;
    void start();
    void stop();
    nlohmann::json stats() const; // Read on the io_context thread.
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // namespace triangular
