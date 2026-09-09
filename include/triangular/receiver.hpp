#pragma once

#include "triangular/core.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/io_context.hpp>
#include <memory>

namespace triangular {

// Call start/stop on the io_context thread. QuoteQueue inspection is thread-safe.
class Receiver {
public:
    Receiver(boost::asio::io_context& io, Config config, std::string api_key, QuoteQueue& queue, AsyncLogger& logger);
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
