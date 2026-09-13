#include "triangular/execution/execution.hpp"
#include <boost/asio/steady_timer.hpp>
#include <iostream>

using namespace triangular;
using namespace triangular::execution;

// Invoked only by gateway_failure_integration.py against its loopback TLS server.
int main(int argc, char** argv) {
    try {
        if (argc != 5) throw std::runtime_error("Expected port, CA, API-key file and secret file");
        boost::asio::io_context io;
        Config config;
        config.execution_mode = "live";
        config.rest_host = "localhost"; config.rest_port = argv[1]; config.ca_file = argv[2];
        config.api_key_file = argv[3]; config.secret_key_file = argv[4];
        Market market{}; market.symbol = "USDTTRY"; market.base = "USDT"; market.quote = "TRY";
        BinanceGateway gateway(io, config, {market});
        bool received = false;
        boost::asio::steady_timer deadline(io, std::chrono::seconds(5));
        deadline.async_wait([&](auto ec) { if (!ec) io.stop(); });
        gateway.submit(OrderRequest{"probe-first-leg", 0, false, "48.54", "20", false}, [&](OrderReport report) {
            received = true; deadline.cancel();
            const char* status = report.status == OrderStatus::Unknown ? "UNKNOWN" :
                report.status == OrderStatus::Rejected ? "REJECTED" : "OTHER";
            std::cout << nlohmann::json{{"status", status}, {"failure", report.failure}}.dump() << '\n';
        });
        io.run();
        if (!received) throw std::runtime_error("Gateway report timeout");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
