#include "triangular/execution/execution.hpp"
#include <boost/asio/steady_timer.hpp>
#include <iostream>

using namespace triangular;
using namespace triangular::execution;

// Invoked only by gateway_failure_integration.py against its loopback TLS server.
int main(int argc, char** argv) {
    try {
        if (argc != 5 && argc != 6) throw std::runtime_error("Expected port, CA, API-key file and secret file");
        boost::asio::io_context io;
        Config config;
        config.execution_mode = "live"; config.latency_timestamps=true;
        config.rest_host = "localhost"; config.rest_port = argv[1]; config.ca_file = argv[2];
        config.hmac_api_key_file = argv[3]; config.hmac_secret_key_file = argv[4];
        Market market{}; market.symbol = "USDTTRY"; market.base = "USDT"; market.quote = "TRY";
        nlohmann::json write_events=nlohmann::json::array();
        BinanceGateway gateway(io, config, {market}, [&](std::string_view event,const nlohmann::json& fields) {
            if(event=="execution_order_write") write_events.push_back(fields);
        });
        const unsigned expected = argc == 6 ? static_cast<unsigned>(std::stoul(argv[5])) : 1;
        if (!expected || expected > 10) throw std::runtime_error("Invalid probe count");
        unsigned received = 0;
        nlohmann::json reports = nlohmann::json::array();
        boost::asio::steady_timer deadline(io, std::chrono::seconds(5));
        deadline.async_wait([&](auto ec) { if (!ec) io.stop(); });
        std::function<void()> next;
        next = [&] {
            gateway.submit(OrderRequest{"probe-first-leg-" + std::to_string(received), 0, false, "48.54", "20", false}, [&](OrderReport report) {
                const char* status = report.status == OrderStatus::Unknown ? "UNKNOWN" :
                    report.status == OrderStatus::Rejected ? "REJECTED" :
                    report.status == OrderStatus::Filled ? "FILLED" : "OTHER";
                reports.push_back({{"client_id",report.client_id},{"status", status}, {"failure", report.failure},{"latency",report.latency},{"write_events",write_events}});
                if (++received == expected) deadline.cancel();
                else next();
            });
        };
        next();
        io.run();
        if (received != expected) throw std::runtime_error("Gateway report timeout");
        std::cout << (argc == 6 ? reports : reports.at(0)).dump() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
