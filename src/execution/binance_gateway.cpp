#include "triangular/execution/execution.hpp"
#include "triangular/logger.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/hmac.h>
#include <openssl/ssl.h>

#include <condition_variable>
#include <deque>
#include <fstream>
#include <functional>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>

namespace triangular::execution {
namespace {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

std::string read_secret(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("Cannot open Binance credential file: " + path.string());
    std::string value((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    while (!value.empty() && (value.back() == '\n' || value.back() == '\r')) value.pop_back();
    if (value.empty()) throw std::runtime_error("Empty Binance credential file: " + path.string());
    return value;
}

struct HttpResult {
    unsigned status{};
    std::string body;
};

class RequestError : public std::runtime_error {
public:
    RequestError(std::string message, bool uncertain) : std::runtime_error(std::move(message)), uncertain_(uncertain) {}
    bool uncertain() const { return uncertain_; }
private:
    bool uncertain_;
};

OrderStatus order_status(const std::string& status) {
    if (status == "NEW" || status == "PENDING_NEW") return OrderStatus::Open;
    if (status == "PARTIALLY_FILLED") return OrderStatus::PartiallyFilled;
    if (status == "FILLED") return OrderStatus::Filled;
    if (status == "CANCELED" || status == "PENDING_CANCEL") return OrderStatus::Canceled;
    if (status == "EXPIRED" || status == "EXPIRED_IN_MATCH") return OrderStatus::Expired;
    if (status == "REJECTED") return OrderStatus::Rejected;
    return OrderStatus::Unknown;
}

Decimal json_decimal(const nlohmann::json& object, const char* key) {
    const auto& value = object.at(key);
    if (value.is_string()) return Decimal(value.get_ref<const std::string&>());
    if (value.is_number()) return Decimal(value.dump());
    throw std::runtime_error(std::string("Expected decimal field: ") + key);
}
}

std::string hmac_sha256_hex(const std::string& secret, const std::string& payload) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned length = 0;
    if (!HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
              reinterpret_cast<const unsigned char*>(payload.data()), payload.size(), digest, &length))
        throw std::runtime_error("HMAC-SHA256 failed");
    std::ostringstream result;
    result << std::hex << std::setfill('0');
    for (unsigned i = 0; i < length; ++i) result << std::setw(2) << static_cast<unsigned>(digest[i]);
    return result.str();
}

struct BinanceGateway::Impl : std::enable_shared_from_this<Impl> {
    net::io_context& callback_io;
    Config config;
    std::vector<Market> markets;
    std::string api_key;
    std::string secret_key;
    mutable std::mutex balance_mutex;
    Balances wallet;
    std::mutex queue_mutex;
    std::condition_variable queue_ready;
    std::deque<std::function<void()>> queue;
    bool stopping = false;
    std::thread worker;
    std::map<std::string, std::uint64_t> revisions;
    std::map<std::string, std::int64_t> order_ids;
    std::map<std::string, OrderReport> accounted_reports;

    Impl(net::io_context& io, const Config& c, std::vector<Market> m)
        : callback_io(io), config(c), markets(std::move(m)),
          api_key(read_secret(c.api_key_file)), secret_key(read_secret(c.secret_key_file)) {
        if (config.execution_mode != "live") throw std::runtime_error("BinanceGateway requires live execution mode");
        refresh_balances();
        worker = std::thread([this] { run(); });
    }

    ~Impl() {
        {
            std::lock_guard lock(queue_mutex);
            stopping = true;
        }
        queue_ready.notify_one();
        if (worker.joinable()) worker.join();
    }

    HttpResult request(http::verb method, const std::string& target) const {
        net::io_context io;
        ssl::context tls(ssl::context::tls_client);
        tls.set_default_verify_paths();
        if (!config.ca_file.empty()) tls.load_verify_file(config.ca_file.string());
        tls.set_verify_mode(ssl::verify_peer);
        if (SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION) != 1)
            throw RequestError("Cannot configure Binance REST TLS", method == http::verb::post);
        tcp::resolver resolver(io);
        beast::ssl_stream<beast::tcp_stream> stream(io, tls);
        stream.set_verify_callback(ssl::host_name_verification(config.rest_host));
        if (SSL_set_tlsext_host_name(stream.native_handle(), config.rest_host.c_str()) != 1)
            throw RequestError("Cannot configure Binance REST SNI", method == http::verb::post);
        beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
        const auto endpoints = resolver.resolve(config.rest_host, config.rest_port);
        beast::get_lowest_layer(stream).connect(endpoints);
        stream.handshake(ssl::stream_base::client);
        http::request<http::empty_body> req(method, target, 11);
        req.set(http::field::host, config.rest_host);
        req.set(http::field::user_agent, "single-exchange-triangular-arbitrage/0.1");
        req.set("X-MBX-APIKEY", api_key);
        http::write(stream, req);
        beast::flat_buffer buffer;
        http::response<http::string_body> response;
        http::read(stream, buffer, response);
        beast::error_code ec;
        stream.shutdown(ec);
        if (ec == net::error::eof || ec == ssl::error::stream_truncated) ec = {};
        if (ec) throw RequestError("Binance REST TLS shutdown failed", method == http::verb::post);
        return {response.result_int(), std::move(response.body())};
    }

    std::string signed_target(const std::string& path, std::string parameters) const {
        if (!parameters.empty()) parameters += '&';
        parameters += "recvWindow=3000&timestamp=" + std::to_string(wall_time_us() / 1000);
        return path + '?' + parameters + "&signature=" + hmac_sha256_hex(secret_key, parameters);
    }

    nlohmann::json checked(http::verb method, const std::string& target, bool submission) const {
        HttpResult response;
        try { response = request(method, target); }
        catch (const RequestError&) { throw; }
        catch (const std::exception& error) { throw RequestError(error.what(), submission); }
        auto body = nlohmann::json::parse(response.body, nullptr, false);
        const int code = body.is_object() && body.contains("code") && body["code"].is_number_integer()
            ? body["code"].get<int>() : 0;
        if (response.status < 200 || response.status >= 300 || body.is_discarded()) {
            const bool uncertain = submission && (response.status >= 500 || code == -1007);
            throw RequestError("Binance REST request failed: HTTP " + std::to_string(response.status) +
                               " code " + std::to_string(code), uncertain);
        }
        return body;
    }

    void refresh_balances() {
        const auto account = checked(http::verb::get, signed_target("/api/v3/account", "omitZeroBalances=true"), false);
        Balances latest;
        for (const auto& balance : account.at("balances"))
            latest[balance.at("asset").get<std::string>()] = json_decimal(balance, "free");
        std::lock_guard lock(balance_mutex);
        wallet = std::move(latest);
    }

    Balances balances() const {
        std::lock_guard lock(balance_mutex);
        return wallet;
    }

    void apply_report(const OrderRequest& request_value, const OrderReport& report) {
        auto& previous = accounted_reports[report.client_id];
        if (report.filled_qty < previous.filled_qty || report.filled_quote < previous.filled_quote) return;
        const auto& market = markets.at(request_value.symbol_index);
        const Decimal filled_delta = report.filled_qty - previous.filled_qty;
        const Decimal quote_delta = report.filled_quote - previous.filled_quote;
        std::lock_guard lock(balance_mutex);
        wallet[market.base] += request_value.buy ? filled_delta : -filled_delta;
        wallet[market.quote] += request_value.buy ? -quote_delta : quote_delta;
        for (const auto& [asset, commission] : report.commissions)
            wallet[asset] -= commission - previous.commissions[asset];
        previous = report;
    }

    OrderReport parse_order(const OrderRequest& request_value, const nlohmann::json& order,
                            const nlohmann::json* trades = nullptr) {
        OrderReport report;
        report.client_id = request_value.client_id;
        report.revision = ++revisions[report.client_id];
        report.status = order_status(order.at("status").get<std::string>());
        report.filled_qty = json_decimal(order, "executedQty");
        report.filled_quote = json_decimal(order, "cummulativeQuoteQty");
        if (order.contains("orderId")) order_ids[report.client_id] = order.at("orderId").get<std::int64_t>();
        if (trades) {
            Decimal trade_qty = 0;
            for (const auto& trade : *trades) {
                if (trade.at("orderId").get<std::int64_t>() != order_ids.at(report.client_id)) continue;
                trade_qty += json_decimal(trade, "qty");
                report.commissions[trade.at("commissionAsset").get<std::string>()] += json_decimal(trade, "commission");
            }
            report.accounting_complete = trade_qty == report.filled_qty;
        } else if (order.contains("fills")) {
            Decimal trade_qty = 0;
            for (const auto& fill : order.at("fills")) {
                trade_qty += json_decimal(fill, "qty");
                report.commissions[fill.at("commissionAsset").get<std::string>()] += json_decimal(fill, "commission");
            }
            report.accounting_complete = trade_qty == report.filled_qty;
        } else {
            report.accounting_complete = report.filled_qty == 0;
        }
        return report;
    }

    OrderReport query_now(const OrderRequest& request_value) {
        const auto& market = markets.at(request_value.symbol_index);
        const auto order = checked(http::verb::get, signed_target("/api/v3/order",
            "symbol=" + market.symbol + "&origClientOrderId=" + request_value.client_id), false);
        order_ids[request_value.client_id] = order.at("orderId").get<std::int64_t>();
        nlohmann::json trades = nlohmann::json::array();
        if (json_decimal(order, "executedQty") > 0) {
            trades = checked(http::verb::get, signed_target("/api/v3/myTrades",
                "symbol=" + market.symbol + "&orderId=" + std::to_string(order_ids.at(request_value.client_id)) +
                "&limit=1000"), false);
        }
        auto report = parse_order(request_value, order, &trades);
        apply_report(request_value, report);
        return report;
    }

    void deliver(Callback callback, OrderReport report) {
        net::post(callback_io, [callback = std::move(callback), report = std::move(report)]() mutable {
            callback(std::move(report));
        });
    }

    void submit(const OrderRequest& request_value, Callback callback) {
        enqueue([self = shared_from_this(), request_value, callback = std::move(callback)]() mutable {
            try {
                const auto& market = self->markets.at(request_value.symbol_index);
                const auto parameters = "symbol=" + market.symbol + "&side=" + (request_value.buy ? "BUY" : "SELL") +
                    "&type=LIMIT&timeInForce=" + (request_value.initial_clear ? "IOC" : "FOK") +
                    "&quantity=" + request_value.quantity + "&price=" + request_value.price +
                    "&newClientOrderId=" + request_value.client_id + "&newOrderRespType=FULL";
                const auto order = self->checked(http::verb::post,
                    self->signed_target("/api/v3/order", parameters), true);
                auto report = self->parse_order(request_value, order);
                self->apply_report(request_value, report);
                self->deliver(std::move(callback), std::move(report));
            } catch (const RequestError& error) {
                OrderReport report; report.client_id = request_value.client_id;
                report.revision = ++self->revisions[report.client_id];
                report.status = error.uncertain() ? OrderStatus::Unknown : OrderStatus::Rejected;
                self->deliver(std::move(callback), std::move(report));
            } catch (...) {
                OrderReport report; report.client_id = request_value.client_id;
                report.revision = ++self->revisions[report.client_id]; report.status = OrderStatus::Unknown;
                self->deliver(std::move(callback), std::move(report));
            }
        });
    }

    void query_order(const OrderRequest& request_value, Callback callback) {
        enqueue([self = shared_from_this(), request_value, callback = std::move(callback)]() mutable {
            try { self->deliver(std::move(callback), self->query_now(request_value)); }
            catch (...) {
                OrderReport report; report.client_id = request_value.client_id;
                report.revision = ++self->revisions[report.client_id]; report.status = OrderStatus::Unknown;
                self->deliver(std::move(callback), std::move(report));
            }
        });
    }

    void cancel_order(const OrderRequest& request_value, Callback callback) {
        enqueue([self = shared_from_this(), request_value, callback = std::move(callback)]() mutable {
            try {
                const auto& market = self->markets.at(request_value.symbol_index);
                self->checked(http::verb::delete_, self->signed_target("/api/v3/order",
                    "symbol=" + market.symbol + "&origClientOrderId=" + request_value.client_id), false);
                self->deliver(std::move(callback), self->query_now(request_value));
            } catch (...) {
                OrderReport report; report.client_id = request_value.client_id;
                report.revision = ++self->revisions[report.client_id]; report.status = OrderStatus::Unknown;
                self->deliver(std::move(callback), std::move(report));
            }
        });
    }

    void enqueue(std::function<void()> operation) {
        {
            std::lock_guard lock(queue_mutex);
            if (stopping) throw std::runtime_error("BinanceGateway is stopping");
            queue.push_back(std::move(operation));
        }
        queue_ready.notify_one();
    }

    void run() {
        for (;;) {
            std::function<void()> operation;
            {
                std::unique_lock lock(queue_mutex);
                queue_ready.wait(lock, [&] { return stopping || !queue.empty(); });
                if (queue.empty()) {
                    if (stopping) return;
                    continue;
                }
                operation = std::move(queue.front());
                queue.pop_front();
            }
            operation();
        }
    }
};

BinanceGateway::BinanceGateway(net::io_context& io, const Config& config, std::vector<Market> markets)
    : impl_(std::make_shared<Impl>(io, config, std::move(markets))) {}
BinanceGateway::~BinanceGateway() = default;
Balances BinanceGateway::balances() const { return impl_->balances(); }
void BinanceGateway::submit(const OrderRequest& request, Callback callback) { impl_->submit(request, std::move(callback)); }
void BinanceGateway::query(const OrderRequest& request, Callback callback) { impl_->query_order(request, std::move(callback)); }
void BinanceGateway::cancel(const OrderRequest& request, Callback callback) { impl_->cancel_order(request, std::move(callback)); }
} // namespace triangular::execution
