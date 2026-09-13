#include "triangular/execution/execution.hpp"
#include "triangular/timestamp_stream.hpp"
#include "triangular/tls_keylog.hpp"
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
    nlohmann::json diagnostics;
};

class RequestError : public std::runtime_error {
public:
    RequestError(std::string message, bool uncertain, nlohmann::json details)
        : std::runtime_error(std::move(message)), uncertain_(uncertain), details_(std::move(details)) {}
    bool uncertain() const { return uncertain_; }
    const nlohmann::json& details() const { return details_; }
private:
    bool uncertain_;
    nlohmann::json details_;
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
    // Used sequentially during construction, then exclusively by the REST worker.
    net::io_context transport_io;
    std::unique_ptr<ssl::context> tls_context;
    std::unique_ptr<beast::ssl_stream<TimestampStream>> connection;
    beast::flat_buffer response_buffer;
    std::uint64_t connection_id = 0;
    Config config;
    std::vector<Market> markets;
    std::string api_key;
    std::string secret_key;
    ArbitrageExecutor::EventObserver latency_observer;
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

    Impl(net::io_context& io, const Config& c, std::vector<Market> m, ArbitrageExecutor::EventObserver observer)
        : callback_io(io), config(c), markets(std::move(m)),
          api_key(read_secret(c.hmac_api_key_file)), secret_key(read_secret(c.hmac_secret_key_file)), latency_observer(std::move(observer)) {
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

    std::string safe_message(std::string message, const std::string& target = "") const {
        auto redact = [&](const std::string& value) {
            if (value.empty()) return;
            std::size_t pos = 0;
            while ((pos = message.find(value, pos)) != std::string::npos) {
                message.replace(pos, value.size(), "[REDACTED]");
                pos += 10;
            }
        };
        // Redact before truncation, including an echoed signature without its key name.
        const auto signature = target.find("signature=");
        if (signature != std::string::npos) {
            const auto start = signature + 10;
            redact(target.substr(start, target.find('&', start) - start));
        }
        redact(target); redact(api_key); redact(secret_key);
        if (message.size() > 1024) message.resize(1024);
        return message;
    }

    void disconnect() {
        if (connection) {
            beast::error_code error;
            beast::get_lowest_layer(*connection).socket().close(error);
            connection.reset();
        }
        response_buffer.consume(response_buffer.size());
    }

    HttpResult request(http::verb method, const std::string& target,
                       const std::function<void(const nlohmann::json&)>& on_write = {}) {
        const auto begin = steady_time_ns();
        const char* stage = "tls_setup";
        bool write_started = false, write_completed = false;
        const bool reused = bool(connection);
        std::uint64_t tx_begin = connection ? connection->next_layer().tx_bytes() : 0, tx_end = tx_begin;
        nlohmann::json points = {{"request_begin_ns",begin},{"request_begin_realtime_ns",realtime_ns()}};
        nlohmann::json tx = nlohmann::json::object();
        nlohmann::json timing = nlohmann::json::object();
        auto details = [&] {
            return nlohmann::json{{"stage", stage}, {"origin", "transport"},
                {"method", std::string(http::to_string(method))}, {"endpoint", target.substr(0, target.find('?'))},
                {"request_write_started", write_started}, {"request_write_completed", write_completed},
                {"elapsed_us", (steady_time_ns() - begin) / 1000}, {"timing_us", timing},
                {"connection_reused", reused}, {"connection_id", connection_id},
                {"timepoints",points},{"kernel_tx",tx}};
        };
        try {
            auto phase_begin = steady_time_ns();
            if (!connection) {
                if (!tls_context) {
                    tls_context = std::make_unique<ssl::context>(ssl::context::tls_client);
                    enable_tls_keylog(tls_context->native_handle(), config.tls_keylog_file);
                    tls_context->set_default_verify_paths();
                    if (!config.ca_file.empty()) tls_context->load_verify_file(config.ca_file.string());
                    tls_context->set_verify_mode(ssl::verify_peer);
                    if (SSL_CTX_set_min_proto_version(tls_context->native_handle(), TLS1_2_VERSION) != 1)
                        throw std::runtime_error("Cannot configure Binance REST TLS");
                }
                connection = std::make_unique<beast::ssl_stream<TimestampStream>>(transport_io, *tls_context);
                ++connection_id;
                connection->set_verify_callback(ssl::host_name_verification(config.rest_host));
                if (SSL_set_tlsext_host_name(connection->native_handle(), config.rest_host.c_str()) != 1)
                    throw std::runtime_error("Cannot configure Binance REST SNI");
                beast::get_lowest_layer(*connection).expires_after(std::chrono::seconds(10));
                stage = "dns_resolve";
                phase_begin = steady_time_ns();
                tcp::resolver resolver(transport_io);
                const auto endpoints = resolver.resolve(config.rest_host, config.rest_port);
                timing["dns"] = (steady_time_ns() - phase_begin) / 1000;
                stage = "tcp_connect"; phase_begin = steady_time_ns();
                beast::get_lowest_layer(*connection).connect(endpoints);
                timing["tcp_connect"] = (steady_time_ns() - phase_begin) / 1000;
                points["tcp_connected_ns"]=steady_time_ns();
                if(config.latency_timestamps) connection->next_layer().enable(false,true);
                stage = "tls_handshake"; phase_begin = steady_time_ns();
                connection->handshake(ssl::stream_base::client);
                timing["tls_handshake"] = (steady_time_ns() - phase_begin) / 1000;
                points["tls_ready_ns"]=steady_time_ns();
                beast::get_lowest_layer(*connection).socket().set_option(tcp::no_delay(true));
            }
            auto& stream = *connection;
            if(config.latency_timestamps) stream.next_layer().drain_tx(0,0);
            tx_begin=stream.next_layer().tx_bytes();
            beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
            stage = "request_build";
            http::request<http::empty_body> req(method, target, 11);
            req.set(http::field::host, config.rest_host);
            req.set(http::field::user_agent, "single-exchange-triangular-arbitrage/0.1");
            req.set("X-MBX-APIKEY", api_key);
            req.keep_alive(true);
            stage = "http_write"; phase_begin = steady_time_ns(); write_started = true;
            points["http_write_begin_ns"]=steady_time_ns();
            points["http_write_begin_realtime_ns"]=realtime_ns();
            http::write(stream, req);
            points["http_write_end_ns"]=steady_time_ns();
            points["http_write_end_realtime_ns"]=realtime_ns();
            tx_end=stream.next_layer().tx_bytes();
            write_completed = true; timing["http_write"] = (steady_time_ns() - phase_begin) / 1000;
            if(on_write) on_write(details());
            stage = "http_read"; phase_begin = steady_time_ns();
            http::response<http::string_body> response;
            http::read(stream, response_buffer, response);
            timing["http_read"] = (steady_time_ns() - phase_begin) / 1000;
            points["http_response_ns"]=steady_time_ns();
            if(config.latency_timestamps) tx=stream.next_layer().drain_tx(tx_begin,tx_end);
            auto diagnostic = details();
            diagnostic["http_status"] = response.result_int();
            // Do not replay POSTs on a stale connection: the exchange may have accepted them.
            // A complete response remains valid even when the server closes afterward.
            diagnostic["connection_keep_alive"] = response.keep_alive();
            if (!response.keep_alive()) disconnect();
            return {response.result_int(), std::move(response.body()), std::move(diagnostic)};
        } catch (const boost::system::system_error& error) {
            if(config.latency_timestamps && connection) {
                tx_end=connection->next_layer().tx_bytes();
                tx=connection->next_layer().drain_tx(tx_begin,tx_end);
            }
            points["transport_error_ns"]=steady_time_ns();
            auto diagnostic = details();
            disconnect();
            diagnostic["error_category"] = error.code().category().name();
            diagnostic["error_code"] = error.code().value();
            diagnostic["message"] = safe_message(error.what(), target);
            const bool uncertain = method == http::verb::post && write_started;
            diagnostic["outcome_uncertain"] = uncertain;
            const auto message = diagnostic["message"].get<std::string>();
            throw RequestError(message, uncertain, std::move(diagnostic));
        } catch (const std::exception& error) {
            if(config.latency_timestamps && connection) {
                tx_end=connection->next_layer().tx_bytes();
                tx=connection->next_layer().drain_tx(tx_begin,tx_end);
            }
            points["transport_error_ns"]=steady_time_ns();
            auto diagnostic = details();
            disconnect();
            diagnostic["message"] = safe_message(error.what(), target);
            const bool uncertain = method == http::verb::post && write_started;
            diagnostic["outcome_uncertain"] = uncertain;
            const auto message = diagnostic["message"].get<std::string>();
            throw RequestError(message, uncertain, std::move(diagnostic));
        }
    }

    std::string signed_target(const std::string& path, std::string parameters) const {
        if (!parameters.empty()) parameters += '&';
        parameters += "recvWindow=3000&timestamp=" + std::to_string(wall_time_us() / 1000);
        return path + '?' + parameters + "&signature=" + hmac_sha256_hex(secret_key, parameters);
    }

    nlohmann::json checked(http::verb method, const std::string& target, bool submission,
                           nlohmann::json* request_info = nullptr,
                           const std::function<void(const nlohmann::json&)>& on_write = {}) {
        const auto response = request(method, target,on_write);
        if (request_info) *request_info = response.diagnostics;
        auto body = nlohmann::json::parse(response.body, nullptr, false);
        const bool has_code = body.is_object() && body.contains("code") && body["code"].is_number_integer();
        const auto code = has_code ? body["code"].get<std::int64_t>() : 0;
        if (response.status < 200 || response.status >= 300 || body.is_discarded() || code < 0) {
            const bool uncertain = submission && (response.status >= 500 || code == -1006 || code == -1007 ||
                (body.is_discarded() && response.status >= 200 && response.status < 300));
            auto diagnostic = response.diagnostics;
            diagnostic["stage"] = body.is_discarded() ? "response_parse" : "exchange_response";
            diagnostic["origin"] = has_code ? "exchange" : "response";
            diagnostic["outcome_uncertain"] = uncertain;
            if (has_code) diagnostic["binance_code"] = code;
            if (body.is_object() && body.contains("msg") && body["msg"].is_string())
                diagnostic["binance_message"] = safe_message(body["msg"].get<std::string>(), target);
            const auto message = "Binance REST request failed: HTTP " + std::to_string(response.status) +
                " code " + std::to_string(code);
            diagnostic["message"] = message;
            throw RequestError(message, uncertain, std::move(diagnostic));
        }
        return body;
    }

    OrderReport failure_report(const OrderRequest& request_value, const RequestError& error) {
        OrderReport report;
        report.client_id = request_value.client_id;
        report.revision = ++revisions[report.client_id];
        report.status = error.uncertain() ? OrderStatus::Unknown : OrderStatus::Rejected;
        report.failure = error.details();
        return report;
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
        net::post(callback_io, [callback = std::move(callback), report = std::move(report), observer=latency_observer]() mutable {
            if(!report.latency.empty()) {
                report.latency["callback_received_ns"]=steady_time_ns();
                report.latency["callback_received_realtime_ns"]=realtime_ns();
                if(observer) {
                    const char* status="UNKNOWN";
                    switch(report.status) {
                    case OrderStatus::Open:status="OPEN";break;
                    case OrderStatus::PartiallyFilled:status="PARTIALLY_FILLED";break;
                    case OrderStatus::Filled:status="FILLED";break;
                    case OrderStatus::Canceled:status="CANCELED";break;
                    case OrderStatus::Expired:status="EXPIRED";break;
                    case OrderStatus::Rejected:status="REJECTED";break;
                    case OrderStatus::Unknown:break;
                    }
                    observer("execution_order_latency",{{"client_id",report.client_id},{"order_status",status},
                        {"filled_qty",decimal_text(report.filled_qty)},{"filled_quote",decimal_text(report.filled_quote)},
                        {"latency",report.latency}});
                }
            }
            callback(std::move(report));
        });
    }

    void submit(const OrderRequest& request_value, Callback callback) {
        const auto enqueued_ns=steady_time_ns();
        enqueue([self = shared_from_this(), request_value, enqueued_ns, callback = std::move(callback)]() mutable {
            const char* stage = "parameters";
            std::string target;
            nlohmann::json request_info = nlohmann::json::object();
            auto latency=request_value.latency;
            if(self->config.latency_timestamps) {
                latency["gateway_enqueued_ns"]=enqueued_ns;
                latency["worker_begin_ns"]=steady_time_ns();
            }
            auto finish=[&](OrderReport report) {
                if(self->config.latency_timestamps) {
                    latency["transport"]=report.failure.empty()?request_info:report.failure;
                    latency["report_ready_ns"]=steady_time_ns();
                    report.latency=latency;
                }
                self->deliver(std::move(callback),std::move(report));
            };
            try {
                const auto& market = self->markets.at(request_value.symbol_index);
                const auto parameters = "symbol=" + market.symbol + "&side=" + (request_value.buy ? "BUY" : "SELL") +
                    "&type=LIMIT&timeInForce=" + (request_value.initial_clear ? "IOC" : "FOK") +
                    "&quantity=" + request_value.quantity + "&price=" + request_value.price +
                    "&newClientOrderId=" + request_value.client_id + "&newOrderRespType=FULL";
                stage = "request_sign";
                if(self->config.latency_timestamps) latency["sign_begin_ns"]=steady_time_ns();
                target = self->signed_target("/api/v3/order", parameters);
                if(self->config.latency_timestamps) latency["sign_end_ns"]=steady_time_ns();
                stage = "request";
                const auto order = self->checked(http::verb::post, target, true, &request_info,
                    [&](const nlohmann::json& progress) {
                        if(!self->config.latency_timestamps || !self->latency_observer) return;
                        auto partial=latency;partial["transport"]=progress;
                        nlohmann::json fields={{"client_id",request_value.client_id},{"latency",std::move(partial)}};
                        net::post(self->callback_io,[observer=self->latency_observer,fields=std::move(fields)] {
                            observer("execution_order_write",fields);
                        });
                    });
                stage = "response_decode";
                auto report = self->parse_order(request_value, order);
                stage = "balance_update";
                self->apply_report(request_value, report);
                finish(std::move(report));
            } catch (const RequestError& error) {
                finish(self->failure_report(request_value, error));
            } catch (const std::exception& error) {
                OrderReport report; report.client_id = request_value.client_id;
                report.revision = ++self->revisions[report.client_id];
                const bool uncertain = std::string_view(stage) != "parameters" && std::string_view(stage) != "request_sign";
                report.status = uncertain ? OrderStatus::Unknown : OrderStatus::Rejected;
                report.failure = std::move(request_info);
                report.failure.update({{"stage", stage}, {"origin", "local"},
                    {"method", "POST"}, {"endpoint", "/api/v3/order"},
                    {"outcome_uncertain", uncertain}, {"message", self->safe_message(error.what(), target)}});
                finish(std::move(report));
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

BinanceGateway::BinanceGateway(net::io_context& io, const Config& config, std::vector<Market> markets, ArbitrageExecutor::EventObserver observer)
    : impl_(std::make_shared<Impl>(io, config, std::move(markets), std::move(observer))) {}
BinanceGateway::~BinanceGateway() = default;
Balances BinanceGateway::balances() const { return impl_->balances(); }
void BinanceGateway::submit(const OrderRequest& request, Callback callback) { impl_->submit(request, std::move(callback)); }
void BinanceGateway::query(const OrderRequest& request, Callback callback) { impl_->query_order(request, std::move(callback)); }
void BinanceGateway::cancel(const OrderRequest& request, Callback callback) { impl_->cancel_order(request, std::move(callback)); }
} // namespace triangular::execution
