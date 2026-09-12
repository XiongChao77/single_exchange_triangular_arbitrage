#include "triangular/receiver.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <nlohmann/json.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <unordered_map>

namespace triangular {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using Error = boost::system::error_code;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

nlohmann::json fetch_exchange_info(const Config& config) {
    net::io_context io;
    ssl::context tls(ssl::context::tls_client);
    tls.set_default_verify_paths();
    if (!config.ca_file.empty()) tls.load_verify_file(config.ca_file.string());
    tls.set_verify_mode(ssl::verify_peer);
    if (SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION) != 1)
        throw std::runtime_error("Cannot configure REST TLS minimum version");
    beast::ssl_stream<beast::tcp_stream> stream(io, tls);
    stream.set_verify_callback(ssl::host_name_verification(config.rest_host));
    if (SSL_set_tlsext_host_name(stream.native_handle(), config.rest_host.c_str()) != 1)
        throw std::runtime_error("Cannot configure REST TLS SNI");
    std::string target = "/api/v3/exchangeInfo?symbols=";
    constexpr char hex[] = "0123456789ABCDEF";
    for (unsigned char ch : nlohmann::json(config.symbols).dump()) {
        target += '%'; target += hex[ch >> 4]; target += hex[ch & 15];
    }
    beast::http::request<beast::http::empty_body> request{beast::http::verb::get, target, 11};
    request.set(beast::http::field::host, config.rest_host + ":" + config.rest_port);
    request.set(beast::http::field::user_agent, "single_exchange_triangular_arbitrage/0.1");
    beast::flat_buffer buffer;
    beast::http::response_parser<beast::http::string_body> response;
    response.body_limit(16 * 1024 * 1024);
    tcp::resolver resolver(io);
    net::steady_timer deadline(io);
    Error failure;
    bool done = false;
    auto finish = [&](Error ec) {
        if (done) return;
        done = true;
        failure = ec;
        deadline.cancel();
        resolver.cancel();
        Error ignored;
        beast::get_lowest_layer(stream).socket().close(ignored);
    };
    deadline.expires_after(15s);
    deadline.async_wait([&](Error ec) { if (!ec) finish(net::error::timed_out); });
    resolver.async_resolve(config.rest_host, config.rest_port, [&](Error ec, tcp::resolver::results_type endpoints) {
        if (done) return;
        if (ec) return finish(ec);
        beast::get_lowest_layer(stream).async_connect(endpoints, [&](Error connect_error, const tcp::endpoint&) {
            if (done) return;
            if (connect_error) return finish(connect_error);
            stream.async_handshake(ssl::stream_base::client, [&](Error tls_error) {
                if (done) return;
                if (tls_error) return finish(tls_error);
                beast::http::async_write(stream, request, [&](Error write_error, std::size_t) {
                    if (done) return;
                    if (write_error) return finish(write_error);
                    beast::http::async_read(stream, buffer, response, [&](Error read_error, std::size_t) { finish(read_error); });
                });
            });
        });
    });
    io.run();
    if (failure) throw std::runtime_error("exchangeInfo request failed: " + failure.message());
    if (response.get().result() != beast::http::status::ok)
        throw std::runtime_error("exchangeInfo HTTP status " + std::to_string(response.get().result_int()));
    return nlohmann::json::parse(response.get().body());
}

struct Receiver::Impl : std::enable_shared_from_this<Receiver::Impl> {
    struct Session;
    net::io_context& io;
    Config config;
    OrderBookManager& orderbooks;
    AsyncLogger& logger;
    std::function<void(const ArbitrageOpportunity&)> on_opportunity;
    ssl::context tls{ssl::context::tls_client};
    net::steady_timer retry;
    std::shared_ptr<Session> session;
    std::vector<std::uint64_t> counts;
    Clock::time_point last_invalid_warning{};
    std::uint64_t invalid = 0;
    std::uint64_t received = 0, total_ingest_ns = 0, max_ingest_ns = 0, log_rejected = 0;
    unsigned retry_seconds = 1;
    bool stopped = false;
    bool started = false;

    Impl(net::io_context& context, Config c, OrderBookManager& manager, AsyncLogger& log, std::function<void(const ArbitrageOpportunity&)> handler)
        : io(context), config(std::move(c)), orderbooks(manager), logger(log), on_opportunity(std::move(handler)), retry(io), counts(config.symbols.size()) {
        tls.set_default_verify_paths();
        if (!config.ca_file.empty()) tls.load_verify_file(config.ca_file.string());
        tls.set_verify_mode(ssl::verify_peer);
        if (SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION) != 1)
            throw std::runtime_error("Cannot configure TLS minimum version");
    }

    void connect();
    void stop();
    void disconnected(const std::string& stage, Error ec, unsigned status = 0) {
        if (stopped) return;
        session.reset();
        logger.log("WARNING", "connection_lost", {{"stage", stage}, {"error", ec.message()},
            {"http_status", status}, {"retry_seconds", retry_seconds}});
        retry.expires_after(std::chrono::seconds(retry_seconds));
        retry_seconds = std::min(30U, retry_seconds * 2);
        retry.async_wait([self = shared_from_this()](Error error) {
            if (!error && !self->stopped) self->connect();
        });
    }

    void ingest(const nlohmann::json& message, std::size_t wire_bytes, std::int64_t start, std::int64_t received_time) {
        OrderBook orderbook;
        std::size_t index;
        try {
            orderbook = decode_best_bid_ask(message, received_time);
            index = orderbooks.index_of(orderbook.symbol);
        } catch (const std::exception& error) {
            ++invalid;
            const auto now = Clock::now();
            if (last_invalid_warning == Clock::time_point{} || now - last_invalid_warning >= 1s) {
                logger.log("WARNING", "invalid_json", {{"reason", error.what()}, {"invalid_total", invalid}});
                last_invalid_warning = now;
            }
            return;
        }
        retry_seconds = 1;
        orderbook.received_steady_ns = start;
        orderbook.receive_sequence = ++received;
        auto& count = counts[index];
        if (++count == 1) logger.log("INFO", "first_orderbook", {{"symbol", orderbook.symbol}});
        if (!logger.log("INFO", "receiver_orderbook", {
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
                {"wire_bytes", wire_bytes},
                {"exchange_to_receive_us", nullptr}})) ++log_rejected;
        orderbooks.update(index, std::move(orderbook));
        ArbitrageOpportunity opportunity;
        if (orderbooks.scan_edge(index, opportunity)) {
            auto legs = nlohmann::json::array();
            for (std::size_t i = 0; i < opportunity.path.size(); ++i) {
                const auto& leg = opportunity.path[i];
                legs.push_back({{"symbol", config.symbols.at(leg.index)}, {"buy", leg.buy},
                    {"price", opportunity.prices[i]}, {"qty", opportunity.quantities[i]},
                    {"book_update_id", opportunity.book_update_ids[i]}});
            }
            logger.log("INFO", "arbitrage_edge", {{"updated_index", index},
                {"group_index", opportunity.group_index}, {"input_usdt", opportunity.input_usdt},
                {"output_usdt", opportunity.output_usdt}, {"profit_usdt", opportunity.profit_usdt},
                {"net_return", opportunity.net_return}, {"legs", std::move(legs)}});
            if (on_opportunity) on_opportunity(opportunity);
        }
        const auto elapsed = static_cast<std::uint64_t>(steady_time_ns() - start);
        total_ingest_ns += elapsed;
        max_ingest_ns = std::max(max_ingest_ns, elapsed);
    }
};

struct Receiver::Impl::Session : std::enable_shared_from_this<Session> {
    std::weak_ptr<Impl> owner;
    tcp::resolver resolver;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws;
    net::steady_timer deadline;
    beast::flat_buffer buffer{1024 * 1024};
    websocket::response_type response;
    std::string host;
    std::string port;
    std::string target;
    bool ended = false;

    explicit Session(const std::shared_ptr<Impl>& impl)
        : owner(impl), resolver(impl->io), ws(impl->io, impl->tls), deadline(impl->io),
          host(impl->config.host), port(impl->config.port), target(stream_target(impl->config.symbols)) {}

    void cancel() {
        ended = true;
        resolver.cancel();
        deadline.cancel();
        Error ignored;
        auto& socket = beast::get_lowest_layer(ws).socket();
        socket.cancel(ignored);
        socket.shutdown(tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
    }
    void fail(const std::string& stage, Error ec, unsigned status = 0) {
        if (ended) return;
        cancel();
        if (auto impl = owner.lock()) impl->disconnected(stage, ec, status);
    }
    void start() {
        if (SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str()) != 1) {
            fail("tls_sni", net::error::invalid_argument);
            return;
        }
        ws.next_layer().set_verify_callback(ssl::host_name_verification(host));
        deadline.expires_after(15s);
        deadline.async_wait([self = shared_from_this()](Error ec) {
            if (!ec) self->fail("connect_timeout", net::error::timed_out);
        });
        resolver.async_resolve(host, port, [self = shared_from_this()](Error ec, tcp::resolver::results_type endpoints) {
            if (self->ended) return;
            if (ec) return self->fail("dns", ec);
            beast::get_lowest_layer(self->ws).expires_after(15s);
            beast::get_lowest_layer(self->ws).async_connect(endpoints,
                [self](Error error, const tcp::resolver::results_type::endpoint_type&) {
                    if (self->ended) return;
                    if (error) return self->fail("tcp", error);
                    self->ws.next_layer().async_handshake(ssl::stream_base::client, [self](Error tls_error) {
                        if (self->ended) return;
                        if (tls_error) return self->fail("tls", tls_error);
                        self->handshake();
                    });
                });
        });
    }
    void handshake() {
        const auto impl = owner.lock();
        if (!impl) return cancel();
        beast::get_lowest_layer(ws).expires_never();
        // async_read automatically handles ping/pong and fragmented text messages.
        ws.set_option(websocket::stream_base::timeout{15s, 60s, false});
        ws.read_message_max(1024 * 1024);
        ws.set_option(websocket::stream_base::decorator([](websocket::request_type& request) {
            request.set(beast::http::field::user_agent, "single_exchange_triangular_arbitrage/0.1");
        }));
        ws.async_handshake(response, host + ":" + port, target, [self = shared_from_this()](Error ec) {
            if (self->ended) return;
            if (ec) return self->fail("websocket", ec, self->response.result_int());
            self->deadline.cancel();
            if (auto receiver = self->owner.lock())
                receiver->logger.log("INFO", "connected", {{"host", self->host}, {"streams", self->target}});
            self->read();
        });
    }
    void read() {
        ws.async_read(buffer, [self = shared_from_this()](Error ec, std::size_t) {
            if (self->ended) return;
            if (ec) return self->fail("read", ec);
            const auto impl = self->owner.lock();
            if (!impl || impl->stopped) return self->cancel();
            const auto start = steady_time_ns();
            const auto received_time = wall_time_us();
            if (self->ws.got_text()) {
                const auto bytes = self->buffer.data();
                const auto* data = static_cast<const char*>(bytes.data());
                const auto message = nlohmann::json::parse(data, data + bytes.size(), nullptr, false);
                const auto& control = message.is_object() && message.contains("data") ? message["data"] : message;
                if (control.is_object() && control.contains("e") && control["e"] == "serverShutdown")
                    return self->fail("serverShutdown", net::error::connection_reset);
                if (message.is_object() && message.contains("code"))
                    return self->fail("stream_control_error", net::error::access_denied);
                if (!(message.is_object() && message.contains("result") && message["result"].is_null() && message.contains("id")))
                    impl->ingest(message, bytes.size(), start, received_time);
            } else {
                // A binary frame is not a JSON market-data message.
                impl->ingest(nullptr, self->buffer.size(), start, received_time);
            }
            self->buffer.consume(self->buffer.size());
            self->read();
        });
    }
};

void Receiver::Impl::connect() {
    if (stopped) return;
    session = std::make_shared<Session>(shared_from_this());
    session->start();
}
void Receiver::Impl::stop() {
    if (stopped) return;
    stopped = true;
    retry.cancel();
    if (session) session->cancel();
    // Release the closed WebSocket after its canceled handlers complete, so its
    // internal idle timer cannot keep io_context alive until the old deadline.
    session.reset();
    const auto s = orderbooks.stats();
    logger.log("INFO", "receiver_stopped", {{"received", s.received}, {"symbols", s.symbols},
        {"populated", s.populated}, {"invalid", invalid}});
    for (std::size_t index = 0; index < counts.size(); ++index)
        logger.log("INFO", "symbol_summary", {{"symbol", config.symbols[index]}, {"received", counts[index]}});
}
Receiver::Receiver(net::io_context& io, Config config, OrderBookManager& orderbooks, AsyncLogger& logger,
                   std::function<void(const ArbitrageOpportunity&)> on_opportunity)
    : impl_(std::make_shared<Impl>(io, std::move(config), orderbooks, logger, std::move(on_opportunity))) {}
Receiver::~Receiver() { impl_->stop(); }
void Receiver::start() {
    if (impl_->started || impl_->stopped) return;
    impl_->started = true;
    impl_->connect();
}
void Receiver::stop() { impl_->stop(); }
nlohmann::json Receiver::stats() const {
    return {{"received", impl_->received}, {"invalid", impl_->invalid},
        {"total_ingest_ns", impl_->total_ingest_ns}, {"max_ingest_ns", impl_->max_ingest_ns},
        {"avg_ingest_ns", impl_->received == 0 ? 0.0 :
            static_cast<double>(impl_->total_ingest_ns) / static_cast<double>(impl_->received)},
        {"log_rejected", impl_->log_rejected}};
}
} // namespace triangular
