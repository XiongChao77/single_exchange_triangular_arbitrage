#include "triangular/receiver.hpp"
#include "triangular/consumer.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <nlohmann/json.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

namespace triangular {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using Error = boost::system::error_code;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

struct Receiver::Impl : std::enable_shared_from_this<Receiver::Impl> {
    struct Session;
    net::io_context& io;
    Config config;
    std::string api_key;
    QuoteQueue& queue;
    AsyncLogger& logger;
    ssl::context tls{ssl::context::tls_client};
    net::steady_timer retry;
    std::shared_ptr<Session> session;
    std::unordered_set<std::string> subscribed;
    std::unordered_map<std::string, std::uint64_t> counts;
    Clock::time_point last_overflow_warning{};
    Clock::time_point last_invalid_warning{};
    std::uint64_t invalid = 0;
    std::uint64_t received = 0, total_ingest_ns = 0, max_ingest_ns = 0, log_rejected = 0;
    unsigned retry_seconds = 1;
    bool stopped = false;
    bool started = false;

    Impl(net::io_context& context, Config c, std::string key, QuoteQueue& q, AsyncLogger& log)
        : io(context), config(std::move(c)), api_key(std::move(key)), queue(q), logger(log), retry(io),
          subscribed(config.symbols.begin(), config.symbols.end()) {
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

    void ingest(std::span<const std::uint8_t> bytes) {
        const auto start = steady_time_ns();
        const auto received_time = wall_time_us();
        BestBidAsk quote;
        try {
            quote = decode_best_bid_ask(bytes, received_time);
            if (!subscribed.contains(quote.symbol)) throw std::runtime_error("Unsubscribed SBE symbol");
        } catch (const std::runtime_error& error) {
            ++invalid;
            const auto now = Clock::now();
            if (last_invalid_warning == Clock::time_point{} || now - last_invalid_warning >= 1s) {
                logger.log("WARNING", "invalid_sbe", {{"reason", error.what()}, {"invalid_total", invalid}});
                last_invalid_warning = now;
            }
            return;
        }
        retry_seconds = 1;
        quote.received_steady_ns = start;
        quote.receive_sequence = ++received;
        auto& count = counts[quote.symbol];
        if (++count == 1) logger.log("INFO", "first_quote", {{"symbol", quote.symbol}});
        auto fields = quote_fields(quote);
        fields["wire_bytes"] = bytes.size();
        fields["exchange_to_receive_us"] = received_time - quote.event_time_us;
        if (!logger.log("INFO", "receiver_quote", std::move(fields))) ++log_rejected;
        if (queue.push(std::move(quote))) {
            const auto now = Clock::now();
            if (last_overflow_warning == Clock::time_point{} || now - last_overflow_warning >= 1s) {
                const auto s = queue.stats();
                logger.log("WARNING", "quote_queue_full", {{"capacity", s.capacity}, {"dropped_total", s.dropped}});
                last_overflow_warning = now;
            }
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
        // async_read automatically handles ping/pong and fragmented binary messages.
        ws.set_option(websocket::stream_base::timeout{15s, 60s, false});
        ws.read_message_max(1024 * 1024);
        ws.set_option(websocket::stream_base::decorator([key = impl->api_key](websocket::request_type& request) {
            request.set("X-MBX-APIKEY", key);
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
            if (self->ws.got_binary()) {
                const auto bytes = self->buffer.data();
                impl->ingest({static_cast<const std::uint8_t*>(bytes.data()), bytes.size()});
            } else {
                const auto text = beast::buffers_to_string(self->buffer.data());
                const auto message = nlohmann::json::parse(text, nullptr, false);
                if (message.is_object() && message.contains("e") && message["e"] == "serverShutdown")
                    return self->fail("serverShutdown", net::error::connection_reset);
                if (message.is_object() && message.contains("code"))
                    return self->fail("stream_control_error", net::error::access_denied);
                // Do not log raw control payloads or HTTP responses: they may echo credentials.
                impl->logger.log("WARNING", "ignored_text_message");
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
    const auto s = queue.stats();
    logger.log("INFO", "receiver_stopped", {{"received", s.received}, {"queued", s.size},
        {"consumed", s.consumed}, {"dropped", s.dropped}, {"invalid", invalid}});
    for (const auto& symbol : config.symbols)
        logger.log("INFO", "symbol_summary", {{"symbol", symbol}, {"received", counts[symbol]}});
}
Receiver::Receiver(net::io_context& io, Config config, std::string api_key, QuoteQueue& queue, AsyncLogger& logger)
    : impl_(std::make_shared<Impl>(io, std::move(config), std::move(api_key), queue, logger)) {}
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
