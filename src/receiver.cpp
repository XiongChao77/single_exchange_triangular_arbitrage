#include "triangular/receiver.hpp"
#include "triangular/timestamp_stream.hpp"
#include "triangular/tls_keylog.hpp"
#include <openssl/sha.h>

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
#include <array>
#include <atomic>
#include <cstring>
#include <thread>
#include <boost/asio/post.hpp>

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
    enable_tls_keylog(tls.native_handle(), config.tls_keylog_file);
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
    // Network state belongs to network_thread; books and execution stay on the caller thread.
    net::io_context io;
    std::thread network_thread;
    static constexpr std::size_t queue_capacity = 1024;
    static constexpr std::size_t max_message_bytes = 16 * 1024;
    struct RawMessage {
        std::array<char, max_message_bytes> payload;
        std::size_t size = 0;
        std::int64_t received_ns = 0, received_us = 0, received_realtime_ns = 0, published_ns = 0;
        KernelRxWindow kernel_rx;
        std::uint64_t sequence = 0;
        bool text = true;
        std::weak_ptr<Session> source;
    };
    std::array<RawMessage, queue_capacity> queue;
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    alignas(64) std::atomic<std::uint64_t> write_index{0};
    alignas(64) std::atomic<std::uint64_t> read_index{0};
    std::atomic<std::uint64_t> wire_received{0}, queue_full{0}, oversized{0}, high_water{0};
    std::uint64_t total_queue_wait_ns = 0, max_queue_wait_ns = 0;
    bool processing_stopped = false;
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

    Impl(Config c, OrderBookManager& manager, AsyncLogger& log, std::function<void(const ArbitrageOpportunity&)> handler)
        : config(std::move(c)), orderbooks(manager), logger(log), on_opportunity(std::move(handler)),
          retry(io), counts(config.symbols.size()) {
        tls.set_default_verify_paths();
        enable_tls_keylog(tls.native_handle(), config.tls_keylog_file);
        if (!config.ca_file.empty()) tls.load_verify_file(config.ca_file.string());
        tls.set_verify_mode(ssl::verify_peer);
        if (SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION) != 1)
            throw std::runtime_error("Cannot configure TLS minimum version");
    }

    void connect();
    void stop();
    void process(std::size_t max_batch);
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

    void ingest(const nlohmann::json& message, std::size_t wire_bytes, std::int64_t start,
                std::int64_t received_time, std::int64_t json_parse_ns,
                std::uint64_t sequence, std::int64_t queue_wait_ns, std::int64_t process_begin,
                std::int64_t received_realtime_ns, const KernelRxWindow& rx_window) {
        const auto kernel_rx=config.latency_timestamps ? rx_window.json() : nlohmann::json::object();
        OrderBook orderbook;
        std::size_t index;
        const auto decode_begin = steady_time_ns();
        std::int64_t decode_end, lookup_end;
        try {
            orderbook = decode_partial_depth(message, received_time);
            decode_end = steady_time_ns();
            index = orderbooks.index_of(orderbook.symbol);
            lookup_end = steady_time_ns();
        } catch (const std::exception& error) {
            ++invalid;
            const auto now = Clock::now();
            if (last_invalid_warning == Clock::time_point{} || now - last_invalid_warning >= 1s) {
                logger.log("WARNING", "invalid_json", {{"reason", error.what()}, {"invalid_total", invalid}});
                last_invalid_warning = now;
            }
            return;
        }
        orderbook.received_steady_ns = start;
        orderbook.receive_sequence = sequence;
        ++received;
        auto& count = counts[index];
        if (++count == 1) logger.log("INFO", "first_orderbook", {{"symbol", orderbook.symbol}});
        const auto update_begin = steady_time_ns();
        orderbooks.update(index, std::move(orderbook));
        const auto update_end = steady_time_ns();
        ArbitrageOpportunity opportunity;
        const auto scan_begin = steady_time_ns();
        const bool edge_found = orderbooks.scan_edge(index, opportunity);
        const auto scan_end = steady_time_ns();
        if (edge_found) {
            opportunity.trigger_receive_sequence = sequence;
            opportunity.trigger_symbol_index = index;
            opportunity.market_received_ns = start;
            opportunity.market_received_realtime_ns = received_realtime_ns;
            opportunity.market_processed_ns = process_begin;
            opportunity.edge_found_ns = scan_end;
            opportunity.kernel_rx = kernel_rx;
            auto legs = nlohmann::json::array();
            for (std::size_t i = 0; i < opportunity.path.size(); ++i) {
                const auto& leg = opportunity.path[i];
                legs.push_back({{"symbol", config.symbols.at(leg.index)}, {"buy", leg.buy},
                    {"price", opportunity.prices[i]}, {"qty", opportunity.quantities[i]},
                    {"book_update_id", opportunity.book_update_ids[i]}, {"receive_sequence", opportunity.receive_sequences[i]}});
            }
            logger.log("INFO", "arbitrage_edge", {{"updated_index", index},
                {"trigger_receive_sequence", sequence}, {"group_index", opportunity.group_index}, {"input_usdt", opportunity.input_usdt},
                {"output_usdt", opportunity.output_usdt}, {"profit_usdt", opportunity.profit_usdt},
                {"net_return", opportunity.net_return}, {"legs", std::move(legs)}});
            if (on_opportunity) on_opportunity(opportunity);
        }
        const auto processing_end = steady_time_ns();
        if(config.latency_timestamps) logger.log("INFO","market_latency", {
            {"receive_sequence",sequence},{"symbol",config.symbols[index]},
            {"market_received_ns",start},{"market_received_realtime_ns",received_realtime_ns},
            {"market_processed_ns",process_begin},{"depth_decode_begin_ns",decode_begin},
            {"depth_decode_end_ns",decode_end},{"book_update_begin_ns",update_begin},
            {"book_update_end_ns",update_end},{"scan_begin_ns",scan_begin},{"edge_found_ns",scan_end},
            {"processing_end_ns",processing_end},{"kernel_rx",kernel_rx}});
        // update() moves the symbol string; scalar fields remain available for logging.
        if (!logger.log("INFO", "receiver_orderbook", {
                {"receive_sequence", orderbook.receive_sequence}, {"symbol", config.symbols[index]},
                {"event_time_us", orderbook.event_time_us ? nlohmann::json(*orderbook.event_time_us) : nlohmann::json(nullptr)},
                {"received_time_us", orderbook.received_time_us}, {"book_update_id", orderbook.book_update_id},
                {"price_exponent", orderbook.price_exponent}, {"qty_exponent", orderbook.qty_exponent},
                {"bid_price", orderbook.bid_price}, {"bid_qty", orderbook.bid_qty},
                {"ask_price", orderbook.ask_price}, {"ask_qty", orderbook.ask_qty},
                {"bid_levels", orderbook.bid_levels}, {"ask_levels", orderbook.ask_levels},
                {"wire_bytes", wire_bytes}, {"exchange_to_receive_us", nullptr},
                {"queue_wait_ns", queue_wait_ns},
                {"receive_to_decision_ns", scan_end - start},
                {"json_parse_ns", json_parse_ns}, {"depth_decode_ns", decode_end - decode_begin},
                {"symbol_lookup_ns", lookup_end - decode_end}, {"book_update_ns", update_end - update_begin},
                {"scan_edge_ns", scan_end - scan_begin},
                {"opportunity_ns", edge_found ? processing_end - scan_end : 0},
                {"processing_before_log_ns", processing_end - start}, {"edge_found", edge_found}})) ++log_rejected;
        const auto elapsed = static_cast<std::uint64_t>(steady_time_ns() - start);
        total_ingest_ns += elapsed;
        max_ingest_ns = std::max(max_ingest_ns, elapsed);
    }
};

struct Receiver::Impl::Session : std::enable_shared_from_this<Session> {
    std::weak_ptr<Impl> owner;
    tcp::resolver resolver;
    websocket::stream<beast::ssl_stream<TimestampStream>> ws;
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
                    if(auto receiver=self->owner.lock(); receiver && receiver->config.latency_timestamps)
                        self->ws.next_layer().next_layer().enable(true,false);
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
            if(auto receiver=self->owner.lock(); receiver && receiver->config.latency_timestamps)
                receiver->logger.log("INFO","kernel_timestamp_capability",self->ws.next_layer().next_layer().capability());
            self->read();
        });
    }
    void read() {
        const auto rx_before=ws.next_layer().next_layer().rx_reads();
        ws.async_read(buffer, [self = shared_from_this(),rx_before](Error ec, std::size_t) {
            if (self->ended) return;
            if (ec) return self->fail("read", ec);
            const auto impl = self->owner.lock();
            if (!impl || impl->stopped) return self->cancel();
            const auto start = steady_time_ns();
            const auto received_time = wall_time_us();
            const auto received_realtime=impl->config.latency_timestamps ? realtime_ns() : 0;
            const auto bytes = self->buffer.data();
            const auto sequence = impl->wire_received.fetch_add(1, std::memory_order_relaxed) + 1;
            const auto write = impl->write_index.load(std::memory_order_relaxed);
            const auto read = impl->read_index.load(std::memory_order_acquire);
            if (bytes.size() > max_message_bytes) {
                impl->oversized.fetch_add(1, std::memory_order_relaxed);
            } else if (write - read == queue_capacity) {
                impl->queue_full.fetch_add(1, std::memory_order_relaxed);
            } else {
                auto& slot = impl->queue[write % queue_capacity];
                std::memcpy(slot.payload.data(), bytes.data(), bytes.size());
                slot.size = bytes.size();
                slot.received_ns = start;
                slot.received_us = received_time;
                slot.received_realtime_ns = received_realtime;
                if(impl->config.latency_timestamps) slot.kernel_rx=self->ws.next_layer().next_layer().rx_window(rx_before);
                slot.sequence = sequence;
                slot.text = self->ws.got_text();
                slot.source = self;
                slot.published_ns = steady_time_ns();
                impl->write_index.store(write + 1, std::memory_order_release);
                const auto depth = write + 1 - read;
                if (depth > impl->high_water.load(std::memory_order_relaxed))
                    impl->high_water.store(depth, std::memory_order_relaxed);
            }
            impl->retry_seconds = 1;
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
void Receiver::Impl::process(std::size_t max_batch) {
    if (processing_stopped) return;
    for (std::size_t n = 0; n < max_batch; ++n) {
        const auto read = read_index.load(std::memory_order_relaxed);
        if (read == write_index.load(std::memory_order_acquire)) break;
        auto& slot = queue[read % queue_capacity];
        const auto parse_begin = steady_time_ns();
        const auto wait_ns = parse_begin - slot.published_ns;
        total_queue_wait_ns += static_cast<std::uint64_t>(wait_ns);
        max_queue_wait_ns = std::max(max_queue_wait_ns, static_cast<std::uint64_t>(wait_ns));
        const auto message = slot.text
            ? nlohmann::json::parse(slot.payload.data(), slot.payload.data() + slot.size, nullptr, false)
            : nlohmann::json();
        const auto parse_ns = steady_time_ns() - parse_begin;
        if (!config.tls_keylog_file.empty()) {
            unsigned char digest[SHA256_DIGEST_LENGTH];
            SHA256(reinterpret_cast<const unsigned char*>(slot.payload.data()), slot.size, digest);
            static constexpr char hex[] = "0123456789abcdef";
            std::string fingerprint(SHA256_DIGEST_LENGTH * 2, '0');
            for (std::size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
                fingerprint[i * 2] = hex[digest[i] >> 4];
                fingerprint[i * 2 + 1] = hex[digest[i] & 15];
            }
            logger.log("INFO", "capture_market_identity", {{"receive_sequence", slot.sequence},
                {"payload_sha256", fingerprint}, {"payload_bytes", slot.size}});
        }
        const auto& control = message.is_object() && message.contains("data") ? message["data"] : message;
        const bool shutdown = control.is_object() && control.contains("e") && control["e"] == "serverShutdown";
        const bool error = message.is_object() && message.contains("code");
        if (shutdown || error) {
            net::post(io, [source = slot.source, shutdown] {
                if (auto connection = source.lock())
                    connection->fail(shutdown ? "serverShutdown" : "stream_control_error", net::error::connection_reset);
            });
        } else if (!(message.is_object() && message.contains("result") && message["result"].is_null() && message.contains("id"))) {
            ingest(message, slot.size, slot.received_ns, slot.received_us, parse_ns, slot.sequence, wait_ns,
                parse_begin,slot.received_realtime_ns,slot.kernel_rx);
        }
        slot.source.reset();
        read_index.store(read + 1, std::memory_order_release);
    }
}
void Receiver::Impl::stop() {
    if (processing_stopped) return;
    if (network_thread.joinable()) {
        net::post(io, [this] {
            stopped = true;
            retry.cancel();
            if (session) session->cancel();
            session.reset();
        });
        network_thread.join();
    }
    process(queue_capacity); // Producer is joined; consume the remaining snapshots.
    processing_stopped = true;
    const auto s = orderbooks.stats();
    logger.log("INFO", "receiver_stopped", {{"received", s.received}, {"symbols", s.symbols},
        {"populated", s.populated}, {"invalid", invalid}});
    for (std::size_t index = 0; index < counts.size(); ++index)
        logger.log("INFO", "symbol_summary", {{"symbol", config.symbols[index]}, {"received", counts[index]}});
}
Receiver::Receiver(Config config, OrderBookManager& orderbooks, AsyncLogger& logger,
                   std::function<void(const ArbitrageOpportunity&)> on_opportunity)
    : impl_(std::make_shared<Impl>(std::move(config), orderbooks, logger, std::move(on_opportunity))) {}
Receiver::~Receiver() { impl_->stop(); }
void Receiver::start() {
    if (impl_->started || impl_->processing_stopped) return;
    impl_->started = true;
    impl_->connect();
    impl_->network_thread = std::thread([impl = impl_] { impl->io.run(); });
}
void Receiver::stop() { impl_->stop(); }
void Receiver::process(std::size_t max_batch) { impl_->process(max_batch); }
nlohmann::json Receiver::stats() const {
    return {{"wire_received", impl_->wire_received.load(std::memory_order_relaxed)},
        {"queue_capacity", Impl::queue_capacity},
        {"queue_depth", impl_->write_index.load(std::memory_order_acquire) - impl_->read_index.load(std::memory_order_relaxed)},
        {"queue_high_watermark", impl_->high_water.load(std::memory_order_relaxed)},
        {"queue_full_total", impl_->queue_full.load(std::memory_order_relaxed)},
        {"oversized_total", impl_->oversized.load(std::memory_order_relaxed)},
        {"total_queue_wait_ns", impl_->total_queue_wait_ns}, {"max_queue_wait_ns", impl_->max_queue_wait_ns},
        {"received", impl_->received}, {"invalid", impl_->invalid},
        {"total_ingest_ns", impl_->total_ingest_ns}, {"max_ingest_ns", impl_->max_ingest_ns},
        {"avg_ingest_ns", impl_->received == 0 ? 0.0 :
            static_cast<double>(impl_->total_ingest_ns) / static_cast<double>(impl_->received)},
        {"log_rejected", impl_->log_rejected}};
}
} // namespace triangular
