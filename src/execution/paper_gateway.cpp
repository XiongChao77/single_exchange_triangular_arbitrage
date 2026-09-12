#include "triangular/execution/execution.hpp"
#include <boost/asio/post.hpp>
#include <algorithm>

namespace triangular::execution {
struct PaperGateway::Impl : std::enable_shared_from_this<Impl> {
    boost::asio::io_context& io;
    OrderBookManager& books;
    std::vector<Market> markets;
    Decimal fee;
    Balances wallet;
    std::map<std::string, OrderReport> orders;
    struct Liquidity { std::int64_t update_id = -1; Decimal used = 0; };
    std::map<std::pair<std::size_t, bool>, Liquidity> consumed;
    Impl(boost::asio::io_context& context, OrderBookManager& b, std::vector<Market> m, Decimal f, Balances balances)
        : io(context), books(b), markets(std::move(m)), fee(std::move(f)), wallet(std::move(balances)) {
        if (!boost::math::isfinite(fee) || fee < 0 || fee >= 1) throw std::runtime_error("Invalid paper fee");
        for (const auto& [asset, amount] : wallet)
            if (asset.empty() || !boost::math::isfinite(amount) || amount < 0) throw std::runtime_error("Invalid paper balance");
    }
    OrderReport fill(const OrderRequest& request) {
        // Idempotent within this simulated session, including already-filled orders.
        if (orders.contains(request.client_id)) return orders.at(request.client_id);
        OrderReport r; r.client_id = request.client_id; r.revision = 1; r.status = OrderStatus::Rejected;
        try {
            const auto& m = markets.at(request.symbol_index);
            const Decimal quantity(request.quantity), limit(request.price);
            if (quantity <= 0 || limit <= 0 || !boost::math::isfinite(quantity) || !boost::math::isfinite(limit) ||
                floor(quantity / m.step)*m.step != quantity || floor(limit / m.tick)*m.tick != limit ||
                quantity < m.min_qty || (m.max_qty > 0 && quantity > m.max_qty) ||
                (m.min_price > 0 && limit < m.min_price) || (m.max_price > 0 && limit > m.max_price) ||
                quantity*limit < m.min_notional || (m.max_notional > 0 && quantity*limit > m.max_notional))
                return orders[request.client_id] = r;
            const auto book = books.get(request.symbol_index);
            if (!book) return orders[request.client_id] = r;
            const Decimal price = Decimal(request.buy ? book->ask_price : book->bid_price) * pow(Decimal(10), int(book->price_exponent));
            auto& usage = consumed[{request.symbol_index, request.buy}];
            if (usage.update_id != book->book_update_id) { usage.update_id = book->book_update_id; usage.used = 0; }
            const Decimal available = Decimal(request.buy ? book->ask_qty : book->bid_qty) * pow(Decimal(10), int(book->qty_exponent)) - usage.used;
            if (price <= 0 || available <= 0 || (request.buy ? price > limit : price < limit) ||
                (!request.initial_clear && available < quantity)) {
                r.status = OrderStatus::Expired; return orders[request.client_id] = r;
            }
            const Decimal base = floor(std::min(quantity, available) / m.step) * m.step;
            const Decimal quote = base * price;
            const auto& spend = request.buy ? m.quote : m.base;
            const auto& receive = request.buy ? m.base : m.quote;
            const Decimal cost = request.buy ? quote : base;
            const Decimal proceeds = request.buy ? base : quote;
            if (base <= 0 || wallet[spend] < cost) return orders[request.client_id] = r;
            wallet[spend] -= cost; wallet[receive] += proceeds * (Decimal(1)-fee);
            usage.used += base;
            r.status = base == quantity ? OrderStatus::Filled : OrderStatus::Expired;
            r.filled_qty = base; r.filled_quote = quote; r.commissions[receive] = proceeds * fee;
        } catch (...) { r.status = OrderStatus::Rejected; }
        return orders[request.client_id] = r;
    }
};
PaperGateway::PaperGateway(boost::asio::io_context& io, OrderBookManager& books, std::vector<Market> markets,
    Decimal fee, Balances balances) : impl_(std::make_shared<Impl>(io, books, std::move(markets), std::move(fee), std::move(balances))) {}
PaperGateway::~PaperGateway() = default;
Balances PaperGateway::balances() const { return impl_->wallet; }
void PaperGateway::submit(const OrderRequest& request, Callback callback) {
    boost::asio::post(impl_->io, [self = impl_, request, callback = std::move(callback)] { callback(self->fill(request)); });
}
void PaperGateway::query(const OrderRequest& request, Callback callback) {
    boost::asio::post(impl_->io, [self = impl_, request, callback = std::move(callback)] {
        const auto it = self->orders.find(request.client_id);
        if (it != self->orders.end()) callback(it->second);
        else { OrderReport r; r.client_id = request.client_id; callback(r); }
    });
}
void PaperGateway::cancel(const OrderRequest& request, Callback callback) { query(request, std::move(callback)); }
} // namespace triangular::execution
