#include "triangular/execution/execution.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/post.hpp>
#include <deque>
#include <iostream>

using namespace triangular;
using namespace triangular::execution;
namespace {
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class F> void rejects(F f) { bool rejected=false; try { f(); } catch (...) { rejected=true; } check(rejected,"Expected rejection"); }
Config configuration() {
    Config c; c.commission_taker=0.001; c.max_arbitrage_usdt=100;
    c.symbols={"AB","AUSDT","BUSDT"}; c.symbol_indices={{"AB",0},{"AUSDT",1},{"BUSDT",2}};
    c.triangle_indices={{0,1,2}};
    c.trading_groups={{{0,1,2},{TradePath{{{1,true},{0,false},{2,false}}},TradePath{{{2,true},{0,true},{1,false}}}}}};
    return c;
}
std::vector<Market> markets() {
    std::vector<Market> result;
    for (const auto& pair : std::vector<std::pair<std::string,std::string>>{{"A","B"},{"A","USDT"},{"B","USDT"}}) {
        Market m{}; m.base=pair.first; m.quote=pair.second; m.symbol=pair.first+pair.second;
        m.tick=Decimal("0.01"); m.step=Decimal("0.001"); m.min_qty=Decimal("0.001");
        m.max_qty=100000; m.min_notional=Decimal("0.01"); result.push_back(m);
    }
    return result;
}
void populate(OrderBookManager& books) {
    auto put=[&](std::size_t index,const char* symbol,const char* bid,const char* ask) {
        auto b=decode_best_bid_ask({{"s",symbol},{"u",1},{"b",bid},{"a",ask},{"B","1000"},{"A","1000"}},0);
        b.received_steady_ns=steady_time_ns(); books.update(index,b);
    };
    put(0,"AB","2.00","2.01"); put(1,"AUSDT","9.99","10.00"); put(2,"BUSDT","6.00","6.01");
}
Options options() { Options o; o.max_book_age=std::chrono::seconds(5); o.execution_timeout=std::chrono::milliseconds(20); return o; }
enum class Action { Fill, Reject, Open, Unknown };
struct FakeGateway : Gateway {
    boost::asio::io_context& io; std::vector<Market> definitions=markets();
    Balances wallet{{"USDT",1000},{"BNB",5}}; std::deque<Action> actions;
    std::vector<OrderRequest> requests; unsigned queries=0,cancels=0; bool duplicate=false;
    std::function<void(std::size_t)> after_fill;
    explicit FakeGateway(boost::asio::io_context& context):io(context) {}
    Balances balances() const override { return wallet; }
    void submit(const OrderRequest& request, Callback callback) override {
        requests.push_back(request); const Action action=actions.empty()?Action::Fill:actions.front();
        if (!actions.empty()) actions.pop_front();
        OrderReport report; report.client_id=request.client_id; report.revision=1;
        if(action==Action::Reject) report.status=OrderStatus::Rejected;
        else if(action==Action::Open) report.status=OrderStatus::Open;
        else if(action==Action::Unknown) report.status=OrderStatus::Unknown;
        else {
            const auto& m=definitions.at(request.symbol_index); report.status=OrderStatus::Filled;
            report.filled_qty=Decimal(request.quantity); report.filled_quote=report.filled_qty*Decimal(request.price);
            wallet[m.base]+=request.buy?report.filled_qty:-report.filled_qty;
            wallet[m.quote]+=request.buy?-report.filled_quote:report.filled_quote;
            const auto fee_asset=request.buy?m.base:m.quote;
            const Decimal fee=(request.buy?report.filled_qty:report.filled_quote)*Decimal("0.001");
            report.commissions[fee_asset]=fee; wallet[fee_asset]-=fee;
            if(after_fill) after_fill(requests.size());
        }
        boost::asio::post(io,[callback,report,duplicate=duplicate]{ callback(report); if(duplicate) callback(report); });
    }
    void query(const OrderRequest&,Callback) override { ++queries; }
    void cancel(const OrderRequest&,Callback) override { ++cancels; }
};
struct Fixture {
    boost::asio::io_context io; Config config=configuration(); OrderBookManager books{config}; FakeGateway gateway{io};
    ArbitrageOpportunity opportunity;
    Fixture(){ populate(books); check(books.scan_edge(0,opportunity),"Fixture must be profitable"); }
    void run(){ io.run(); io.restart(); }
};
void tests() {
    check(hmac_sha256_hex("NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
          "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559")==
          "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71","HMAC vector failed");
    {
        Fixture f; f.gateway.duplicate=true; ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        check(e.try_start(f.opportunity),"First opportunity rejected"); check(!e.try_start(f.opportunity),"Overlap accepted"); f.run();
        check(e.state()==State::Idle && e.stats()["completed"]==1 && f.gateway.requests.size()==3,"Cycle/duplicate handling failed");
        check(Decimal(f.gateway.requests[1].quantity)<Decimal(f.gateway.requests[0].quantity),"Actual fill did not size next leg");
    }
    {
        Fixture f; auto o=options(); o.max_cycles=10; ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        for(int i=0;i<10;++i){ check(e.try_start(f.opportunity),"Cycle below cap rejected"); f.run(); }
        check(!e.try_start(f.opportunity) && e.stats()["accepted"]==10,"Ten-cycle cap failed");
    }
    {
        Fixture f; f.gateway.actions={Action::Reject}; ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && e.stats()["failed"]==1 && f.gateway.requests.size()==1,"Rejected leg triggered cleanup");
    }
    {
        Fixture f; f.gateway.after_fill=[&](std::size_t n){ if(n==1){ auto b=*f.books.get(2); b.bid_price=1; f.books.update(2,b); }};
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options()); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && f.gateway.requests.size()==3,"Order books were rechecked between legs");
    }
    {
        Fixture f; f.gateway.wallet["A"]=Decimal("1"); ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        e.try_start(f.opportunity); f.run();
        check(f.gateway.requests.size()==4 && f.gateway.requests.front().initial_clear &&
              f.gateway.requests.front().symbol_index==1 && !f.gateway.requests.front().buy,"Initial asset not cleared");
        check(!f.gateway.requests[1].initial_clear,"Arbitrage did not follow initial clear");
    }
    {
        Fixture f; f.gateway.actions={Action::Open}; auto o=options(); o.execution_timeout=std::chrono::milliseconds(2);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Halted && f.gateway.queries==0 && f.gateway.cancels==0,"Timeout initiated recovery requests");
    }
    {
        Fixture f; auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options()); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && f.gateway.requests.empty(),"Stale initial book accepted");
    }
    {
        Fixture f; PaperGateway gateway(f.io,f.books,markets(),Decimal(f.config.commission_taker),{{"USDT",1000}});
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),gateway,options()); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && e.stats()["completed"]==1 && gateway.balances()["USDT"]>1000,"Paper cycle failed");
    }
    {
        auto c=configuration(); auto info=nlohmann::json{{"symbols",nlohmann::json::array()}};
        for(const auto& m:markets()) info["symbols"].push_back({{"symbol",m.symbol},{"baseAsset",m.base},{"quoteAsset",m.quote},
          {"status","TRADING"},{"isSpotTradingAllowed",true},{"filters",nlohmann::json::array({
          {{"filterType","PRICE_FILTER"},{"tickSize","0.01"},{"minPrice","0"},{"maxPrice","0"}},
          {{"filterType","LOT_SIZE"},{"stepSize","0.001"},{"minQty","0.001"},{"maxQty","1000"}},
          {{"filterType","MIN_NOTIONAL"},{"minNotional","5"}}})}});
        auto loaded=load_markets(c,info); check(loaded[0].step==Decimal("0.001")&&loaded[0].min_notional==5,"Filter load failed");
        info["symbols"][0]["filters"][0]["tickSize"]="0"; rejects([&]{load_markets(c,info);});
    }
}
}
int main(){ try{ tests(); std::cout<<"Execution tests passed\n"; return 0; }
catch(const std::exception& error){ std::cerr<<error.what()<<'\n'; return 1; } }
