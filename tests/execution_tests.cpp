#include "triangular/execution/execution.hpp"
#include "triangular/logger.hpp"
#include <boost/asio/post.hpp>
#include <deque>
#include <cmath>
#include <iostream>
#include <fstream>

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
        auto b=decode_partial_depth({{"stream",std::string(symbol)+"@depth20@100ms"},{"data",{{"lastUpdateId",1},{"bids",nlohmann::json::array({nlohmann::json::array({bid,"1000"})})},{"asks",nlohmann::json::array({nlohmann::json::array({ask,"1000"})})}}}},0);
        b.received_steady_ns=steady_time_ns(); books.update(index,b);
    };
    put(0,"AB","2.00","2.01"); put(1,"AUSDT","9.99","10.00"); put(2,"BUSDT","6.00","6.01");
}
Options options() { Options o; o.max_book_age=std::chrono::seconds(5); o.execution_timeout=std::chrono::milliseconds(20); return o; }
enum class Action { Fill, Reject, Open, Unknown };
struct FakeGateway : Gateway {
    boost::asio::io_context& io; std::vector<Market> definitions=markets();
    Balances wallet{{"USDT",1000},{"BNB",5}}; std::deque<Action> actions;
    std::vector<OrderRequest> requests; unsigned queries=0,cancels=0; bool duplicate=false, throw_submit=false;
    std::function<void(std::size_t)> after_fill;
    bool defer = false;
    std::function<void()> deferred;
    nlohmann::json failure = nlohmann::json::object();
    explicit FakeGateway(boost::asio::io_context& context):io(context) {}
    Balances balances() const override { return wallet; }
    void submit(const OrderRequest& request, Callback callback) override {
        if (throw_submit) throw std::runtime_error("Synthetic submit failure");
        requests.push_back(request); const Action action=actions.empty()?Action::Fill:actions.front();
        if (!actions.empty()) actions.pop_front();
        OrderReport report; report.client_id=request.client_id; report.revision=1; report.failure=failure;
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
        auto deliver=[callback,report,duplicate=duplicate]{ callback(report); if(duplicate) callback(report); };
        if (defer) deferred=std::move(deliver);
        else boost::asio::post(io,std::move(deliver));
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
void timeout_tests() {
    for (auto action : {Action::Fill, Action::Reject, Action::Unknown, Action::Open}) {
        Fixture f; f.gateway.defer=true; f.gateway.actions={action};
        auto o=options(); o.execution_timeout=std::chrono::milliseconds(2);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        check(e.try_start(f.opportunity),"Initial cycle rejected"); f.run();
        check(e.state()==State::LegPending && e.stats()["waiting_gateway"]==true &&
              !e.try_start(f.opportunity),"Timeout released a busy gateway");
        auto deliver=std::move(f.gateway.deferred); deliver();
        check(e.state()==State::Idle && e.stats()["gateway_busy"]==false &&
              e.stats()["waiting_gateway"]==false && e.stats()["failed"]==1 &&
              f.gateway.requests.size()==1,"Late response advanced cycle or counted failure twice");
        deliver();
        check(e.stats()["failed"]==1,"Duplicate late response changed failure count");
        check(e.try_start(f.opportunity),"Gateway completion did not release executor");
    }
}
void tests() {
    {
        const auto path=std::filesystem::temp_directory_path()/("cycle-limit-"+std::to_string(steady_time_ns())+".json");
        nlohmann::json config={{"triangles",{{"AB","AUSDT","BUSDT"}}},{"live_test_mode",true}};
        auto write=[&]{std::ofstream output(path); output << config;};
        write(); check(load_config(path).max_cycles==2,"Missing limit changed live test default");
        for(unsigned limit : {0U,1U,7U}) {
            config["max_cycles"]=limit; write();
            check(load_config(path).max_cycles==limit,"Configured cycle limit ignored");
        }
        for(const nlohmann::json& invalid : {nlohmann::json(-1),nlohmann::json(1.5),nlohmann::json("2"),nlohmann::json(true)}) {
            config["max_cycles"]=invalid; write(); rejects([&]{load_config(path);});
        }
        std::filesystem::remove(path);
    }
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
        e.start_initial_cleanup(); f.run();
        check(e.stats()["submitted_cycles"]==0 && e.stats()["startup_orders_submitted"]==1, "Startup consumed cycle cap");
        e.try_start(f.opportunity); f.run();
        check(f.gateway.requests.size()==4 && f.gateway.requests.front().initial_clear &&
              f.gateway.requests.front().symbol_index==1 && !f.gateway.requests.front().buy,"Initial asset not cleared");
        check(!f.gateway.requests[1].initial_clear,"Arbitrage did not follow initial clear");
    }
    {
        Fixture f; f.gateway.actions={Action::Open}; auto o=options(); o.execution_timeout=std::chrono::milliseconds(2);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && e.stats()["stop_reason"]=="cycle_failed" &&
              f.gateway.queries==0 && f.gateway.cancels==0 && e.try_start(f.opportunity),
              "Timeout ended the cycle and blocked the next cycle");
    }
    {
        Fixture f; auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options()); e.try_start(f.opportunity); f.run();
        check(e.state()==State::Idle && f.gateway.requests.empty(),"Stale initial book accepted");
    }
    {
        Fixture f; auto o=options(); o.max_cycles=1;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        f.gateway.wallet["USDT"]=0;
        for(int i=0;i<3;++i) { check(e.try_start(f.opportunity),"Unsubmitted attempts consumed cap"); f.run(); }
        check(e.stats()["submitted_cycles"]==0 && e.stats()["rejection"]["code"]=="insufficient_usdt",
              "Balance rejection diagnostic/counter missing");
        f.gateway.wallet["USDT"]=1000;
        check(e.try_start(f.opportunity),"Funded retry rejected"); f.run();
        check(e.stats()["submitted_cycles"]==1 && f.gateway.requests.size()==3,"Cycle counted more than once");
        check(!e.try_start(f.opportunity),"Submitted cycle did not consume cap");
    }
    {
        Fixture f; auto o=options(); o.max_cycles=1;
        auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        e.try_start(f.opportunity); f.run();
        auto d=e.stats()["rejection"];
        check(d["code"]=="stale_orderbook" && d["symbol"]=="AUSDT" && d["leg"]==0 &&
              d.contains("book_age_ns"),"Stale rejection lacks details");
        populate(f.books);
        check(e.try_start(f.opportunity),"Stale preflight consumed cap"); f.run();
        check(e.stats()["completed"]==1 && e.stats()["rejection"].empty(),"Fresh retry failed or retained rejection");
    }
    {
        Fixture f; auto m=markets(); m[1].min_notional=1000;
        ArbitrageExecutor e(f.io,f.config,f.books,m,f.gateway,options()); e.try_start(f.opportunity); f.run();
        auto d=e.stats()["rejection"];
        check(d["code"]=="notional_below_min" && d.contains("notional") && d["min_notional"]=="1000",
              "Market filter rejection lacks values");
        check(f.gateway.requests.empty(),"Invalid notional submitted");
    }
    {
        Fixture f; auto b=*f.books.get(1); b.ask_qty=0; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options()); e.try_start(f.opportunity); f.run();
        check(e.stats()["rejection"]["code"]=="insufficient_top_level_liquidity", "Liquidity diagnostic missing");
    }
    {
        Fixture f; auto candidate=f.opportunity; candidate.prices[0]=9;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options()); e.try_start(candidate); f.run();
        check(e.stats()["rejection"]["code"]=="price_worsened", "Price diagnostic missing");
    }
    for (const double threshold : {0.0, 0.1, 0.3}) {
        Fixture f; auto c=f.config; c.edge_threshold=threshold;
        OrderBookManager books(c); populate(books);
        ArbitrageOpportunity candidate;
        const bool found=books.scan_edge(0,candidate);
        check(found==(threshold<0.19), "Scanner ignored theoretical return threshold");
        if(found) {
            const long double expected=1.2L*std::pow(1.0L-c.commission_taker,3)-1.0L;
            check(std::abs(candidate.net_return-expected)<1e-15L,
                  "Scanner return is not the three-leg theoretical spread");
        } else check(candidate.input_usdt==0, "Rejected scan retained opportunity");
    }
    {
        Fixture f; auto m=markets(); m[0].step=Decimal("7");
        ArbitrageExecutor e(f.io,f.config,f.books,m,f.gateway,options());
        ArbitrageOpportunity candidate;
        check(f.books.scan_edge(0,candidate) && e.try_start(candidate), "Step rounding affected theoretical scan");
        f.run();
        check(e.stats()["completed"]==1 && f.gateway.requests.size()==3 &&
              f.gateway.wallet["USDT"]<1000 && e.stats()["rejection"].empty(),
              "Executor reapplied cash-return profitability check");
    }
    {
        Fixture f; auto c=f.config; c.commission_taker=0; c.edge_threshold=5;
        OrderBookManager books(c); populate(books);
        auto first=*books.get(1); first.ask_price=1; first.bid_price=1; first.price_exponent=0; books.update(1,first);
        auto middle=*books.get(0); middle.bid_price=2; middle.ask_price=3; middle.price_exponent=0; books.update(0,middle);
        auto last=*books.get(2); last.bid_price=3; last.ask_price=4; last.price_exponent=0; books.update(2,last);
        ArbitrageOpportunity candidate;
        check(!books.scan_edge(0,candidate), "Return equal to threshold was accepted");
    }
    {
        Fixture f; f.gateway.actions={Action::Reject}; auto o=options(); o.max_cycles=1;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o); e.try_start(f.opportunity); f.run();
        check(e.stats()["submitted_cycles"]==1 && !e.try_start(f.opportunity),"Rejected submitted order did not consume cap");
    }
    {
        Fixture f; f.gateway.wallet["USDT"]=0; f.gateway.wallet["A"]=Decimal("0.0005");
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        e.start_initial_cleanup(); f.run();
        for(int i=0;i<5;++i) {
            check(e.try_start(f.opportunity),"Dust retry rejected"); f.run();
            check(e.stats()["dust"]["A"]=="0.0005", "Same dust balance accumulated across retries");
        }
        f.gateway.wallet["A"]=Decimal("0.0007");
        e.try_start(f.opportunity); f.run();
        check(e.stats()["dust"]["A"]=="0.0005", "Startup dust snapshot changed during arbitrage");
        f.gateway.wallet["A"]=0;
        e.try_start(f.opportunity); f.run();
        e.start_initial_cleanup(); f.run();
        check(e.stats()["dust"]["A"]=="0.0005", "Cleanup ran more than once");
        check(f.gateway.requests.empty(), "Dust-only retries sent orders");
    }
    {
        Fixture f; f.gateway.wallet["A"]=Decimal("1.0005"); f.gateway.duplicate=true;
        std::vector<std::pair<std::string,nlohmann::json>> events;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view name,const nlohmann::json& fields){ if(name.starts_with("arbitrage_first_leg_")) events.emplace_back(name,fields); });
        e.start_initial_cleanup(); f.run();
        e.try_start(f.opportunity); f.run();
        check(events.size()==3 && events[0].first=="arbitrage_first_leg_submit_attempt" &&
              events[1].first=="arbitrage_first_leg_submitted" && events[2].first=="arbitrage_first_leg_report",
              "First-leg events duplicated or missing");
        const auto& first=f.gateway.requests[1];
        for(const auto& event:events) {
            check(event.second["client_id"]==first.client_id && event.second["symbol"]=="AUSDT" &&
                  event.second["side"]=="BUY" && event.second["leg"]==0 &&
                  event.second["time_in_force"]=="FOK", "First-leg event confused clearing/later leg");
        }
        check(events[2].second["order_status"]=="FILLED" &&
              events[2].second["filled_qty"]==first.quantity, "First-leg fill details missing");
        check(e.stats()["startup_cleanup_results"]["A"]["remaining_balance"]=="0.0005", "Clearing remainder incorrect");
    }
    {
        Fixture f; f.gateway.throw_submit=true;
        std::vector<std::string> events;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view name,const nlohmann::json&){ events.emplace_back(name); });
        e.try_start(f.opportunity); f.run();
        check(events==std::vector<std::string>{"arbitrage_first_leg_submit_attempt","arbitrage_first_leg_submit_failed"},
              "Throwing submit logged success");
        check(e.state()==State::Idle && e.stats()["stop_reason"]=="cycle_failed" &&
              e.stats()["submitted_cycles"]==0 && e.try_start(f.opportunity),
              "Failed submit consumed cap or blocked the next cycle");
    }
    {
        Fixture f; auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        unsigned events=0;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view,const nlohmann::json&){ ++events; });
        e.try_start(f.opportunity); f.run();
        check(events==0,"Preflight failure logged first-leg submission");
    }
    for (const auto action : {Action::Reject,Action::Unknown}) {
        Fixture f; f.gateway.actions={action}; std::vector<nlohmann::json> reports;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view name,const nlohmann::json& fields){
                if(name=="arbitrage_first_leg_report") reports.push_back(fields);
            });
        e.try_start(f.opportunity); f.run();
        check(reports.size()==1 && reports[0]["order_status"]==(action==Action::Reject?"REJECTED":"UNKNOWN"),
              "First-leg rejection/unknown report missing");
    }
    for (const auto action : {Action::Reject, Action::Unknown}) {
        Fixture f; f.gateway.actions={action}; f.gateway.duplicate=true;
        f.gateway.failure={{"stage","exchange_response"},{"http_status",400},
            {"binance_code",-1013},{"binance_message","Filter failure: LOT_SIZE"},
            {"outcome_uncertain",action==Action::Unknown}};
        std::vector<std::pair<std::string,nlohmann::json>> events;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view name,const nlohmann::json& fields){events.emplace_back(name,fields);});
        e.try_start(f.opportunity); f.run();
        unsigned errors=0, reports=0;
        for(const auto& [name,fields]:events) {
            if(name=="execution_order_failed" || name=="arbitrage_first_leg_report") {
                check(fields["failure"]==f.gateway.failure,"Failure diagnostics lost in event");
                if(name=="execution_order_failed") ++errors; else ++reports;
            }
        }
        check(errors==1 && reports==1 && e.stats()["order_failure"]==f.gateway.failure,
              "Missing or duplicated failure diagnostics");
        if(action==Action::Reject) {
            f.gateway.failure=nlohmann::json::object();
            e.try_start(f.opportunity); f.run();
            check(e.stats()["order_failure"].empty(),"New cycle retained previous order failure");
        }
    }
    {
        Fixture f; auto m=markets(); m[1].base="BNB"; m[1].symbol="BNBUSDT";
        auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,m,f.gateway,options());
        e.start_initial_cleanup(); f.run();
        check(e.state()==State::Idle && e.stats()["startup_cleanup_done"]==true &&
              e.stats()["startup_cleanup_results"]["BNB"]["code"]=="protected_asset" &&
              f.gateway.requests.empty() && f.gateway.wallet["BNB"]==5, "Startup sold BNB or waited for its book");
    }
    {
        Fixture f; f.gateway.wallet["A"]=1; auto o=options(); o.max_cycles=2;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        e.start_initial_cleanup(); f.run();
        for(int i=0;i<2;++i) {
            f.gateway.wallet["A"]+=1;
            e.start_initial_cleanup();
            check(e.try_start(f.opportunity), "Startup consumed arbitrage allowance"); f.run();
        }
        check(f.gateway.requests.size()==7 && e.stats()["startup_orders_submitted"]==1 &&
              e.stats()["submitted_cycles"]==2 && !e.try_start(f.opportunity), "Later cycles repeated startup cleanup");
        for(std::size_t i=1;i<f.gateway.requests.size();++i)
            check(!f.gateway.requests[i].initial_clear, "Later cycle swept deposited balance");
    }
    {
        Fixture f; f.gateway.wallet["A"]=1; auto o=options(); o.initial_cleanup_wait=std::chrono::milliseconds(1);
        auto b=*f.books.get(1); b.received_steady_ns=1; f.books.update(1,b);
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,o);
        e.start_initial_cleanup();
        check(!e.try_start(f.opportunity), "Arbitrage overlapped startup cleanup"); f.run();
        check(e.state()==State::Idle && e.stats()["startup_cleanup_done"]==true &&
              e.stats()["startup_cleanup_results"]["A"]["code"]=="stale_orderbook" &&
              f.gateway.requests.empty(), "Stale startup book halted execution");
        populate(f.books); e.try_start(f.opportunity); f.run();
        check(f.gateway.requests.size()==3 && !f.gateway.requests[0].initial_clear, "Skipped startup asset retried later");
    }
    {
        Fixture f; f.gateway.wallet["A"]=1; f.gateway.actions={Action::Reject};
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        e.start_initial_cleanup(); f.run();
        check(e.state()==State::Idle && f.gateway.requests.size()==1 &&
              e.stats()["startup_cleanup_results"]["A"]["code"]=="not_filled", "Rejected startup order repeated or halted");
        e.try_start(f.opportunity); f.run();
        check(f.gateway.requests.size()==4 && !f.gateway.requests[1].initial_clear, "Rejected cleanup retried in cycle");
    }
    {
        Fixture f; f.gateway.wallet["A"]=1; f.gateway.actions={Action::Unknown};
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options());
        e.start_initial_cleanup(); f.run();
        check(e.state()==State::Halted && f.gateway.requests.size()==1 && !e.try_start(f.opportunity),
              "Unknown startup order allowed further trading");
    }
    {
        Fixture f; f.gateway.wallet["BTTC"]=Decimal("0.9"); f.gateway.wallet["A"]=Decimal("0.0005");
        unsigned states=0, finished=0, skipped_bttc=0;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),
            [&](const nlohmann::json& fields) {
                ++states;
                check(!fields.contains("startup_cleanup_results") && !fields.contains("dust"),
                      "Execution state repeated startup details");
            }, [&](std::string_view name,const nlohmann::json& fields) {
                if(name=="startup_cleanup_finished") {
                    ++finished;
                    check(fields["results"]["BTTC"]["balance"]=="0.9" && fields["dust"]["A"]=="0.0005",
                          "Startup summary lost cleanup details");
                }
                if(name=="startup_cleanup_skipped" && fields["asset"]=="BTTC") ++skipped_bttc;
            });
        e.start_initial_cleanup(); f.run();
        for(int i=0;i<2;++i) { e.try_start(f.opportunity); f.run(); }
        e.start_initial_cleanup(); f.run();
        check(states>5 && finished==1 && skipped_bttc==1 && e.stats()["startup_cleanup_results"].contains("BTTC"),
              "Startup details repeated or final summary lost");
    }
    {
        Fixture f; f.config.latency_timestamps=true;
        f.opportunity.trigger_receive_sequence=123;
        f.opportunity.receive_sequences={121,122,123};
        f.opportunity.market_received_ns=steady_time_ns();
        f.opportunity.market_processed_ns=steady_time_ns();
        f.opportunity.edge_found_ns=steady_time_ns();
        std::vector<nlohmann::json> reports;
        ArbitrageExecutor e(f.io,f.config,f.books,markets(),f.gateway,options(),{},
            [&](std::string_view name,const nlohmann::json& fields) {
                if(name=="execution_order_report") reports.push_back(fields);
            });
        e.try_start(f.opportunity);f.run();
        check(f.gateway.requests.size()==3 && reports.size()==3,"All-leg latency reports missing");
        for(std::size_t i=0;i<3;++i) {
            const auto& trace=f.gateway.requests[i].latency;
            check(trace["trigger_receive_sequence"]==123 && trace["book_receive_sequences"]==nlohmann::json({121,122,123}) &&
                  trace["leg"]==i && trace["gateway_submit_ns"].get<std::int64_t>()>=trace["order_prepared_ns"].get<std::int64_t>(),
                  "Order lost trigger correlation or latency markers");
            if(i) check(trace["previous_report_ns"]==reports[i-1]["report_received_ns"],"Next leg lost report correlation");
        }
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
int main(int argc,char** argv){ try{ timeout_tests(); if(argc<2 || std::string(argv[1])!="--timeout-only") tests(); std::cout<<"Execution tests passed\n"; return 0; }
catch(const std::exception& error){ std::cerr<<error.what()<<'\n'; return 1; } }
