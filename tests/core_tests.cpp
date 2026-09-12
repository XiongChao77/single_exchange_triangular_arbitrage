#include "triangular/orderbook.hpp"

#include <atomic>
#include <cmath>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace triangular;
namespace {
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class F> void rejects(F f) {
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    check(rejected, "Expected rejection");
}
nlohmann::json fixture() {
    return {{"stream", "ethbtc@bookTicker"}, {"data", {
        {"u", 9007199254740993LL}, {"s", "ETHBTC"},
        {"b", "0.04200001"}, {"B", "2.00000"}, {"a", "0.04200002"}, {"A", "3.00000"}}}};
}
}

int main() {
    try {
        const auto message = fixture();
        const auto orderbook = decode_best_bid_ask(message, 12345);
        check(orderbook.symbol == "ETHBTC" && !orderbook.event_time_us && orderbook.received_time_us == 12345, "Timestamp/symbol mismatch");
        check(orderbook.book_update_id == 9007199254740993LL, "Integer precision lost");
        check(orderbook.price_exponent == -8 && orderbook.qty_exponent == -5, "Signed exponent mismatch");
        check(orderbook.bid_price == 4200001 && orderbook.ask_price == 4200002 && orderbook.bid_qty == 200000 && orderbook.ask_qty == 300000, "Order book mismatch");
        check(decode_best_bid_ask(message.at("data"), 0).bid_price == orderbook.bid_price, "Raw payload failed");
        for (const auto* field : {"u", "s", "b", "B", "a", "A"}) {
            auto bad = message;
            bad["data"].erase(field);
            rejects([&] { decode_best_bid_ask(bad, 0); });
        }
        for (const nlohmann::json bad_value : {nlohmann::json(nullptr), nlohmann::json(1.2), nlohmann::json(""),
             nlohmann::json("-1"), nlohmann::json("NaN"), nlohmann::json("1e-8"), nlohmann::json("1.2.3"),
             nlohmann::json(".1"), nlohmann::json("1."), nlohmann::json("9223372036854775808"),
             nlohmann::json("0." + std::string(128, '0'))}) {
            auto bad = message;
            bad["data"]["b"] = bad_value;
            rejects([&] { decode_best_bid_ask(bad, 0); });
        }
        for (const nlohmann::json bad_id : {nlohmann::json(-1), nlohmann::json(1.5), nlohmann::json("1"),
             nlohmann::json(9223372036854775808ULL), nlohmann::json(true)}) {
            auto bad = message;
            bad["data"]["u"] = bad_id;
            rejects([&] { decode_best_bid_ask(bad, 0); });
        }
        auto changed = message;
        changed["data"]["b"] = "1.2";
        changed["data"]["a"] = "1.234";
        changed["data"]["B"] = "0";
        changed["data"]["A"] = "2.50";
        const auto mixed = decode_best_bid_ask(changed, 0);
        check(mixed.bid_price == 1200 && mixed.ask_price == 1234 && mixed.price_exponent == -3 &&
              mixed.bid_qty == 0 && mixed.ask_qty == 250 && mixed.qty_exponent == -2, "Decimal scale alignment failed");
        changed["data"]["b"] = "9223372036854775807";
        rejects([&] { decode_best_bid_ask(changed, 0); });
        changed["data"]["a"] = "0";
        check(decode_best_bid_ask(changed, 0).bid_price == 9223372036854775807LL, "Max mantissa rejected");
        changed["data"]["b"] = "9";
        changed["data"]["a"] = "0.000000000000000001";
        const auto scaled = decode_best_bid_ask(changed, 0);
        check(scaled.bid_price == 9000000000000000000LL && scaled.ask_price == 1 &&
              scaled.price_exponent == -18, "Maximum scale factor failed");
        changed["data"]["b"] = "10";
        rejects([&] { decode_best_bid_ask(changed, 0); });
        changed["data"]["b"] = "1";
        changed["data"]["a"] = "0.0000000000000000001";
        rejects([&] { decode_best_bid_ask(changed, 0); });
        changed["data"]["b"] = "0";
        changed["data"]["a"] = "0." + std::string(126, '0') + "1";
        const auto tiny = decode_best_bid_ask(changed, 0);
        check(tiny.bid_price == 0 && tiny.ask_price == 1 && tiny.price_exponent == -127,
              "Zero rescaling failed");
        rejects([] { decode_best_bid_ask(nullptr, 0); });
        rejects([] { decode_best_bid_ask(nlohmann::json::array(), 0); });

        nlohmann::json partial={{"stream","ethbtc@depth20@100ms"},{"data",{
            {"lastUpdateId",160},{"bids",nlohmann::json::array()},{"asks",nlohmann::json::array()}}}};
        for(int level=0;level<20;++level) {
            partial["data"]["bids"].push_back({"0.04200001",std::to_string(20-level)+".00000"});
            partial["data"]["asks"].push_back({"0.04200002",std::to_string(21+level)+".00000"});
        }
        const auto depth=decode_partial_depth(partial,54321);
        check(depth.symbol=="ETHBTC" && depth.book_update_id==160 && depth.received_time_us==54321,
              "Partial depth metadata failed");
        check(depth.bid_levels==20 && depth.ask_levels==20 && depth.bid_price==4200001 && depth.ask_price==4200002,
              "Partial depth top or level count failed");
        check(depth.bids[19].qty==100000 && depth.asks[19].qty==4000000,
              "Partial depth levels were not retained");

        const auto config_path = std::filesystem::temp_directory_path() / "triangular-json-config-test.json";
        auto load = [&](const nlohmann::json& value) {
            { std::ofstream file(config_path); file << value; }
            return load_config(config_path);
        };
        const auto settings = nlohmann::json::parse(R"({"triangles":[["ethbtc","ETHUSDT","BTCUSDT"],["BTCUSDT","BNBBTC","BNBUSDT"]]})");
        auto config = load(settings);
        check(config.host == "stream.binance.com" && config.port == "9443", "Defaults failed");
        check(config.execution_timeout_ms == 1000, "Default execution timeout failed");
        check(config.symbols == std::vector<std::string>{"ETHBTC","ETHUSDT","BTCUSDT","BNBBTC","BNBUSDT"}, "Unique symbols failed");
        check(config.triangle_indices == std::vector<std::array<std::size_t,3>>{{0,1,2},{2,3,4}}, "Triangle indices failed");
        auto mode = settings; mode["execution_mode"] = "paper";
        check(load(mode).execution_mode == "paper", "Paper mode failed");
        mode["execution_mode"] = "live";
        rejects([&] { load(mode); });
        mode["api_key_file"] = "api.key";
        mode["secret_key_file"] = "secret.key";
        mode["live_test_mode"] = true;
        const auto live = load(mode);
        check(live.execution_mode == "live" && live.live_test_mode &&
              live.api_key_file == config_path.parent_path() / "api.key" &&
              live.secret_key_file == config_path.parent_path() / "secret.key", "Live settings failed");
        mode["execution_mode"] = "invalid";
        rejects([&] { load(mode); });
        auto alternate = settings; alternate["port"] = "443";
        check(load(alternate).port == "443", "Alternative port failed");
        auto timeout = settings; timeout["execution_timeout_ms"] = 25;
        check(load(timeout).execution_timeout_ms == 25, "Execution timeout setting ignored");
        timeout["execution_timeout_ms"] = 0; rejects([&] { load(timeout); });
        timeout["execution_timeout_ms"] = 60001; rejects([&] { load(timeout); });
        for (const auto* bad : {R"({})", R"({"triangles":[]})", R"({"triangles":[["ETHBTC"]]})",
             R"({"triangles":[["ETHBTC","ethbtc","BTCUSDT"]]})", R"({"triangles":[[1,"ETHBTC","BTCUSDT"]]})"})
            rejects([&] { load(nlohmann::json::parse(bad)); });
        std::filesystem::remove(config_path);
        nlohmann::json metadata = {{"symbols", nlohmann::json::array()}};
        const std::vector<std::pair<std::string,std::string>> assets{{"ETH","BTC"},{"ETH","USDT"},{"BTC","USDT"},{"BNB","BTC"},{"BNB","USDT"}};
        for (std::size_t i = 0; i < assets.size(); ++i)
            metadata["symbols"].push_back({{"symbol", config.symbols[i]}, {"baseAsset", assets[i].first},
                {"quoteAsset", assets[i].second}, {"status","TRADING"}, {"isSpotTradingAllowed",true}});
        config.trading_groups = validate_arbitrage(config, metadata);
        auto check_usdt_paths = [&](const std::vector<TradeGroup>& groups,
                                    const std::vector<std::pair<std::string,std::string>>& market_assets) {
            check(groups.size() == 2, "Expected two triangle groups");
            for (const auto& group : groups) {
                check(group.paths.size() == 2, "Expected two paths per group");
                for (const auto& path : group.paths) {
                    std::string holding = "USDT";
                    for (const auto& leg : path) {
                        check(std::find(group.symbol_indices.begin(), group.symbol_indices.end(), leg.index) !=
                              group.symbol_indices.end(), "Path leg must belong to group");
                        const auto& [base, quote] = market_assets.at(leg.index);
                        check(holding == (leg.buy ? quote : base), "Leg spends an asset not held");
                        holding = leg.buy ? base : quote;
                    }
                    check(holding == "USDT", "Path must return to USDT");
                }
                check(group.paths[0][0].index != group.paths[1][0].index,
                      "Paths must use different first conversions");
            }
        };
        check_usdt_paths(config.trading_groups, assets);
        for (std::size_t group = 0; group < config.trading_groups.size(); ++group)
            check(config.trading_groups[group].symbol_indices == config.triangle_indices[group],
                  "Group symbols must match configured triangle");
        auto reordered = config;
        reordered.triangle_indices = {{2, 0, 1}, {4, 3, 2}};
        check_usdt_paths(validate_arbitrage(reordered, metadata), assets);
        auto unfunded = metadata;
        for (auto& market : unfunded["symbols"])
            if (market["quoteAsset"] == "USDT") market["quoteAsset"] = "USDC";
        rejects([&] { validate_arbitrage(config, unfunded); });
        auto reversed_usdt = metadata;
        reversed_usdt["symbols"][1]["baseAsset"] = "USDT";
        reversed_usdt["symbols"][1]["quoteAsset"] = "ETH";
        const auto reversed_groups = validate_arbitrage(config, reversed_usdt);
        auto reversed_assets = assets;
        reversed_assets[1] = {"USDT", "ETH"};
        check_usdt_paths(reversed_groups, reversed_assets);
        check(std::any_of(reversed_groups[0].paths.begin(), reversed_groups[0].paths.end(),
              [&](const TradePath& path) { return path.front().index == 1 && !path.front().buy; }),
              "USDT-base market must start with SELL");
        auto priced = [](const char* symbol, const char* bid, const char* ask) {
            return decode_best_bid_ask({{"s",symbol},{"u",1},{"b",bid},{"a",ask},{"B","10"},{"A","10"}}, 0);
        };
        ArbitrageOpportunity opportunity;
        auto reversed_config = config;
        reversed_config.commission_taker = 0;
        reversed_config.edge_threshold = 0;
        reversed_config.trading_groups = reversed_groups;
        OrderBookManager reversed_edges(reversed_config);
        reversed_edges.update(0, priced("ETHBTC", "2", "2.01"));
        reversed_edges.update(1, priced("ETHUSDT", "2", "2.01"));
        reversed_edges.update(2, priced("BTCUSDT", "2", "2.01"));
        check(reversed_edges.scan_edge(1, opportunity) && opportunity.path.front().index == 1 &&
              !opportunity.path.front().buy, "USDT-base SELL path was not scanned");
        OrderBookManager edges(config);
        check(!edges.scan_edge(0, opportunity), "Missing books must not signal");
        rejects([&] { edges.scan_edge(99, opportunity); });
        edges.update(0, priced("ETHBTC", "0.0499", "0.0500"));
        edges.update(1, priced("ETHUSDT", "3010", "3011"));
        check(!edges.scan_edge(0, opportunity), "Incomplete triangle must not signal");
        edges.update(2, priced("BTCUSDT", "59999", "60000"));
        check(edges.scan_edge(0, opportunity) && edges.scan_edge(2, opportunity), "Profitable ask-buy/bid-sell path missed");
        check(opportunity.input_usdt == 100.0L && opportunity.group_index == 0,
              "Initial investment must be capped at 100 USDT");
        auto near = [](long double a, long double b) { return std::abs(a - b) < 1e-10L; };
        const long double fee = 1.0L - config.commission_taker;
        check(opportunity.path[0].index == 2 && opportunity.path[0].buy &&
              opportunity.path[1].index == 0 && opportunity.path[1].buy &&
              opportunity.path[2].index == 1 && !opportunity.path[2].buy, "Wrong returned path");
        check(near(opportunity.prices[0], 60000) && near(opportunity.prices[1], 0.05L) &&
              near(opportunity.prices[2], 3010), "Incorrect decimal prices");
        check(near(opportunity.quantities[0], 100.0L / 60000) &&
              near(opportunity.quantities[1], opportunity.quantities[0] * fee / 0.05L) &&
              near(opportunity.quantities[2], opportunity.quantities[1] * fee), "Fee-aware leg sizes incorrect");
        check(near(opportunity.output_usdt, opportunity.quantities[2] * 3010 * fee) &&
              near(opportunity.profit_usdt, opportunity.output_usdt - 100), "Incorrect net proceeds");
        for (std::size_t limited_leg = 0; limited_leg < 3; ++limited_leg) {
            const auto full = opportunity;
            OrderBookManager limited(config);
            for (std::size_t i = 0; i < 3; ++i) limited.update(i, *edges.get(i));
            auto book = *edges.get(full.path[limited_leg].index);
            book.qty_exponent = -6;
            if (full.path[limited_leg].buy) book.ask_qty = 100; else book.bid_qty = 100;
            limited.update(full.path[limited_leg].index, book);
            ArbitrageOpportunity smaller;
            check(limited.scan_edge(0, smaller), "Limited liquidity should reduce trade size");
            check(smaller.input_usdt < 100 && near(smaller.quantities[limited_leg], 0.0001L),
                  "Every leg must constrain initial USDT by its base liquidity");
        }
        check(!edges.scan_edge(3, opportunity), "Unrelated triangle must not signal");
        check(opportunity.input_usdt == 0 && opportunity.quantities[0] == 0,
              "Failed scan must clear previous opportunity");
        edges.update(1, priced("ETHUSDT", "3004", "3005"));
        check(!edges.scan_edge(0, opportunity), "Three taker fees must remove gross profit");
        edges.update(1, priced("ETHUSDT", "2980", "2981"));
        check(edges.scan_edge(1, opportunity), "Reverse direction missed");
        auto empty = priced("ETHUSDT", "2980", "2981"); empty.ask_qty = 0;
        edges.update(1, empty);
        check(!edges.scan_edge(1, opportunity), "Empty ask must not signal");
        empty.ask_qty = 10; empty.ask_price = 0; edges.update(1, empty);
        check(!edges.scan_edge(1, opportunity), "Zero ask must not signal");
        edges.update(3, priced("BNBBTC", "0.0099", "0.0100"));
        edges.update(4, priced("BNBUSDT", "603", "604"));
        check(edges.scan_edge(3, opportunity) && edges.scan_edge(2, opportunity), "Second triangle missed");
        check(!edges.scan_edge(0, opportunity), "Scan must only evaluate affected paths");
        auto boundary = config; boundary.commission_taker = 0; boundary.edge_threshold = 0.125;
        OrderBookManager exact(boundary);
        exact.update(0, priced("ETHBTC", "1", "1"));
        exact.update(1, priced("ETHUSDT", "9", "9"));
        exact.update(2, priced("BTCUSDT", "8", "8"));
        check(!exact.scan_edge(0, opportunity), "Threshold equality must not signal");
        boundary.edge_threshold = 0.124;
        OrderBookManager above(boundary);
        for (std::size_t i = 0; i < 3; ++i) above.update(i, *exact.get(i));
        check(above.scan_edge(0, opportunity), "Return above threshold missed");
        auto fees = settings; fees["commission_taker"] = 0.002; fees["edge_threshold"] = 0.003;
        check(load(fees).commission_taker == 0.002 && load(fees).edge_threshold == 0.003, "Fee settings ignored");
        for (double invalid_fee : {-0.1, 1.0, 2.0}) {
            fees["commission_taker"] = invalid_fee;
            rejects([&] { load(fees); });
        }
        fees["commission_taker"] = 0.0; fees["edge_threshold"] = -0.1;
        rejects([&] { load(fees); });
        auto budget = settings; budget["max_arbitrage_usdt"] = 25;
        auto capped_config = config;
        capped_config.max_arbitrage_usdt = load(budget).max_arbitrage_usdt;
        OrderBookManager capped(capped_config);
        for (std::size_t i = 0; i < 3; ++i) capped.update(i, *above.get(i));
        check(capped.scan_edge(0, opportunity) && opportunity.input_usdt == 25, "Custom investment cap ignored");
        for (double invalid : {0.0, -1.0}) {
            budget["max_arbitrage_usdt"] = invalid;
            rejects([&] { load(budget); });
            capped_config.max_arbitrage_usdt = invalid;
            rejects([&] { OrderBookManager rejected(capped_config); });
        }
        std::filesystem::remove(config_path);
        auto bad_meta = metadata; bad_meta["symbols"][0]["baseAsset"] = "SOL";
        rejects([&] { validate_arbitrage(config,bad_meta); });
        bad_meta = metadata; bad_meta["symbols"].erase(0);
        rejects([&] { validate_arbitrage(config,bad_meta); });
        bad_meta = metadata; bad_meta["symbols"][0]["status"] = "BREAK";
        rejects([&] { validate_arbitrage(config,bad_meta); });
        bad_meta = metadata; bad_meta["symbols"][0]["isSpotTradingAllowed"] = false;
        rejects([&] { validate_arbitrage(config,bad_meta); });
        OrderBookManager orderbooks(config);
        check(!orderbooks.get(0) && orderbooks.stats().populated == 0, "Uninitialized orderbook present");
        check(orderbooks.index_of("BTCUSDT") == 2, "Order book index mismatch");
        rejects([&] { orderbooks.index_of("UNKNOWN"); });
        for (std::int64_t id = 1; id <= 10000; ++id) {
            auto update = orderbook; update.book_update_id = id; orderbooks.update(0, std::move(update));
        }
        check(orderbooks.get(0)->book_update_id == 10000 && !orderbooks.get(1) && orderbooks.stats().received == 10000 &&
              orderbooks.stats().symbols == 5 && orderbooks.stats().populated == 1, "Latest orderbook replacement failed");
        OrderBookManager concurrent(config);
        std::atomic<bool> done = false;
        std::thread producer([&] {
            for (std::int64_t id = 0; id < 10000; ++id) {
                auto update = orderbook; update.book_update_id = id; update.bid_price = id; concurrent.update(0, std::move(update));
            }
            done = true;
        });
        bool valid = true;
        while (!done) {
            const auto view = concurrent.snapshot();
            if (view[0]) valid = valid && view[0]->book_update_id == view[0]->bid_price;
        }
        producer.join();
        check(valid && concurrent.get(0)->book_update_id == 9999, "Concurrent orderbook read failed");
        check(stream_target({"ETHBTC", "ethbtc", "BTCUSDT"}) ==
              "/stream?streams=ethbtc@depth20@100ms/ethbtc@depth20@100ms/btcusdt@depth20@100ms", "Stream URL failed");
        std::cout << "All orderbook tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
