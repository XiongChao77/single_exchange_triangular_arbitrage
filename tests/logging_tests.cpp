#include "triangular/logger.hpp"

#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>

using namespace triangular;
using namespace std::chrono_literals;
namespace {
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class F> void until(F predicate) {
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= end) throw std::runtime_error("Timed out");
        std::this_thread::sleep_for(1ms);
    }
}
std::vector<nlohmann::json> rows(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::vector<nlohmann::json> result;
    std::string line;
    while (std::getline(input, line)) result.push_back(nlohmann::json::parse(line));
    return result;
}
}
int main() {
    const auto root = std::filesystem::temp_directory_path() / ("tri-log-test-" + std::to_string(steady_time_ns()));
    try {
        {
            AsyncLogger log(root / "batch.jsonl", {8192, 64, 10s});
            std::thread a([&] { for (int i = 0; i < 2000; ++i) log.log("INFO", "a", {{"id", i}}); });
            std::thread b([&] { for (int i = 0; i < 2000; ++i) log.log("INFO", "b", {{"id", i}}); });
            a.join(); b.join();
            until([&] { return log.stats().batches > 0; }); // Threshold flush before shutdown.
            log.stop();
            const auto s = log.stats();
            check(s.written == 4000 && s.dropped == 0 && !s.failed, "Concurrent logs lost");
            std::set<std::pair<std::string, int>> ids;
            std::uint64_t sequence = 0;
            bool threshold = false;
            for (const auto& r : rows(log.path())) {
                if (r.contains("log_sequence")) {
                    check(r["log_sequence"] == ++sequence, "Log order corrupted");
                    ids.emplace(r["event"].get<std::string>(), r["fields"]["id"].get<int>());
                } else if (r["event"] == "logger_batch") threshold |= r["fields"]["trigger"] == "threshold";
            }
            check(ids.size() == 4000 && threshold, "Batch records missing");
        }
        {
            AsyncLogger log(root / "interval.jsonl", {64, 32, 20ms});
            log.log("INFO", "interval", {{"text", "embedded\nnewline"}});
            until([&] { return log.stats().written == 1; });
            log.stop();
            check(rows(log.path())[0]["fields"]["text"] == "embedded\nnewline", "JSON escaping failed");
        }
        {
            AsyncLogger log(root / "shutdown.jsonl", {64, 32, 1h});
            log.log("INFO", "last");
            log.stop();
            log.stop();
            check(log.stats().written == 1 && log.stats().queued == 0, "Shutdown did not drain");
        }
        {
            AsyncLogger log(root / "overflow.jsonl", {1, 1, 1h});
            for (int i = 0; i < 10000; ++i) log.log("INFO", "burst");
            log.stop();
            const auto s = log.stats();
            check(s.submitted == 10000 && s.accepted + s.dropped == s.submitted, "Overflow accounting failed");
            check(s.accepted == s.written && s.high_water <= 1 && !s.failed, "Overflow drain failed");
        }
        if (std::filesystem::exists("/dev/full")) {
            AsyncLogger log("/dev/full", {64, 1, 20ms});
            log.log("INFO", "disk_error");
            until([&] { return log.stats().failed; });
            check(!log.log("INFO", "after_error"), "Failed writer accepted new logs");
            log.stop();
            const auto s = log.stats();
            check(s.accepted == s.written + s.write_lost && s.dropped == 1, "Disk failure accounting failed");
        }
        std::filesystem::remove_all(root);
        std::cout << "Logger batching, timeout, overflow, and failure tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::filesystem::remove_all(root);
        std::cerr << e.what() << '\n';
        return 1;
    }
}
