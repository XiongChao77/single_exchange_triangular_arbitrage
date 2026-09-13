# Latency 实验记录

本文件持续记录 latency 实验。后续实验按日期和编号追加独立章节，保留已有结论及原始结果链接；每次注明代码版本、埋点补丁、机器与构建环境、数据集、负载、重复次数、指标口径、完整性检查和局限。改变埋点、编译参数、绑核或日志策略时，应视为新的实验条件。

## 实验索引

|编号|日期（UTC）|实验|原始结果|
|---|---|---|---|
|历史基线|原结果未记录执行日期|旧机器 main / lock-free 重放比较|[旧报告](benchmarks/replay-results/report.md)、[环境](benchmarks/replay-results/environment.json)|
|LAT-20260913-01|2026-09-13|Ryzen 9 7950X 上复跑同一录制及负载矩阵|[逐轮报告](benchmarks/replay-results-20260913-7950x/report.md)、[JSON 结果](benchmarks/replay-results-20260913-7950x/results.json)|
|LAT-20260913-01 补测|2026-09-13|large burst-500 异常长尾复测|[逐轮报告](benchmarks/replay-results-20260913-7950x-burst-check/report.md)、[JSON 结果](benchmarks/replay-results-20260913-7950x-burst-check/results.json)|

## LAT-20260913-01：main / lock-free 本机重放比较

### 结果与结论

原矩阵于 2026-09-13 05:20:59–05:24:04 UTC 执行。32 轮合计发送并处理 **55,808 / 55,808** 条消息，缺失、重复、乱序、非法消息及日志丢失均为 0。lock-free 队列溢出和超长消息均为 0，最终队列深度为 0，队列最大水位为 **401 / 1024**。current / large 最终分别有 17 / 144 个更新过的订单簿；各版本、负载、重复之间的最终价格、数量、精度、档数和更新 ID 均一致。脚本也验证最终更新 ID 与录制一致，详见 [校验结果](benchmarks/replay-results-20260913-7950x/provenance/validation.json)。

**本次样本中，lock-free 在全部 16 对配对轮次中的端到端 P99 都更低，但 CPU 消耗明显更高；P50 并非全面改善。** current 50× 的 P50 从 main 的 88.8 μs 变为 lock-free 的 92.5 μs。下面保留每组两次 P99 的范围，避免掩盖波动。

所有 latency 单位为 **μs**。P50 是两次运行各自 P50 的算术平均；P99 列为两次结果的最小值–最大值，不是置信区间。未把各轮分位数平均值称为合并样本分位数。

|场景|负载|main P50 均值|lock-free P50 均值|main P99 范围|lock-free P99 范围|
|---|---|---:|---:|---:|---:|
|current|1×|611.3|149.7|830.9–858.0|190.5–228.9|
|current|10×|176.0|144.0|829.8–835.9|192.9–223.5|
|current|50×|88.8|92.5|276.5–391.8|161.5–175.9|
|current|burst-500|7,118.8|5,318.3|15,799.1–16,310.3|12,072.5–13,945.0|
|large|1×|428.7|151.6|921.2–924.8|294.4–338.4|
|large|10×|89.7|86.9|303.7–366.6|253.6–258.4|
|large|50×|88.7|84.7|392.3–775.9|296.4–309.9|
|large|burst-500|71,670.9|7,997.7|20,390.7–236,185.6|16,106.0–18,879.0|

下表每格为 **main / lock-free**，指标均为两轮各自统计值的算术平均。分位数之间不能直接相减来分解端到端延迟。

|场景|负载|接收进程 CPU 秒|receive → decision P99|sender lateness P99|
|---|---|---:|---:|---:|
|current|1×|0.125 / 16.075|107.4 / 46.8|589.5 / 73.9|
|current|10×|0.085 / 2.840|78.7 / 48.2|572.3 / 99.1|
|current|50×|0.050 / 1.845|49.5 / 56.2|124.2 / 68.1|
|current|burst-500|0.050 / 1.640|53.5 / 8,869.8|2,322.5 / 4,121.1|
|large|1×|0.480 / 16.425|283.4 / 154.0|573.8 / 83.6|
|large|10×|0.185 / 3.095|152.6 / 201.9|70.0 / 68.6|
|large|50×|0.165 / 1.890|143.8 / 250.7|89.0 / 74.7|
|large|burst-500|0.155 / 2.145|148.5 / 13,599.0|16,043.8 / 3,994.6|

- **CPU 代价稳定存在。** 原矩阵每版各运行 16 轮，main 累计 2.59 CPU 秒，lock-free 累计 91.91 CPU 秒（约 35.5 倍）。按负载分组，约为 11.5–128.6 倍；1× 时 lock-free 的忙轮询接近持续占用一个核。该差异不是更高消息吞吐的证明。
- **低频负载受发送端与整机状态影响。** current 1× 的 sender lateness P99 均值为 589.5 / 73.9 μs，large 1× 为 573.8 / 83.6 μs。接收后的 P99 也有改善，但不能将全部端到端差异归因为 lock-free 队列。忙轮询可能改变调频、休眠唤醒和调度状态，这是待验证的解释；本次未采集能证明因果关系的系统追踪。
- **突发时等待发生在不同位置。** main 在接收回调之前出现积压；lock-free 更快接收消息，随后在用户态队列等待。例如 large burst 第一次运行，send-start → receive P99 为 main 16,686.2 μs、lock-free 275.9 μs，而 receive → decision P99 为 151.1 / 14,989.2 μs。因此不能仅比较 receive → decision 就断言 main 的整体延迟更好。
- **large burst 的主实验结果含异常长尾。** main 第二次 P99 为 236,185.6 μs，第一次为 20,390.7 μs。异常轮序列 232→233、1583→1584 的相邻接收回调分别间隔约 204.3、204.4 ms；send-start → receive P99 为 221,051.7 μs，但 receive → decision P99 仅 146.0 μs。发送端 P99 迟到亦升至 28,466.1 μs。现有日志可定位到接收回调之前的等待，但不足以确定是 TCP、调度还是其他原因，不能归因于 `scan_edge`。原始异常轮保留，不据此宣称稳定获得约 86% 的 P99 改善。

### large burst 异常复测（独立追加，不替换原矩阵）

对完全相同的二进制与录制，仅复测 large / burst-500，每版四次，继续交替先后顺序，共 8 轮。结果另存于 [复测报告](benchmarks/replay-results-20260913-7950x-burst-check/report.md) 和 [JSON](benchmarks/replay-results-20260913-7950x-burst-check/results.json)，不混入上表两次重复的统计。8 轮 **23,240 / 23,240** 条消息全部处理并通过相同完整性检查，见 [校验结果](benchmarks/replay-results-20260913-7950x-burst-check/validation.json)。

|复测轮次|main scheduled → decision P99（μs）|lock-free P99（μs）|
|---|---:|---:|
|1|22,161.0|18,293.3|
|2|19,725.1|18,321.4|
|3|19,043.6|17,738.4|
|4|20,390.0|18,532.0|

四次复测未重现 200 ms 级长尾：main P99 为 19.0–22.2 ms，lock-free 为 17.7–18.5 ms。两版逐轮 P99 的平均值分别为 20.33 / 18.22 ms，lock-free 低约 **10.4%**。该结果支持此负载下较温和的延迟收益，但不能排除偶发长尾；主实验的异常样本仍属于本次结果。

### 与旧机器结果的关系及局限

旧 [environment.json](benchmarks/replay-results/environment.json) 只记录 Linux 5.15、glibc 2.35、2 个逻辑 CPU 和二进制哈希，未记录 CPU 型号、完整提交与补丁、工具链或调频配置。因此这里只做描述性对照，不计算可归因于硬件的加速比。旧实验使用相同录制及负载矩阵，也无缺失消息。

- 旧机器 current 1×：main P99 为 480.3–505.8 μs，lock-free 为 727.8–1,488.1 μs；本机则为 830.9–858.0 / 190.5–228.9 μs，优劣关系反转，且 main 的该项绝对延迟变差。更强的机器不保证每个低负载分位数都更好。
- 旧机器 large 50×：main P99 为 6,127.1–8,467.2 μs，lock-free 为 3,050.8–6,175.6 μs；本机为 392.3–775.9 / 296.4–309.9 μs。该差异同时受硬件、工具链、发送端和系统状态影响。

主要矩阵每组仅两次重复，每轮只有 583 或 2,905 条消息；复测也是有限样本，均不构成统计显著性或生产 SLO 保证。未独立预热或进行 CPU 隔离，发送端与接收端共机，逐条 JSON 日志带来额外开销。burst-500 的发送也不是瞬时完成的；请求倍率和计划突发速率不能当作已验证的持续吞吐能力。

### 原始产物

主实验与复测目录均保留 `results.json`、`environment.json`、每轮 `metrics.json`、`sender.jsonl`、接收日志、配置及退出输出。主目录的 `provenance/` 另存提交/数据/脚本哈希、计时补丁、脚本快照、编译参数、CMake 缓存和构建记录。编译产物在忽略目录 `build-latency-20260913/`；复现时应使用记录的提交和补丁重新构建，而不是依赖未来的分支 HEAD。

### 方法与可复现条件

- main：`2c5b390`；lock-free：`abf9dce`。从本地分支提交用 `git archive` 导出独立源码，均只追加同一逻辑的 `TRIANGULAR_REPLAY=1` 计时埋点。当前工作区已有修改未纳入构建；其中已有的 receiver 埋点作为两份补丁的来源。提交本身不含该埋点，不能仅用提交号代表实验源码。完整提交号、文件 SHA-256 和两份补丁保存在 [provenance](benchmarks/replay-results-20260913-7950x/provenance/)。
- 本机：AMD Ryzen 9 7950X，16 核 / 32 线程，约 93 GiB RAM；Ubuntu 24.04.4，Linux 6.8.0-138-generic，CPU governor 为 `powersave`。未额外绑核、隔离 CPU 或修改调频策略，操作系统正常调度。
- 构建：GCC 13.3.0、CMake 4.4.3，`CMAKE_BUILD_TYPE=Release`（`-O3 -DNDEBUG`），C++20；使用 `.deps/usr/include` 下的 Boost 1.74 / nlohmann JSON 3.10.5，以及系统 OpenSSL 3.0.13。重放脚本使用 Python 3.12.13。CMake 临时安装于 `/tmp/latency-cmake`，未修改项目依赖配置。
- 数据：复用 [原始录制](benchmarks/recording/manifest.json)，录制时长设定为 15 秒，保留行情内容及消息顺序，不重新访问交易所采样。current 为 10 个三角组合、21 个订阅交易对、583 条消息，首尾间隔 14.919548 秒；large 为 116 个组合、180 个订阅交易对、2,905 条消息，首尾间隔 14.951320 秒。订阅交易对数不等于录制中实际有更新的交易对数。
- 矩阵：两种规模 ×（1×、10×、50×、burst-500）× 两次重复 × 两个版本，共 32 轮。每轮启动新进程，无单独预热；每组第一轮 main → lock-free，第二轮 lock-free → main，所有接收程序顺序运行。burst-500 每 100 ms 最多发送 500 条。
- 本地 TLS REST / WebSocket 服务重放，`execution_mode=disabled`，不下单。Python `monotonic_ns()` 和 C++ `steady_clock` 在本机计时；旧数据只复用相对到达间隔，每轮重新生成绝对计划时间。

主要指标 **scheduled → decision** 从计划发送时间量到该消息的 `scan_edge` 完成，包含发送端调度迟到、发送背压、传输和接收端处理。**receive → decision** 从 WebSocket 完整消息回调开始计时，lock-free 版本还包含消息队列等待；它不是纯计算耗时。**sender lateness** 是实际开始发送减去计划发送时间。计时终点不包括后续机会处理、订单完成或该条计时日志写盘；日志开销仍会影响后续消息。

CPU 秒来自接收进程 `/proc/<pid>/stat`，含各线程累计 CPU 时间，覆盖启动、重放和等待日志排空，采样在 SIGINT 前；不含 Python 发送端，也不是 CPU 百分比。实验测量有限录制的处理表现，不代表生产交易所到决策延迟或最大可持续吞吐量。

### 复跑命令

在仓库根目录运行，输出目录必须尚不存在。下例使用本次隔离构建的二进制；以后应选择新的输出目录及实验编号。

```bash
python3 tools/replay_benchmark.py benchmark \
  --dataset benchmarks/recording \
  --output benchmarks/replay-results-20260913-7950x \
  --main "$PWD/build-latency-20260913/main/build-release/json_receiver" \
  --lock-free "$PWD/build-latency-20260913/lock-free/build-release/json_receiver" \
  --speeds 1 10 50 --bursts 500 --repeats 2
```

重建时，分别导出上述提交，在各自目录应用 `provenance/main-replay.patch`、`provenance/lock-free-replay.patch`，再运行（将源码目录替换为对应版本）：

```bash
cmake -S <源码目录> -B <源码目录>/build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBOOST_INCLUDE_DIR="$PWD/.deps/usr/include" \
  -DJSON_INCLUDE_DIR="$PWD/.deps/usr/include"
cmake --build <源码目录>/build-release --target json_receiver -j 4
```

首次构建默认 `all` 目标时，main 的原有 `core_tests` 因引用不存在的 `decode_best_bid_ask` 编译失败，见 [构建记录](benchmarks/replay-results-20260913-7950x/provenance/main-build.log)。随后两个版本的 `json_receiver` Release 目标均成功构建。未修改历史测试，也不声称整个测试套件通过。

补测通过同一脚本的 Python 入口指定空 `speeds` 列表，仅运行突发负载（原 CLI 的 `--speeds` 要求至少一个值）：

```python
import argparse, runpy
from pathlib import Path
root = Path.cwd()
runner = runpy.run_path(str(root / "tools/replay_benchmark.py"))
runner["benchmark"](argparse.Namespace(
    dataset=root / "benchmarks/recording",
    output=root / "benchmarks/replay-results-20260913-7950x-burst-check",
    main=root / "build-latency-20260913/main/build-release/json_receiver",
    lock_free=root / "build-latency-20260913/lock-free/build-release/json_receiver",
    scenarios=["large"], speeds=[], bursts=[500], repeats=4,
))
```

原矩阵校验可运行 `python3 benchmarks/replay-results-20260913-7950x/provenance/validate_results.py`；补测校验在其后追加 `--burst-check benchmarks/replay-results-20260913-7950x-burst-check`。
