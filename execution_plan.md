# 三角套利 execution 模块

`OrderBookManager` 按 group 扫描两条以 USDT 开始和结束的 path，`Receiver` 把机会交给全局唯一的
`ArbitrageExecutor`。执行器工作期间直接拒绝其他 group 的机会，因此配置可以共享 `BTCUSDT` 等
symbol，同时只会执行一组三角套利。

## 执行流程

一轮被接受后，执行器只做一次开始前检查：

1. 从 Gateway 的余额缓存确认 USDT 足够。
2. 检查该 group 涉及的非 USDT asset。若存在余额，先通过 group 内对应的 `asset/USDT`
   最优 bid 发送 LIMIT/IOC 卖单；低于交易过滤器的余额记为 dust。
3. 一次性读取三本 order book，检查时效、一档流动性、扫描价格没有变差、交易过滤器以及取整后利润。
4. 顺序发送三条 LIMIT/FOK 主动单。

第一条腿发出后不再读取 order book、不重新计算 edge，也不查询账户余额。后续订单价格使用
`ArbitrageOpportunity::prices` 中已接受的价格，数量则根据上一条订单实际成交数量和实际手续费计算。
这样保留成交驱动的数量控制，同时去掉腿间行情和账户检查造成的延迟。

## 状态和失败

状态主路径为：

    IDLE → VALIDATING → LEG_PENDING（第 1/2/3 腿）→ IDLE
                  ↘ CLEARING_INITIAL_POSITION ↗

同一时刻只允许一轮执行。某条腿被拒绝、过期、取消或没有完全成交时，本轮记为失败并返回
`IDLE`，不会发送异常清仓订单。订单结果为 Unknown 或整轮超过 `execution_timeout_ms` 时进入
`HALTED`，阻止新执行；当前版本不查单、不撤单、不恢复，也不记录订单意图。

`execution_timeout_ms` 配置的是从接受机会开始，到已有资产清理和三条套利腿全部完成的总时间。
当前配置为 1000ms，加载时允许范围为 1 到 60000ms。

## Gateway

`PaperGateway` 使用最优档模拟成交和钱包。`BinanceGateway` 在独立 worker 线程执行签名 HTTPS，
再把回调投递到主 `io_context`。构造 live gateway 时调用一次 `/api/v3/account` 建立余额缓存；
每个 `newOrderRespType=FULL` 回报的累计成交与手续费直接更新缓存，因此三条腿之间没有额外账户请求。

普通套利订单使用 LIMIT/FOK，开始前清理已有资产使用 LIMIT/IOC。Gateway 接口仍保留 query/cancel，
方便以后扩展，但当前低延迟执行路径不调用它们。

`live_test_mode=true` 时每个进程最多接受 10 轮套利。失败和开始前检查失败也计入额度。配置中的
密钥文件只在 live 模式读取，内容不会写入日志。

## 当前范围

当前实现面向正常成交路径，没有订单意图日志、跨进程恢复、用户数据 WebSocket、异常仓位自动清理、
时钟偏移校准或完整限流调度。默认配置仍为 paper 模式，不会发送真实订单。
