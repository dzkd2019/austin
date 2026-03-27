# Austin 高并发性能瓶颈排查指南

> **Reviewer**：压测时应该盯住哪些大盘指标，以及什么表现是异常的。直接给结论，不讲概念。

---

## 1. JVM 层面

### 1.1 Heap 内存使用率

| 指标路径（Prometheus）                                     | 正常表现                              | 异常信号（立刻排查）                             |
|--------------------------------------------------------|-----------------------------------|------------------------------------------|
| `jvm_memory_used_bytes{area="heap"}`                   | 锯齿波动，每次 GC 后可回落至稳定基线              | Old Gen 持续单调上涨，每次 FGC 后回落幅度越来越小 → 内存泄漏 |
| `jvm_memory_max_bytes{area="heap"}`                    | 固定值                               | 不应变化                                   |
| `jvm_memory_used_bytes{area="nonheap"}` (Metaspace)    | 缓慢增长后趋于平稳                         | 持续增长不收敛 → 类加载泄漏或动态代理过多                |

**排查动作**：

```bash
# 查看 Heap 分代占用（G1GC 场景）
jcmd <PID> GC.heap_info

# 导出 Heap Dump（OOM 前兆时执行）
jcmd <PID> GC.heap_dump /tmp/austin-heap-$(date +%s).hprof

# 用 MAT / VisualVM 打开 hprof，按 Retained Heap 排序，找最大对象
```

---

### 1.2 YGC / FGC 频率与耗时

| 指标路径（Prometheus）                                                       | 正常阈值                     | 异常信号                                |
|--------------------------------------------------------------------------|--------------------------|-------------------------------------|
| `jvm_gc_pause_seconds_count{action="end of minor GC"}`                   | 基准场景：≤ 5 次/min           | > 20 次/min → Eden 空间过小或对象晋升速率过高     |
| `jvm_gc_pause_seconds_sum{action="end of minor GC"}`                     | 平均单次 < 20ms              | 单次 YGC > 100ms → 新生代过大或 Survivor 配置异常 |
| `jvm_gc_pause_seconds_count{action="end of major GC"}`（FGC）              | Soak 测试 2 小时内 ≤ 4 次      | > 2 次/小时，或单次 STW > 1s → 立刻检查 Heap 泄漏 |
| `process_cpu_usage`                                                      | 正常 < 70%                 | GC 期间 CPU 飙至 90%+ 且持续 → GC 占用过多计算资源  |

**关键判断**：在 Grafana 上将 `jvm_gc_pause_seconds_sum` 对 `jvm_gc_pause_seconds_count` 做 `rate()` 相除，得到**平均单次 GC 耗时趋势图**。若该曲线在 Soak 测试后半段开始上翘，说明 Heap 正在恶化。

---

## 2. 虚拟线程层面（Java 21+ Virtual Threads）

> Austin 使用虚拟线程处理请求。虚拟线程本身近乎无限，但它们运行在有限的 **Carrier Thread（平台线程）** 上。瓶颈在 Carrier，不在虚拟线程数量。

### 2.1 应该看什么指标

| 观测手段                                                      | 正常表现                                         | 异常信号                                          |
|-----------------------------------------------------------|----------------------------------------------|-----------------------------------------------|
| JFR 事件：`jdk.VirtualThreadPinned`                         | 极少或 0                                        | 频繁出现 → synchronized 块/native 方法持有锁，导致 Carrier 被 pin 住无法复用 |
| JFR 事件：`jdk.VirtualThreadSubmitFailed`                   | 0                                            | 出现任意一次 → ForkJoinPool（默认 Carrier）队列已满，系统过载   |
| `jvm_threads_live_threads`（平台线程数）                        | 稳定在 CPU 核心数的 1~2 倍左右（默认 Carrier 数 = CPU 核心数） | 持续增长 → 有线程泄漏，或大量 Carrier 被 pin 导致系统自动扩展     |
| `jvm_threads_states_threads{state="BLOCKED"}`（平台线程阻塞数） | ≈ 0（虚拟线程的阻塞不应反映为平台线程 BLOCKED）               | 持续 > CPU 核数的 50% → synchronized 导致 Carrier pin，退化为传统线程模型 |

### 2.2 Carrier Thread 被 pin 住的排查步骤

```bash
# 步骤1：开启 JFR 录制（30s 采样）
jcmd <PID> JFR.start duration=30s filename=/tmp/austin-vt.jfr settings=profile

# 步骤2：分析 VirtualThreadPinned 事件
jfr print --events jdk.VirtualThreadPinned /tmp/austin-vt.jfr | head -100

# 步骤3：查找调用栈中的 synchronized 关键词
# 常见罪魁祸首：JDBC Driver、某些日志框架、老版本 JSON 库

# 步骤4：实时查看 Carrier 被 pin 的线程转储
jcmd <PID> Thread.print | grep -A 20 "VirtualThread"
```

**典型 Pin 场景**：JDBC 连接（HikariCP 在获取连接时内部使用 `synchronized`）、`HttpClient` 的某些实现、Spring Security 的 session 锁。解决方案：升级驱动到支持 virtual-thread 的版本，或将阻塞调用提交至独立的 executor。

---

## 3. 中间件层面

### 3.1 Kafka Consumer Lag（消费积压）

| Prometheus 指标                                                       | 正常表现                    | 异常信号                                          |
|---------------------------------------------------------------------|-------------------------|-----------------------------------------------|
| `kafka_consumer_group_lag{topic="austin"}`                          | 随流量波动，峰值后 5 分钟内恢复至 < 1000 条 | 断崖式堆积：Lag 在 1~2 分钟内从 0 跳升到 10w+ 且不下降 → 消费者挂了或处理逻辑死循环 |
| `kafka_consumer_group_lag` 斜率（`rate()[1m]`）                       | 正斜率短暂出现后反转              | 持续正斜率（> 500 条/min 连续 5 min）→ 生产速率远超消费速率，需扩分区或增加消费者实例 |
| `kafka_consumer_fetch_rate`                                         | 与 Lag 增长同向增长            | Lag 高但 fetch_rate 也高 → 消费慢（处理逻辑慢）；Lag 高且 fetch_rate 为 0 → 消费者掉线 |

**排查 Lag 断崖堆积**：

```bash
# 1. 查看消费组所有分区的 Lag 明细
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group austin-consumer-group \
  --describe

# 2. 查看 Topic 分区 Leader 是否均匀分布（不均匀会导致热分区）
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic austin

# 3. 查看消费者日志：是否有 CommitFailedException / Rebalancing 频繁输出
grep -E "CommitFailedException|Rebalancing|poll timeout" /var/log/austin/application.log | tail -50

# 4. 检查 max.poll.interval.ms vs 实际处理耗时
# 如果下游渠道（短信网关）响应慢 > max.poll.interval.ms（默认5分钟），
# 消费者会被 Group Coordinator 踢出，触发 Rebalance，Lag 持续堆积
```

---

### 3.2 数据库连接池（HikariCP）

| Prometheus 指标（Micrometer 暴露）                                  | 正常表现                    | 异常信号                                        |
|----------------------------------------------------------------|-------------------------|-----------------------------------------------|
| `hikaricp_connections_active`                                  | < max-pool-size × 70%   | 长期 = max-pool-size → 连接被持有不释放，或下游 DB 响应慢      |
| `hikaricp_connections_pending`（等待连接数）                        | 稳定 = 0                  | **任何非零值都是危险信号**：说明已无空闲连接，请求在排队等待，P99 将急剧劣化    |
| `hikaricp_connections_timeout_total`（获取连接超时次数）               | 0                        | > 0 → 用户请求开始因等不到连接而报错，`connectionTimeout` 配置过短或连接泄漏 |
| `hikaricp_connections_creation_seconds`（创建新连接耗时）              | 稳定低值（< 100ms）           | 突然飙高 → DB 服务器负载高，TCP 握手慢                    |

**排查连接池耗尽**：

```bash
# 1. 查看当前 Hikari 连接状态（Actuator 接口）
curl http://localhost:8080/actuator/metrics/hikaricp.connections.active
curl http://localhost:8080/actuator/metrics/hikaricp.connections.pending

# 2. 在 DB 侧查看长事务（MySQL）
SELECT * FROM information_schema.INNODB_TRX
WHERE TIME_TO_SEC(TIMEDIFF(NOW(), trx_started)) > 10
ORDER BY trx_started;

# 3. 查看当前 DB 连接数
SHOW STATUS WHERE variable_name = 'Threads_connected';
SHOW PROCESSLIST;

# 4. 检查是否有事务未提交（Spring @Transactional 在虚拟线程环境下需确认传播行为）
grep -E "TransactionSystemException|Unable to acquire JDBC Connection" /var/log/austin/application.log | tail -20
```

**连接池推荐配置（参考起点，按压测结果调整）**：

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20          # 不要无脑加大，连接越多 DB 侧锁竞争越激烈
      minimum-idle: 5
      connection-timeout: 3000       # 3s 等不到连接就报错，快速失败而非无限等待
      idle-timeout: 600000           # 空闲连接 10 分钟后回收
      max-lifetime: 1800000          # 连接最大生命周期 30 分钟，防止 DB 主动断开
      leak-detection-threshold: 5000 # 5s 检测连接泄漏（压测时建议开启）
```

---

## 4. 系统全局指标速查表

| 类别        | Grafana Panel 名称 / Prometheus 查询                                        | 报警阈值（参考）                 |
|-----------|---------------------------------------------------------------------------|----------------------------|
| CPU       | `rate(process_cpu_seconds_total[1m]) * 100`                               | > 85% 持续 3 min             |
| 内存（RSS）  | `process_resident_memory_bytes`                                           | 超过 JVM -Xmx 的 1.5 倍        |
| 网络发送      | `rate(node_network_transmit_bytes_total[1m])`                             | 接近网卡带宽上限                   |
| HTTP 错误率  | `rate(http_server_requests_seconds_count{status=~"5.."}[1m])`            | > 1% 持续 1 min              |
| HTTP P99  | `histogram_quantile(0.99, rate(http_server_requests_seconds_bucket[1m]))` | > 500ms                    |
| Kafka Lag | `kafka_consumer_group_lag`                                                | > 10000 且斜率 > 0 持续 5 min   |
| DB 等待连接   | `hikaricp_connections_pending`                                            | > 0 持续 30s                 |
| GC STW    | `rate(jvm_gc_pause_seconds_sum[1m])`                                      | > 0.1s/s（即每秒有 100ms 在 GC） |

---

## 5. 双场景 Grafana 核心监控指标（Reviewer 避坑清单）

> **Reviewer 原则**：场景不同，死亡链路不同。A 场景死在 HTTP 层，B 场景死在 Kafka + 文件 IO。

### 场景 A（实时 API 发送）：必盯 3 个指标

| 优先级 | Grafana Panel / Prometheus 查询 | 死亡阈值 | 发生场景 & 后果 |
|------|-------------------------------|--------|--------------|
| ⚠️ **P1** | **HTTP P99 延迟**<br>`histogram_quantile(0.99, rate(http_server_requests_seconds_bucket{uri="/send"}[1m]))` | > 500ms 持续 2 min | Semaphore 限流队列被撑满，请求开始排队等待而非快速拒绝。说明背压未生效，服务正在向雪崩滑落。立刻检查 Semaphore 配置和虚拟线程 Carrier pin 情况 |
| ⚠️ **P1** | **非预期 5xx 错误率**<br>`rate(http_server_requests_seconds_count{uri="/send",status=~"5[0-9][0-9]"}[1m]) / rate(http_server_requests_seconds_count{uri="/send"}[1m])` | > 1% 持续 1 min | 非 429/503 的错误出现意味着 Kafka Producer 连接断开、消息体序列化异常或 DB 不可达。需立即排查 austin-handler 日志 |
| 🔴 **P2** | **Kafka Consumer Lag 峰值增长速率**<br>`rate(kafka_consumer_group_lag{topic="austin"}[1m])` | 斜率 > 500条/min 持续 3min | Spike 阶段 Kafka 被瞬间打满，消费端追不上生产端。若 5 分钟内 Lag 不回落至 < 1000，说明 Consumer 处理能力不足，需扩分区或增加 Consumer 实例 |

**场景 A 快速判断口诀**：
- P99 正常 + 高 429 率 = 限流生效，正常现象 ✅
- P99 飙高 + 低 429 率 = 限流失效，请求在死队列里排队，危险 ❌
- P99 飙高 + 高 5xx 率 = 服务崩溃边缘，立刻停止压测并保留现场 🆘

---

### 场景 B（xxl-job 调度发送）：必盯 3 个指标

| 优先级 | Grafana Panel / Prometheus 查询 | 死亡阈值 | 发生场景 & 后果 |
|------|-------------------------------|--------|--------------|
| ⚠️ **P1** | **Kafka Consumer Lag 断崖堆积**<br>`kafka_consumer_group_lag{topic="austin"}` | 1~2 分钟内从 0 跳升至 10万+ 且不下降 | xxl-job 触发 CronTaskHandler 后，批量从 CSV 读取数十万条记录并打入 Kafka，生产速率远超消费速率。Lag 断崖式增长且 `fetch_rate` 为 0，说明消费者已离线（Rebalance 超时或 max.poll.interval 违规）。这是场景 B 最常见的崩溃路径 |
| ⚠️ **P1** | **HikariCP 连接池积压**<br>`hikaricp_connections_pending` | > 0 持续 30s | TaskHandlerImpl 在每个 Job 中都会查询 DB（MessageTemplate + 缓存 miss 时）。并发启动大量 Job 时，连接池被瞬间耗尽，后续请求开始排队。一旦 pending > 0，P99 将进入指数级恶化，最终触发 `ConnectionTimeoutException` |
| 🔴 **P2** | **xxl-job Executor 线程数**<br>`jvm_threads_live_threads`（结合 JFR 查看 `jdk.VirtualThreadPinned`） | 平台线程数持续增长 > 初始值 × 2，或 VirtualThreadPinned 事件持续出现 | CronTaskHandler 同步执行 `taskHandler.handle()`，该方法在读取大文件时会阻塞 xxl-job 的 Executor 线程。若 CSV 文件极大（百万行），同时触发多个 Job 会导致 xxl-job Executor 线程全部被占满，新 Job 无法调度，且 synchronized 块（HikariCP 等）可能 pin 住 Carrier Thread |

**场景 B 快速判断口诀**：
- Kafka Lag 线性增长后平稳 = 正常，系统在消化积压 ✅
- Kafka Lag 断崖 + `fetch_rate = 0` = Consumer 掉线，立刻检查 Rebalance 日志 ❌
- HikariCP pending > 0 = 连接池告急，减少并发 Job 数量或扩大连接池 ⚡
- xxl-job 线程持续增长 = CronTaskHandler 阻塞，考虑异步化文件读取（线程池隔离） 🔥

---

## 6. 压测快速开始 Checklist

```text
[压测前 - 通用]
□ 确认 Prometheus + Grafana 已接入 Austin 的 Micrometer 端点（/actuator/prometheus）
□ 确认 Kafka 消费组 Lag 监控面板已就绪
□ JVM 启动参数加上 GC 日志：-Xlog:gc*:file=/tmp/gc.log:time,uptime:filecount=5,filesize=20m

[压测前 - 场景 A]
□ 生成单发数据：python3 scripts/generate_payloads.py --mode api --count 3000 --output data/api_payloads.json
□ 生成批量发数据：python3 scripts/generate_payloads.py --mode api --batch --count 500 --output data/batch_payloads.json
□ k6 run k6/api_stress_test.js

[压测前 - 场景 B]
□ 生成定时任务注册数据：python3 scripts/generate_payloads.py --mode cron --count 50 --output data/cron_payloads.json
□ 确认 cronCrowdPath 指向的 CSV 文件存在且行数足够（建议 10 万行以上才能体现压力）
□ 确认 xxl-job admin 服务已启动，且 Austin 的 xxl-job executor 已注册
□ k6 run k6/job_trigger_test.js

[压测中 - 场景 A]
□ 盯住 HTTP P99（/send 接口），确认 Spike 阶段出现 429 而非 500
□ 盯住 austin_rate_limited 指标（k6 自定义），确认限流比例合理
□ 若 P99 > 500ms 且无 429，立刻暂停：背压失效，检查 Semaphore 配置

[压测中 - 场景 B]
□ 每 30s 查看 kafka_consumer_group_lag 趋势（增速不应持续上升）
□ 若 Lag 断崖堆积：立刻执行 kafka-consumer-groups.sh --describe 查看分区状态
□ 若 hikaricp_connections_pending > 0：减少并发 Job 数量，或临时扩大连接池

[压测后]
□ 导出 k6 results/ 目录下的 summary JSON，与 SLA 阈值对比
□ 分析 GC 日志：grep -E "GC\(|Pause" /tmp/gc.log | awk '{print $1, $2, $NF}' | tail -50
□ 检查 Heap Dump（如有 OOM）：jmap -heap <PID>（或已保存的 .hprof 文件）
□ 归档本次压测报告（场景、VUs、时长、P99、错误率、Kafka Lag 峰值）
```
