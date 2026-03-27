/**
 * Austin 高并发消息平台 — k6 核心压测脚本
 * Dev-QA 产出物
 *
 * 运行前准备:
 *   1. 安装 k6: https://k6.io/docs/get-started/installation/
 *   2. 生成测试数据:
 *        python3 scripts/generate_payloads.py --count 2000 --output data/payloads.json
 *   3. 按需修改下方 CONFIG 区域（BASE_URL、AUTH_TOKEN 等）
 *
 * 运行命令:
 *   # 基准测试
 *   k6 run --env SCENARIO=baseline k6/austin_stress_test.js
 *
 *   # 突发流量测试
 *   k6 run --env SCENARIO=spike k6/austin_stress_test.js
 *
 *   # 疲劳测试（2 小时，建议后台运行）
 *   k6 run --env SCENARIO=soak k6/austin_stress_test.js
 *
 *   # 带 Prometheus 远程写入的运行（需要 xk6-prometheus-rw 扩展）
 *   k6 run --out experimental-prometheus-rw k6/austin_stress_test.js
 */

import http from "docs/k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "docs/k6/metrics";
import { SharedArray } from "docs/k6/data";

// ──────────────────────────────────────────────────────────────
// CONFIG：按实际环境修改
// ──────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const SEND_ENDPOINT = `${BASE_URL}/send`;
const BATCH_SEND_ENDPOINT = `${BASE_URL}/batchSend`;

// 如果接口有鉴权，填入 Token；否则留空字符串
const AUTH_TOKEN = __ENV.AUTH_TOKEN || "";

// 数据文件路径（相对于脚本执行目录）
const PAYLOAD_FILE = __ENV.PAYLOAD_FILE || "data/payloads.json";

// ──────────────────────────────────────────────────────────────
// 流量模型：通过 SCENARIO 环境变量切换
// ──────────────────────────────────────────────────────────────
const SCENARIO = __ENV.SCENARIO || "baseline";

const SCENARIOS_CONFIG = {
  /**
   * 基准测试：预热 2 min + 稳态采样 3 min，100 VUs
   * SLA: P99 ≤ 200ms，错误率 ≤ 0.1%，RPS ≥ 500
   */
  baseline: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "1m", target: 50 },   // 爬坡至 50 VUs（预热阶段一）
      { duration: "1m", target: 100 },  // 爬坡至 100 VUs（预热阶段二）
      { duration: "3m", target: 100 },  // 稳态持续 3 min（正式采样）
      { duration: "30s", target: 0 },   // 收尾降载
    ],
    gracefulRampDown: "30s",
  },

  /**
   * 突发流量测试：瞬间 10× 流量（100 → 1000 VUs），验证限流/背压
   * SLA: 系统不宕机，限流响应 ≤ 50ms，正常请求 P99 ≤ 500ms
   */
  spike: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "1m", target: 100 },  // 正常水位
      { duration: "10s", target: 1000 }, // 瞬间 10× Spike
      { duration: "2m", target: 1000 }, // 保持高压
      { duration: "10s", target: 100 }, // 回落
      { duration: "1m", target: 100 },  // 观察恢复情况
      { duration: "30s", target: 0 },   // 收尾
    ],
    gracefulRampDown: "30s",
  },

  /**
   * 疲劳测试：350 VUs 持续 2 小时，暴露内存泄漏/连接泄漏
   * SLA: P99 漂移 ≤ 20%，全程错误率 ≤ 0.5%，无持续单调 Heap 增长
   */
  soak: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "5m", target: 350 },    // 5 min 爬坡
      { duration: "115m", target: 350 },  // 稳态持续 115 min
      { duration: "5m", target: 0 },      // 收尾降载
    ],
    gracefulRampDown: "60s",
  },
};

// ──────────────────────────────────────────────────────────────
// SLA 阈值：k6 内置 thresholds，不满足时 exit code 非零
// ──────────────────────────────────────────────────────────────
const THRESHOLDS_BY_SCENARIO = {
  baseline: {
    http_req_duration: ["p(99)<200", "p(95)<100", "avg<50"],
    http_req_failed: ["rate<0.001"],
    austin_send_errors: ["count<10"],
  },
  spike: {
    http_req_duration: ["p(99)<500"],
    // 允许较高错误率（包含预期的 429/503 限流响应）
    http_req_failed: ["rate<0.5"],
    austin_send_errors: ["count<50"],
  },
  soak: {
    http_req_duration: ["p(99)<300", "avg<100"],
    http_req_failed: ["rate<0.005"],
    austin_send_errors: ["count<100"],
  },
};

// ──────────────────────────────────────────────────────────────
// 自定义指标
// ──────────────────────────────────────────────────────────────
const sendErrors = new Counter("austin_send_errors");
const rateLimitedRate = new Rate("austin_rate_limited");
const sendLatency = new Trend("austin_send_latency_ms", true);

// ──────────────────────────────────────────────────────────────
// 测试数据：SharedArray 在所有 VU 间共享，避免内存爆炸
// ──────────────────────────────────────────────────────────────
const payloads = new SharedArray("payloads", function () {
  // k6 在初始化阶段加载此文件，路径相对于 k6 run 命令执行目录
  return JSON.parse(open(PAYLOAD_FILE));
});

// ──────────────────────────────────────────────────────────────
// k6 options：由 SCENARIO 环境变量决定使用哪套配置
// ──────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    austin_load: SCENARIOS_CONFIG[SCENARIO] || SCENARIOS_CONFIG.baseline,
  },
  thresholds: THRESHOLDS_BY_SCENARIO[SCENARIO] || THRESHOLDS_BY_SCENARIO.baseline,

  // 统计摘要输出（可替换为 Prometheus remote-write）
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// ──────────────────────────────────────────────────────────────
// 请求头构造
// ──────────────────────────────────────────────────────────────
function buildHeaders() {
  const headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    // 用于链路追踪，便于在 Grafana / Jaeger 里关联请求
    "X-Request-ID": `k6-${__VU}-${__ITER}-${Date.now()}`,
  };
  if (AUTH_TOKEN) {
    headers["Authorization"] = `Bearer ${AUTH_TOKEN}`;
  }
  return headers;
}

// ──────────────────────────────────────────────────────────────
// 核心虚拟用户逻辑（每次迭代执行一次）
// ──────────────────────────────────────────────────────────────
export default function () {
  // 从共享数据集中随机取一条 Payload（避免所有 VU 发同一条导致缓存命中率异常高）
  const payload = payloads[Math.floor(Math.random() * payloads.length)];
  const body = JSON.stringify(payload);
  const headers = buildHeaders();

  const startTs = Date.now();
  const res = http.post(SEND_ENDPOINT, body, {
    headers,
    timeout: "10s", // 单次请求超时：10s，超时直接计入错误
    tags: {
      scenario: SCENARIO,
      template_id: String(payload.messageTemplateId),
    },
  });
  const elapsed = Date.now() - startTs;

  sendLatency.add(elapsed);

  // ── 断言区域 ──
  const isSuccess = check(res, {
    // 正常发送场景：200 或 202（异步入队成功）
    "status is 200 or 202": (r) =>
      r.status === 200 || r.status === 202,

    // 限流/背压场景：429 或 503 是预期内的拒绝，不算服务端错误
    "no unexpected 5xx": (r) =>
      r.status !== 500 && r.status !== 504,

    // 响应体非空
    "response body not empty": (r) => r.body && r.body.length > 0,
  });

  // 统计非预期错误（排除 429/503 限流）
  if (
    res.status !== 200 &&
    res.status !== 202 &&
    res.status !== 429 &&
    res.status !== 503
  ) {
    sendErrors.add(1);
  }

  // 统计被限流的请求比例（用于验证背压机制是否生效）
  rateLimitedRate.add(res.status === 429 || res.status === 503 ? 1 : 0);

  // 模拟真实用户行为间隔：0.1~0.5s 随机思考时间，避免纯粹的 closed-loop 压测
  sleep(Math.random() * 0.4 + 0.1);
}

// ──────────────────────────────────────────────────────────────
// 测试结束后的自定义摘要（输出到控制台，方便 CI 解析）
// ──────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const metrics = data.metrics;

  const summary = {
    scenario: SCENARIO,
    timestamp: new Date().toISOString(),
    rps: metrics.http_reqs
      ? (metrics.http_reqs.values.rate || 0).toFixed(2)
      : "N/A",
    p99_ms: metrics.http_req_duration
      ? (metrics.http_req_duration.values["p(99)"] || 0).toFixed(2)
      : "N/A",
    p95_ms: metrics.http_req_duration
      ? (metrics.http_req_duration.values["p(95)"] || 0).toFixed(2)
      : "N/A",
    avg_ms: metrics.http_req_duration
      ? (metrics.http_req_duration.values.avg || 0).toFixed(2)
      : "N/A",
    error_rate_pct: metrics.http_req_failed
      ? (metrics.http_req_failed.values.rate * 100 || 0).toFixed(3)
      : "N/A",
    rate_limited_pct: metrics.austin_rate_limited
      ? (metrics.austin_rate_limited.values.rate * 100 || 0).toFixed(3)
      : "N/A",
    unexpected_errors: metrics.austin_send_errors
      ? metrics.austin_send_errors.values.count || 0
      : 0,
  };

  console.log("\n══════════ Austin 压测结果摘要 ══════════");
  console.log(JSON.stringify(summary, null, 2));
  console.log("═════════════════════════════════════════\n");

  // 同时输出标准 k6 摘要到 stdout
  return {
    stdout: "\n",  // 保留 k6 自带的终端输出
    "results/summary.json": JSON.stringify(summary, null, 2),
  };
}
