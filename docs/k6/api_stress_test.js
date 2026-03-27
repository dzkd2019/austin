/**
 * Austin 场景 A — 实时 API 发送 阶梯式压测脚本
 * Dev-QA 产出物
 *
 * 适用链路：外部系统直接调用 POST /send 或 POST /batchSend 进行消息下发。
 *
 * 核心压测目标：
 *   1. 用 stages 实现 10 → 100 → 1000 VUs 阶梯爬坡，验证系统 Semaphore 限流/背压机制：
 *      - 限流触发时应快速返回 429/503（≤ 50ms），而不是无限排队直到超时崩溃。
 *   2. 正常流量下 P99 ≤ 200ms，错误率 ≤ 0.1%。
 *   3. 10× 峰值流量下服务不宕机，限流比例可观测。
 *
 * 运行前准备:
 *   1. 生成测试数据（单发）:
 *        python3 scripts/generate_payloads.py --mode api --count 3000 --output data/api_payloads.json
 *   2. 生成批量发测试数据:
 *        python3 scripts/generate_payloads.py --mode api --batch --count 500 --output data/batch_payloads.json
 *   3. 按需修改 CONFIG 区域（BASE_URL、AUTH_TOKEN）
 *
 * 运行命令:
 *   # 完整阶梯压测（10 → 100 → 1000 VUs）
 *   k6 run k6/api_stress_test.js
 *
 *   # 仅压单发接口
 *   k6 run --env API_MODE=single k6/api_stress_test.js
 *
 *   # 仅压批量发接口
 *   k6 run --env API_MODE=batch k6/api_stress_test.js
 *
 *   # 配合 Prometheus 远程写入（需要 xk6-prometheus-rw 扩展）
 *   k6 run --out experimental-prometheus-rw k6/api_stress_test.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

// ──────────────────────────────────────────────────────────────
// CONFIG：按实际环境修改
// ──────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const SEND_ENDPOINT = `${BASE_URL}/send`;
const BATCH_SEND_ENDPOINT = `${BASE_URL}/batchSend`;

// 如果接口有鉴权，填入 Token；否则留空字符串
const AUTH_TOKEN = __ENV.AUTH_TOKEN || "";

// 压测模式：single=单发(/send)，batch=批量发(/batchSend)，mixed=混合（7:3）
const API_MODE = __ENV.API_MODE || "mixed";

// 数据文件路径（相对于 k6 run 命令执行目录）
const SINGLE_PAYLOAD_FILE = __ENV.SINGLE_PAYLOAD_FILE || "data/api_payloads.json";
const BATCH_PAYLOAD_FILE = __ENV.BATCH_PAYLOAD_FILE || "data/batch_payloads.json";

// ──────────────────────────────────────────────────────────────
// 阶梯流量模型：10 → 100 → 1000 VUs，验证限流/背压机制
// ──────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    api_ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        // 第一阶梯：热身，10 VUs，确认基础功能正常
        { duration: "1m", target: 10 },
        { duration: "1m", target: 10 },

        // 第二阶梯：正常水位，100 VUs，采集基准 P99/RPS
        { duration: "1m", target: 100 },
        { duration: "2m", target: 100 },

        // 第三阶梯：高压，500 VUs，验证背压是否开始生效
        { duration: "30s", target: 500 },
        { duration: "2m", target: 500 },

        // 第四阶梯：极限 Spike，1000 VUs，验证限流不崩溃
        { duration: "30s", target: 1000 },
        { duration: "2m", target: 1000 },

        // 收尾：快速回落，观察恢复情况
        { duration: "30s", target: 100 },
        { duration: "1m", target: 100 },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "30s",
    },
  },

  // ── SLA 阈值（不满足则 k6 以非零 exit code 退出，可接入 CI 门禁）──
  thresholds: {
    // 全量 P99 ≤ 500ms（含限流阶段，限流响应应 ≤ 50ms 因此不会拉高 P99）
    http_req_duration: ["p(99)<500", "p(95)<300"],

    // 非预期 5xx 错误率 ≤ 1%（429/503 限流不计入，见下方自定义指标）
    austin_unexpected_errors: ["rate<0.01"],

    // 自定义延迟：单发接口响应时间 P99
    austin_send_latency_ms: ["p(99)<500"],
  },

  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// ──────────────────────────────────────────────────────────────
// 自定义指标
// ──────────────────────────────────────────────────────────────
// 非预期错误计数（排除 429/503 限流响应）
const unexpectedErrors = new Counter("austin_unexpected_errors");
// 被限流请求占比（验证背压机制是否生效）
const rateLimitedRate = new Rate("austin_rate_limited");
// 单发接口延迟趋势（用于 Grafana 趋势图）
const sendLatency = new Trend("austin_send_latency_ms", true);
// 批量发接口延迟趋势
const batchSendLatency = new Trend("austin_batch_send_latency_ms", true);

// ──────────────────────────────────────────────────────────────
// 测试数据：SharedArray 在所有 VU 间共享内存
// ──────────────────────────────────────────────────────────────
const singlePayloads = new SharedArray("singlePayloads", function () {
  return JSON.parse(open(SINGLE_PAYLOAD_FILE));
});

// 批量发数据（仅在 batch/mixed 模式下使用，文件不存在时降级为单发）
let batchPayloads;
try {
  batchPayloads = new SharedArray("batchPayloads", function () {
    return JSON.parse(open(BATCH_PAYLOAD_FILE));
  });
} catch (_) {
  console.log(
    `[api_stress_test] 批量发数据文件 "${BATCH_PAYLOAD_FILE}" 未找到，降级为单发数据。` +
    `如需批量发压测，请先运行: python3 scripts/generate_payloads.py --mode api --batch --output ${BATCH_PAYLOAD_FILE}`
  );
  batchPayloads = singlePayloads;
}

// ──────────────────────────────────────────────────────────────
// 请求头构造
// ──────────────────────────────────────────────────────────────
function buildHeaders() {
  const headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    // X-Request-ID 便于在 Grafana / Jaeger 中精确关联单次请求
    "X-Request-ID": `k6-api-${__VU}-${__ITER}-${Date.now()}`,
  };
  if (AUTH_TOKEN) {
    headers["Authorization"] = `Bearer ${AUTH_TOKEN}`;
  }
  return headers;
}

// ──────────────────────────────────────────────────────────────
// 单发请求（POST /send）
// 对应 Java DTO：SendRequest { code, messageTemplateId, messageParam }
// ──────────────────────────────────────────────────────────────
function doSingleSend(headers) {
  const payload = singlePayloads[Math.floor(Math.random() * singlePayloads.length)];
  const body = JSON.stringify(payload);

  const start = Date.now();
  const res = http.post(SEND_ENDPOINT, body, {
    headers,
    timeout: "10s",
    tags: {
      api: "send",
      template_id: String(payload.messageTemplateId),
    },
  });
  sendLatency.add(Date.now() - start);

  const isRateLimited = res.status === 429 || res.status === 503;
  rateLimitedRate.add(isRateLimited ? 1 : 0);

  check(res, {
    "send: status 200/202 or rate-limited 429/503": (r) =>
      r.status === 200 || r.status === 202 || r.status === 429 || r.status === 503,
    "send: no unexpected 5xx": (r) =>
      r.status !== 500 && r.status !== 502 && r.status !== 504,
    "send: response body not empty": (r) => r.body && r.body.length > 0,
  });

  if (res.status !== 200 && res.status !== 202 && !isRateLimited) {
    unexpectedErrors.add(1);
  }
}

// ──────────────────────────────────────────────────────────────
// 批量发请求（POST /batchSend）
// 对应 Java DTO：BatchSendRequest { code, messageTemplateId, messageParamList[] }
// ──────────────────────────────────────────────────────────────
function doBatchSend(headers) {
  const payload = batchPayloads[Math.floor(Math.random() * batchPayloads.length)];
  const body = JSON.stringify(payload);

  const start = Date.now();
  const res = http.post(BATCH_SEND_ENDPOINT, body, {
    headers,
    timeout: "15s",
    tags: {
      api: "batchSend",
      template_id: String(payload.messageTemplateId),
    },
  });
  batchSendLatency.add(Date.now() - start);

  const isRateLimited = res.status === 429 || res.status === 503;
  rateLimitedRate.add(isRateLimited ? 1 : 0);

  check(res, {
    "batchSend: status 200/202 or rate-limited 429/503": (r) =>
      r.status === 200 || r.status === 202 || r.status === 429 || r.status === 503,
    "batchSend: no unexpected 5xx": (r) =>
      r.status !== 500 && r.status !== 502 && r.status !== 504,
    "batchSend: response body not empty": (r) => r.body && r.body.length > 0,
  });

  if (res.status !== 200 && res.status !== 202 && !isRateLimited) {
    unexpectedErrors.add(1);
  }
}

// ──────────────────────────────────────────────────────────────
// VU 主循环（每次迭代执行一次请求）
// ──────────────────────────────────────────────────────────────
export default function () {
  const headers = buildHeaders();

  if (API_MODE === "single") {
    doSingleSend(headers);
  } else if (API_MODE === "batch") {
    doBatchSend(headers);
  } else {
    // mixed 模式：70% 单发 + 30% 批量发（模拟真实流量比例）
    if (Math.random() < 0.7) {
      doSingleSend(headers);
    } else {
      doBatchSend(headers);
    }
  }

  // 模拟真实调用间隔（0.1~0.3s），避免纯 closed-loop 压测导致 RPS 虚高
  sleep(Math.random() * 0.2 + 0.1);
}

// ──────────────────────────────────────────────────────────────
// 测试结束摘要
// ──────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const m = data.metrics;

  const summary = {
    scenario: "api_ramp (10→100→1000 VUs)",
    api_mode: API_MODE,
    timestamp: new Date().toISOString(),
    rps: m.http_reqs ? (m.http_reqs.values.rate || 0).toFixed(2) : "N/A",
    p99_ms: m.http_req_duration
      ? (m.http_req_duration.values["p(99)"] || 0).toFixed(2) : "N/A",
    p95_ms: m.http_req_duration
      ? (m.http_req_duration.values["p(95)"] || 0).toFixed(2) : "N/A",
    avg_ms: m.http_req_duration
      ? (m.http_req_duration.values.avg || 0).toFixed(2) : "N/A",
    error_rate_pct: m.http_req_failed
      ? (m.http_req_failed.values.rate * 100 || 0).toFixed(3) : "N/A",
    rate_limited_pct: m.austin_rate_limited
      ? (m.austin_rate_limited.values.rate * 100 || 0).toFixed(3) : "N/A",
    unexpected_errors: m.austin_unexpected_errors
      ? m.austin_unexpected_errors.values.count || 0 : 0,
    send_p99_ms: m.austin_send_latency_ms
      ? (m.austin_send_latency_ms.values["p(99)"] || 0).toFixed(2) : "N/A",
    batch_send_p99_ms: m.austin_batch_send_latency_ms
      ? (m.austin_batch_send_latency_ms.values["p(99)"] || 0).toFixed(2) : "N/A",
  };

  console.log("\n══════════ Austin 场景A 压测结果摘要 ══════════");
  console.log(JSON.stringify(summary, null, 2));
  console.log("════════════════════════════════════════════════\n");

  return {
    stdout: "\n",
    "results/api_stress_summary.json": JSON.stringify(summary, null, 2),
  };
}
