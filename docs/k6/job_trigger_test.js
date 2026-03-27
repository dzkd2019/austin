/**
 * Austin 场景 B — xxl-job 调度发送 压测脚本
 * Dev-QA 产出物
 *
 * 适用链路：
 *   通过 API 注册定时任务，随后由 xxl-job 调度中心触发 CronTaskHandler.execute()，
 *   系统从 cronCrowdPath 指向的 CSV 文件中批量读取接收者并下发海量消息。
 *
 * 压测策略（两种模式，通过 TRIGGER_MODE 切换）：
 *
 *   模式 1 — austin_start（默认，推荐）：
 *     通过 Austin 自身的模板管理接口触发调度：
 *       POST /messageTemplate/save   → 注册消息模板（关联 xxl-job Task）
 *       POST /messageTemplate/start/{id} → 启动定时任务（austin 调 xxl-job admin API）
 *     此路径与真实业务流程 100% 一致，压的是从 API 调用到 xxl-job 注册再到调度拉起的全链路。
 *
 *   模式 2 — xxl_trigger（高级，需要 xxl-job admin 直连权限）：
 *     直接调用 xxl-job admin 的一次性触发接口：
 *       POST /xxl-job-admin/jobinfo/triggerJob（或 /api/jobinfo/trigger，版本不同路径略有差异）
 *     可携带自定义 executorParam（即 messageTemplateId），强制让 CronTaskHandler 立即执行，
 *     绕过 cron 调度周期，适合在任意时间点压测 TaskHandler 的消费路径。
 *
 * 运行前准备:
 *   1. 生成定时任务注册 Payload:
 *        python3 scripts/generate_payloads.py --mode cron --count 50 --output data/cron_payloads.json
 *   2. 确保数据库中存在 cronCrowdPath 指向的人群 CSV 文件（行数越多，压测规模越大）
 *   3. 修改下方 CONFIG 区域（BASE_URL、XXL_ADMIN_URL、AUTH_TOKEN 等）
 *
 * 运行命令:
 *   # 模式 1：通过 Austin API 注册并启动定时任务
 *   k6 run k6/job_trigger_test.js
 *
 *   # 模式 2：直接调用 xxl-job admin 触发接口
 *   k6 run --env TRIGGER_MODE=xxl_trigger k6/job_trigger_test.js
 *
 *   # 指定已存在的 templateId（跳过注册步骤，直接压启动/触发链路）
 *   k6 run --env TRIGGER_MODE=austin_start --env EXISTING_TEMPLATE_IDS=101,102,103 k6/job_trigger_test.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

// ──────────────────────────────────────────────────────────────
// CONFIG：按实际环境修改
// ──────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const XXL_ADMIN_URL = __ENV.XXL_ADMIN_URL || "http://localhost:8888/xxl-job-admin";

// 压测模式：austin_start（默认）或 xxl_trigger
const TRIGGER_MODE = __ENV.TRIGGER_MODE || "austin_start";

// 如果接口有鉴权，填入 Token；否则留空字符串
const AUTH_TOKEN = __ENV.AUTH_TOKEN || "";

// xxl-job admin 登录凭证（仅 xxl_trigger 模式需要）
const XXL_USERNAME = __ENV.XXL_USERNAME || "admin";
const XXL_PASSWORD = __ENV.XXL_PASSWORD || "123456";

// 已存在的消息模板 ID 列表（填入后跳过注册步骤，直接压启动/触发链路）
// 格式："101,102,103"，留空则使用从 cron_payloads.json 注册得到的 ID
const EXISTING_TEMPLATE_IDS_STR = __ENV.EXISTING_TEMPLATE_IDS || "";

// cron_payloads.json 路径（--mode cron 生成的文件）
const CRON_PAYLOAD_FILE = __ENV.CRON_PAYLOAD_FILE || "data/cron_payloads.json";

// ──────────────────────────────────────────────────────────────
// 流量模型：场景 B 以并发启动任务为主要压力，VUs 数不需要很高，
// 真正的压力来自每次任务启动后 CronTaskHandler 对 CSV 文件的批量处理
// ──────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    job_trigger: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        // 小量预热：验证接口通路和 xxl-job 连通性
        { duration: "30s", target: 5 },
        { duration: "1m", target: 5 },

        // 中量压测：多任务并发启动，观察 Kafka 生产端 Lag 增长曲线
        { duration: "30s", target: 20 },
        { duration: "3m", target: 20 },

        // 峰值冲击：同时启动大量定时任务，模拟运营活动批量开启场景
        // （真正的瓶颈在 xxl-job → TaskHandler → CSV 读取 → Kafka 生产这条链路，而非 HTTP 并发数）
        { duration: "30s", target: 50 },
        { duration: "2m", target: 50 },

        // 收尾
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "30s",
    },
  },

  thresholds: {
    // 任务注册/启动接口本身不应成为瓶颈，P99 ≤ 1s
    http_req_duration: ["p(99)<1000"],
    // 非预期 5xx 错误率 ≤ 1%
    austin_job_errors: ["rate<0.01"],
  },

  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// ──────────────────────────────────────────────────────────────
// 自定义指标
// ──────────────────────────────────────────────────────────────
// 任务注册失败计数
const jobRegisterErrors = new Counter("austin_job_register_errors");
// 任务启动/触发失败率（非预期）
const jobErrors = new Rate("austin_job_errors");
// 任务启动接口延迟
const jobStartLatency = new Trend("austin_job_start_latency_ms", true);
// 任务注册接口延迟
const jobRegisterLatency = new Trend("austin_job_register_latency_ms", true);

// ──────────────────────────────────────────────────────────────
// 测试数据：SharedArray 在所有 VU 间共享
// ──────────────────────────────────────────────────────────────
const cronPayloads = new SharedArray("cronPayloads", function () {
  return JSON.parse(open(CRON_PAYLOAD_FILE));
});

// 解析预置的模板 ID（如果提供了）
const presetTemplateIds = EXISTING_TEMPLATE_IDS_STR
  ? EXISTING_TEMPLATE_IDS_STR.split(",").map((s) => parseInt(s.trim(), 10)).filter(Boolean)
  : [];

// ──────────────────────────────────────────────────────────────
// 请求头构造
// ──────────────────────────────────────────────────────────────
function buildHeaders(contentType = "application/json") {
  const headers = {
    "Content-Type": contentType,
    "Accept": "application/json",
    "X-Request-ID": `k6-job-${__VU}-${__ITER}-${Date.now()}`,
  };
  if (AUTH_TOKEN) {
    headers["Authorization"] = `Bearer ${AUTH_TOKEN}`;
  }
  return headers;
}

// ──────────────────────────────────────────────────────────────
// 模式 1：通过 Austin API 注册并启动定时任务
//
// 步骤：
//   1. POST /messageTemplate/save   → 注册 MessageTemplate（含 cronCrowdPath、expectPushTime）
//      对齐 Java DTO：MessageTemplate { name, idType, sendChannel, templateType,
//                                       expectPushTime, msgContent, cronCrowdPath,
//                                       sendAccount, auditStatus, msgStatus, creator, team }
//   2. 从响应中提取新创建的 id
//   3. POST /messageTemplate/start/{id} → 调用 Austin → xxl-job 注册并启动 Job
// ──────────────────────────────────────────────────────────────
function doAustinStartMode() {
  const headers = buildHeaders();

  // 如果提供了预置模板 ID，跳过注册步骤，直接压启动接口
  if (presetTemplateIds.length > 0) {
    const templateId = presetTemplateIds[Math.floor(Math.random() * presetTemplateIds.length)];
    doStartCronTask(templateId, headers);
    return;
  }

  // 步骤 1：注册消息模板（MessageTemplate）
  const payload = cronPayloads[Math.floor(Math.random() * cronPayloads.length)];
  const body = JSON.stringify(payload);

  const regStart = Date.now();
  const saveRes = http.post(`${BASE_URL}/messageTemplate/save`, body, {
    headers,
    timeout: "10s",
    tags: { api: "messageTemplate/save" },
  });
  jobRegisterLatency.add(Date.now() - regStart);

  const saveOk = check(saveRes, {
    "save: status 200": (r) => r.status === 200,
    "save: response has id": (r) => {
      try {
        const json = JSON.parse(r.body);
        // AustinResult 包装后结构：直接返回 MessageTemplate 对象或包裹在 data 字段中
        return (json.id !== undefined && json.id !== null) ||
               (json.data && json.data.id !== undefined);
      } catch (_) {
        return false;
      }
    },
  });

  if (!saveOk) {
    jobRegisterErrors.add(1);
    jobErrors.add(1);
    return;
  }

  // 提取注册返回的 id
  let templateId;
  try {
    const json = JSON.parse(saveRes.body);
    templateId = json.id || (json.data && json.data.id);
  } catch (_) {
    jobRegisterErrors.add(1);
    jobErrors.add(1);
    return;
  }

  if (!templateId) {
    jobRegisterErrors.add(1);
    jobErrors.add(1);
    return;
  }

  // 步骤 2：启动定时任务（触发 Austin → xxl-job 注册 + 开始调度）
  doStartCronTask(templateId, headers);
}

function doStartCronTask(templateId, headers) {
  const startTs = Date.now();
  // POST /messageTemplate/start/{id}
  // Austin 内部：创建 XxlJobInfo 并调用 xxl-job admin API 的 /jobinfo/start
  const startRes = http.post(
    `${BASE_URL}/messageTemplate/start/${templateId}`,
    null,
    {
      headers,
      timeout: "15s",
      tags: { api: "messageTemplate/start", template_id: String(templateId) },
    }
  );
  jobStartLatency.add(Date.now() - startTs);

  const startOk = check(startRes, {
    "start: status 200": (r) => r.status === 200,
    "start: no 5xx": (r) => r.status < 500,
  });

  jobErrors.add(startOk ? 0 : 1);
}

// ──────────────────────────────────────────────────────────────
// 模式 2：直接调用 xxl-job admin 一次性触发接口
//
// xxl-job admin 提供 /jobinfo/triggerJob 接口（2.x 版本），参数：
//   id           → xxl-job jobId（需要提前知道，或从 Austin 查询 cronTaskId）
//   executorParam→ 传给 JobHandler 的参数（这里是 messageTemplateId）
//
// 使用此模式可绕过 cron 调度周期，在压测中随时强制触发 CronTaskHandler.execute()，
// 模拟运营人员手动触发或 xxl-job 到点自动触发的效果。
// ──────────────────────────────────────────────────────────────
function doXxlTriggerMode() {
  if (presetTemplateIds.length === 0) {
    console.log(
      "[job_trigger_test] xxl_trigger 模式需要通过 EXISTING_TEMPLATE_IDS 提供有效的 xxl-job jobId 列表"
    );
    sleep(1);
    return;
  }

  // 在 xxl_trigger 模式下，EXISTING_TEMPLATE_IDS 应填入 xxl-job 的 jobId（即 cronTaskId），
  // executorParam 中填入对应的 messageTemplateId（让 CronTaskHandler 能读取到）
  const jobId = presetTemplateIds[Math.floor(Math.random() * presetTemplateIds.length)];
  // executorParam 中传入 messageTemplateId，CronTaskHandler 通过 XxlJobHelper.getJobParam() 读取
  const executorParam = String(jobId);

  const formBody = `id=${jobId}&executorParam=${encodeURIComponent(executorParam)}`;
  const headers = buildHeaders("application/x-www-form-urlencoded");

  const startTs = Date.now();
  // xxl-job admin 一次性触发接口（需要 xxl-job admin 的 session cookie，此处简化为 Basic Auth 模式）
  // 实际部署时，如果 xxl-job admin 开启了鉴权，需先 POST /auth/doLogin 获取 cookie
  const triggerRes = http.post(
    `${XXL_ADMIN_URL}/jobinfo/triggerJob`,
    formBody,
    {
      headers: {
        ...headers,
        // xxl-job admin 默认通过 cookie 鉴权，压测时可关闭鉴权或预先注入 cookie
        "Cookie": __ENV.XXL_COOKIE || "",
      },
      timeout: "15s",
      tags: { api: "xxl/triggerJob", job_id: String(jobId) },
    }
  );
  jobStartLatency.add(Date.now() - startTs);

  const ok = check(triggerRes, {
    "xxl trigger: status 200": (r) => r.status === 200,
    "xxl trigger: code 200 in body": (r) => {
      try {
        return JSON.parse(r.body).code === 200;
      } catch (_) {
        return false;
      }
    },
  });

  jobErrors.add(ok ? 0 : 1);
}

// ──────────────────────────────────────────────────────────────
// VU 主循环
// ──────────────────────────────────────────────────────────────
export default function () {
  if (TRIGGER_MODE === "xxl_trigger") {
    doXxlTriggerMode();
  } else {
    doAustinStartMode();
  }

  // 场景 B 的真实压力在 xxl-job 触发后的异步处理链路（CSV 读取 → Kafka 生产）上，
  // HTTP 层本身 RPS 不高，适当增大思考时间以避免注册接口被打爆（重点在下游链路）
  sleep(Math.random() * 1.0 + 0.5);
}

// ──────────────────────────────────────────────────────────────
// 测试结束摘要
// ──────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const m = data.metrics;

  const summary = {
    scenario: "job_trigger (场景B: xxl-job调度)",
    trigger_mode: TRIGGER_MODE,
    timestamp: new Date().toISOString(),
    rps: m.http_reqs ? (m.http_reqs.values.rate || 0).toFixed(2) : "N/A",
    p99_ms: m.http_req_duration
      ? (m.http_req_duration.values["p(99)"] || 0).toFixed(2) : "N/A",
    job_start_p99_ms: m.austin_job_start_latency_ms
      ? (m.austin_job_start_latency_ms.values["p(99)"] || 0).toFixed(2) : "N/A",
    job_register_p99_ms: m.austin_job_register_latency_ms
      ? (m.austin_job_register_latency_ms.values["p(99)"] || 0).toFixed(2) : "N/A",
    job_error_rate_pct: m.austin_job_errors
      ? (m.austin_job_errors.values.rate * 100 || 0).toFixed(3) : "N/A",
    job_register_errors: m.austin_job_register_errors
      ? m.austin_job_register_errors.values.count || 0 : 0,
    note: "真实压力在 Kafka Lag 和 xxl-job Executor 处理线程上，请同步观察 Grafana 监控",
  };

  console.log("\n══════════ Austin 场景B 压测结果摘要 ══════════");
  console.log(JSON.stringify(summary, null, 2));
  console.log("════════════════════════════════════════════════\n");
  console.log("⚠️  重要：场景B的真实压力体现在以下指标，请在Grafana中查看：");
  console.log("   1. kafka_consumer_group_lag — Kafka 消费积压（核心指标）");
  console.log("   2. hikaricp_connections_active — 数据库连接池占用");
  console.log("   3. jvm_threads_live_threads — xxl-job Executor 线程数增长");

  return {
    stdout: "\n",
    "results/job_trigger_summary.json": JSON.stringify(summary, null, 2),
  };
}
