#!/usr/bin/env python3
"""
Austin 压测 Mock 数据生成器 v2
Dev-QA 产出物：批量生成符合接口格式的测试 Payload，输出到 data/ 目录

支持两种生成模式：
  --mode api   生成 POST /send 或 POST /batchSend 的 Payload（场景 A：实时 API 发送）
  --mode cron  生成 POST /messageTemplate/save 的 Payload（场景 B：注册定时任务）

使用方式:
    # 场景 A：生成单发 Payload（SendRequest 格式）
    python3 scripts/generate_payloads.py --mode api --count 2000 --output data/api_payloads.json

    # 场景 A：生成批量发 Payload（BatchSendRequest 格式）
    python3 scripts/generate_payloads.py --mode api --batch --count 500 --output data/batch_payloads.json

    # 场景 B：生成定时任务注册 Payload（MessageTemplate 格式）
    python3 scripts/generate_payloads.py --mode cron --count 50 --output data/cron_payloads.json

依赖: Python 3.8+，标准库，无需 pip install。
"""

import argparse
import json
import os
import random
import string
import sys
from pathlib import Path


# ──────────────────────────────────────────────────────────────
# 常量：与真实业务对齐，按需修改
# ──────────────────────────────────────────────────────────────
TEMPLATE_IDS = [1001, 1002, 1003, 1004, 1005]  # 对应数据库中已存在的 messageTemplateId

CHANNEL_TYPES = ["SMS", "EMAIL", "PUSH", "WECHAT"]

# 与 MessageTemplate.sendChannel 枚举值对齐
SEND_CHANNEL_MAP = {
    "SMS": 10,
    "EMAIL": 40,
    "PUSH": 50,
    "WECHAT": 60,
}

EMAIL_DOMAINS = ["qq.com", "163.com", "gmail.com", "outlook.com", "example.com"]

VARIABLE_KEYS_SMS = ["code", "name", "amount", "order_no"]
VARIABLE_KEYS_EMAIL = ["username", "product", "expire_date", "order_id"]

# 对应 MessageTemplate.idType 枚举值
ID_TYPE_PHONE = 1
ID_TYPE_EMAIL = 3


# ──────────────────────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────────────────────

def rand_phone() -> str:
    """生成 11 位中国大陆手机号（测试用，非真实号码）"""
    prefixes = ["130", "131", "132", "133", "155", "156", "158",
                "180", "181", "182", "185", "186", "188", "189"]
    return random.choice(prefixes) + "".join(random.choices(string.digits, k=8))


def rand_email() -> str:
    name_len = random.randint(5, 12)
    name = "".join(random.choices(string.ascii_lowercase + string.digits, k=name_len))
    return f"{name}@{random.choice(EMAIL_DOMAINS)}"


def rand_receivers(channel: str, max_count: int = 5) -> str:
    """
    接口规范：多个接收者用逗号分隔，最多 100 个。
    压测时随机生成 1-max_count 个。
    """
    count = random.randint(1, max_count)
    if channel == "EMAIL":
        return ",".join(rand_email() for _ in range(count))
    return ",".join(rand_phone() for _ in range(count))


def rand_variables(template_id: int) -> dict:
    """根据 templateId 生成对应的动态变量"""
    if template_id % 2 == 0:
        keys = VARIABLE_KEYS_EMAIL
    else:
        keys = VARIABLE_KEYS_SMS

    variables = {}
    for k in random.sample(keys, k=random.randint(1, len(keys))):
        if "code" in k:
            variables[k] = "".join(random.choices(string.digits, k=6))
        elif "amount" in k:
            variables[k] = f"{random.uniform(0.01, 9999.99):.2f}"
        elif "date" in k:
            variables[k] = f"2026-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        elif "order" in k or "id" in k:
            variables[k] = "ORD" + "".join(random.choices(string.digits, k=10))
        else:
            variables[k] = "user_" + "".join(random.choices(string.ascii_lowercase, k=6))
    return variables


def rand_biz_id() -> str:
    """可选的业务幂等 ID，模拟上游传入"""
    return "BIZ" + "".join(random.choices(string.hexdigits[:16], k=16)).upper()


# ──────────────────────────────────────────────────────────────
# 场景 A：实时 API 发送 Payload 生成函数
# 对应 Java DTO：SendRequest / BatchSendRequest / MessageParam
# ──────────────────────────────────────────────────────────────

def generate_single_send_payload(include_biz_id: bool = False) -> dict:
    """
    生成一条符合 SendRequest 格式的 Payload。
    对齐字段：
      SendRequest.code              → "send"（BusinessCode.COMMON_SEND）
      SendRequest.messageTemplateId → Long
      SendRequest.messageParam      → MessageParam
        MessageParam.receiver       → 逗号分隔的手机号/邮箱（必填）
        MessageParam.variables      → Map<String,String>（可选）
        MessageParam.bizId          → String（可选，幂等 ID）
        MessageParam.extra          → Map<String,String>（可选，扩展参数）
    """
    template_id = random.choice(TEMPLATE_IDS)
    channel = random.choice(CHANNEL_TYPES)
    receivers = rand_receivers(channel)
    variables = rand_variables(template_id)

    message_param = {
        "receiver": receivers,
        "variables": variables,
    }
    if include_biz_id:
        message_param["bizId"] = rand_biz_id()

    return {
        "code": "send",
        "messageTemplateId": template_id,
        "messageParam": message_param,
    }


def generate_batch_send_payload(batch_size: int = 5, include_biz_id: bool = False) -> dict:
    """
    生成一条符合 BatchSendRequest 格式的 Payload（不同文案发给不同的人）。
    对齐字段：
      BatchSendRequest.code              → "send"
      BatchSendRequest.messageTemplateId → Long
      BatchSendRequest.messageParamList  → List<MessageParam>
        MessageParam.receiver            → 必填
        MessageParam.variables           → Map<String,String>（可选）
        MessageParam.bizId               → String（可选）
        MessageParam.extra               → Map<String,String>（可选）
    """
    template_id = random.choice(TEMPLATE_IDS)
    channel = random.choice(CHANNEL_TYPES)
    count = random.randint(2, batch_size)

    message_param_list = []
    for _ in range(count):
        param = {
            "receiver": rand_receivers(channel, max_count=3),
            "variables": rand_variables(template_id),
        }
        if include_biz_id:
            param["bizId"] = rand_biz_id()
        message_param_list.append(param)

    return {
        "code": "send",
        "messageTemplateId": template_id,
        "messageParamList": message_param_list,
    }


# ──────────────────────────────────────────────────────────────
# 场景 B：定时任务注册 Payload 生成函数
# 对应 Java DTO：MessageTemplate（通过 POST /messageTemplate/save 注册，
#                随后由 POST /messageTemplate/start/{id} 启动 xxl-job 调度）
# ──────────────────────────────────────────────────────────────

# cron 表达式样本（用于 expectPushTime）
CRON_EXPRESSIONS = [
    "0 0/5 * * * ?",   # 每 5 分钟
    "0 0 9 * * ?",     # 每天早上 9 点
    "0 0 10 * * ?",    # 每天早上 10 点
    "0 0 20 * * ?",    # 每天晚上 8 点
    "0 30 12 * * ?",   # 每天中午 12:30
]

# 消息内容模板（模拟各渠道的短信/推送内容）
MSG_CONTENT_TEMPLATES = [
    '{{"content":"您的验证码是{$code}，请勿泄露给他人。"}}',
    '{{"content":"尊敬的{$name}，您的订单{$order_no}已发货，请注意查收。"}}',
    '{{"content":"您有一笔{$amount}元的待付款订单，请在24小时内完成支付。"}}',
    '{{"title":"重要通知","content":"您的账户将于{$expire_date}到期，请及时续费。"}}',
]

# 消息模板类型（对应 templateType 枚举：1-通知类 2-营销类 3-验证码）
TEMPLATE_TYPES = [1, 2, 3]

# 审核状态（auditStatus：0-待审核 1-审核通过 2-审核拒绝）
AUDIT_STATUS_APPROVED = 1

# 消息状态（msgStatus：10-草稿 20-审核中 30-发送中）
MSG_STATUS_PENDING = 10


def generate_cron_task_payload(crowd_file_path: str = "") -> dict:
    """
    生成一条符合 MessageTemplate 格式的定时任务注册 Payload。
    对应接口：POST /messageTemplate/save
    之后需要调用：POST /messageTemplate/start/{id} 来启动 xxl-job 调度。

    对齐字段：
      MessageTemplate.name           → 模板标题（必填）
      MessageTemplate.idType         → 接收者 ID 类型（1=手机号, 3=邮箱）
      MessageTemplate.sendChannel    → 发送渠道（10=SMS, 40=EMAIL, 50=PUSH, 60=WECHAT）
      MessageTemplate.templateType   → 模板类型（1=通知, 2=营销, 3=验证码）
      MessageTemplate.expectPushTime → cron 表达式（定时发送时必填）
      MessageTemplate.msgContent     → 消息内容（含占位符）
      MessageTemplate.cronCrowdPath  → 人群文件路径（定时任务必填，系统从此文件读取接收者）
      MessageTemplate.sendAccount    → 发送账号 ID
      MessageTemplate.auditStatus    → 审核状态
      MessageTemplate.msgStatus      → 消息状态
      MessageTemplate.creator        → 创建者
      MessageTemplate.team           → 业务团队
    """
    channel = random.choice(list(SEND_CHANNEL_MAP.keys()))
    send_channel_code = SEND_CHANNEL_MAP[channel]
    id_type = ID_TYPE_EMAIL if channel == "EMAIL" else ID_TYPE_PHONE

    # 如果未传入人群文件路径，则生成一个压测占位路径
    if not crowd_file_path:
        crowd_file_path = f"/data/austin/crowd/perf_test_{random.randint(10000, 99999)}.csv"

    template_suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    msg_content = random.choice(MSG_CONTENT_TEMPLATES)

    return {
        "name": f"PerfTest_{channel}_{template_suffix}",
        "idType": id_type,
        "sendChannel": send_channel_code,
        "templateType": random.choice(TEMPLATE_TYPES),
        "expectPushTime": random.choice(CRON_EXPRESSIONS),
        "msgContent": msg_content,
        "cronCrowdPath": crowd_file_path,
        "sendAccount": random.randint(1, 5),
        "auditStatus": AUDIT_STATUS_APPROVED,
        "msgStatus": MSG_STATUS_PENDING,
        "creator": "perf_test_bot",
        "team": "perf",
        "proposer": "perf_test",
    }


# ──────────────────────────────────────────────────────────────
# CLI 入口
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Austin 压测 Mock 数据生成器 — 双模式生成测试 Payload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 场景 A：生成 2000 条 /send 单发 Payload
  python3 scripts/generate_payloads.py --mode api --count 2000 --output data/api_payloads.json

  # 场景 A：生成 500 条 /batchSend 批量发 Payload（每批最多 5 人）
  python3 scripts/generate_payloads.py --mode api --batch --batch-size 5 --count 500 --output data/batch_payloads.json

  # 场景 A：含业务幂等 ID
  python3 scripts/generate_payloads.py --mode api --count 1000 --biz-id --output data/api_payloads.json

  # 场景 B：生成 50 条定时任务注册 Payload
  python3 scripts/generate_payloads.py --mode cron --count 50 --output data/cron_payloads.json
        """,
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["api", "cron"],
        default="api",
        help="生成模式：api=实时API发送(场景A), cron=定时任务注册(场景B)（默认 api）",
    )
    parser.add_argument(
        "--count", "-n",
        type=int,
        default=1000,
        help="生成的 Payload 条数（默认 1000）",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="data/payloads.json",
        help="输出文件路径（默认 data/payloads.json）",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        default=False,
        help="[仅 --mode api] 生成 BatchSendRequest 格式（/batchSend 接口）而非单发格式",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="[仅 --mode api --batch] 每条批量请求最多包含的接收人数（默认 5，最大 100）",
    )
    parser.add_argument(
        "--biz-id",
        action="store_true",
        default=False,
        help="[仅 --mode api] 是否为每条请求生成业务幂等 ID（bizId）",
    )
    parser.add_argument(
        "--crowd-path",
        type=str,
        default="",
        help="[仅 --mode cron] 人群文件路径前缀（留空则自动生成 /data/austin/crowd/ 下的占位路径，"
             "按实际部署环境修改）",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="输出格式化（缩进）JSON（文件较大时不建议开启）",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"[generate_payloads] 模式={args.mode}, 正在生成 {args.count} 条 Payload...",
        file=sys.stderr,
    )

    payloads = []
    if args.mode == "api":
        if args.batch:
            batch_size = min(args.batch_size, 100)
            payloads = [
                generate_batch_send_payload(batch_size=batch_size, include_biz_id=args.biz_id)
                for _ in range(args.count)
            ]
        else:
            payloads = [
                generate_single_send_payload(include_biz_id=args.biz_id)
                for _ in range(args.count)
            ]
    elif args.mode == "cron":
        payloads = [
            generate_cron_task_payload(crowd_file_path=args.crowd_path)
            for _ in range(args.count)
        ]

    indent = 2 if args.pretty else None
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payloads, f, ensure_ascii=False, indent=indent)

    size_kb = output_path.stat().st_size / 1024
    print(
        f"[generate_payloads] 完成！输出至 {output_path}，"
        f"共 {len(payloads)} 条，文件大小 {size_kb:.1f} KB",
        file=sys.stderr,
    )

    print("\n── 样例数据（前 3 条）──")
    print(json.dumps(payloads[:3], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
