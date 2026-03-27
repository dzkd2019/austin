#!/usr/bin/env python3
"""
Austin 压测 Mock 数据生成器
Dev-QA 产出物：批量生成符合 /send 接口格式的测试 Payload，输出到 data/payloads.json

使用方式:
    python3 scripts/generate_payloads.py --count 1000 --output data/payloads.json

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

EMAIL_DOMAINS = ["qq.com", "163.com", "gmail.com", "outlook.com", "example.com"]

VARIABLE_KEYS_SMS = ["code", "name", "amount", "order_no"]
VARIABLE_KEYS_EMAIL = ["username", "product", "expire_date", "order_id"]

BUSINESS_CODES = ["send"]  # 目前只测发送场景


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
# 核心生成函数
# ──────────────────────────────────────────────────────────────

def generate_payload(include_biz_id: bool = False) -> dict:
    """生成一条符合 SendRequest 格式的 JSON Payload"""
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


def generate_payloads(count: int, include_biz_id: bool = False) -> list:
    return [generate_payload(include_biz_id) for _ in range(count)]


# ──────────────────────────────────────────────────────────────
# CLI 入口
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Austin 压测 Mock 数据生成器 — 批量生成 /send 请求 Payload"
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
        "--biz-id",
        action="store_true",
        default=False,
        help="是否为每条请求生成业务幂等 ID（bizId）",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="输出格式化（缩进）JSON（文件较大时不建议开启）",
    )
    args = parser.parse_args()

    # 创建输出目录
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[generate_payloads] 正在生成 {args.count} 条 Payload...", file=sys.stderr)
    payloads = generate_payloads(args.count, include_biz_id=args.biz_id)

    indent = 2 if args.pretty else None
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payloads, f, ensure_ascii=False, indent=indent)

    size_kb = output_path.stat().st_size / 1024
    print(
        f"[generate_payloads] 完成！输出至 {output_path}，"
        f"共 {len(payloads)} 条，文件大小 {size_kb:.1f} KB",
        file=sys.stderr,
    )

    # 向 stdout 输出前 3 条样例，便于快速验证
    print("\n── 样例数据（前 3 条）──")
    print(json.dumps(payloads[:3], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
