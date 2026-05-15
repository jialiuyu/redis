#!/usr/bin/env python3
"""Proposal Agent

交互式接收用户输入，并将输入整理为结构化方案文档（Markdown）。
"""

import argparse
import datetime
import os
import re
import textwrap

SECTION_PROMPTS = [
    ("title", "方案标题"),
    ("overview", "背景与现状描述"),
    ("objectives", "目标与期望成果"),
    ("scope", "范围与边界"),
    ("requirements", "关键需求或约束"),
    ("approach", "解决方案概要"),
    ("architecture", "架构设计与关键组件"),
    ("timeline", "实施计划与里程碑"),
    ("risks", "风险与对应措施"),
    ("next_steps", "后续行动与落地建议"),
]


def prompt_multiline(prompt_text: str) -> str:
    print(f"\n{prompt_text}")
    print("(请输入多行内容，单独一行回车结束输入)")
    lines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line.strip() == "":
            if lines:
                break
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "proposal"


def build_document(data: dict) -> str:
    parts = [f"# {data['title']}", ""]
    parts.append(f"**生成时间**：{data['created_at']}  ")
    if data.get("author"):
        parts.append(f"**作者**：{data['author']}  ")
    parts.append("")

    def add_section(name: str, header: str):
        text = data.get(name, "")
        if text:
            parts.append(f"## {header}")
            parts.append("")
            parts.append(text)
            parts.append("")

    add_section("overview", "背景与现状")
    add_section("objectives", "目标与期望成果")
    add_section("scope", "范围与边界")
    add_section("requirements", "关键需求与约束")
    add_section("approach", "解决方案概要")
    add_section("architecture", "架构设计与关键组件")
    add_section("timeline", "实施计划与里程碑")
    add_section("risks", "风险与应对措施")
    add_section("next_steps", "后续行动与落地建议")
    return "\n".join(parts).strip() + "\n"


def collect_inputs() -> dict:
    print("方案文档智能体：请按提示输入内容，最后生成结构化方案文档。\n")
    data = {}
    for key, prompt_text in SECTION_PROMPTS:
        data[key] = prompt_multiline(prompt_text)
    return data


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="交互式方案文档生成智能体。"
    )
    parser.add_argument("--author", help="文档作者")
    parser.add_argument("--output", help="输出 Markdown 文件路径")
    parser.add_argument("--title", help="方案标题")
    parser.add_argument("--overview", help="背景与现状描述")
    parser.add_argument("--objectives", help="目标与期望成果")
    parser.add_argument("--scope", help="范围与边界")
    parser.add_argument("--requirements", help="关键需求或约束")
    parser.add_argument("--approach", help="解决方案概要")
    parser.add_argument("--architecture", help="架构设计与关键组件")
    parser.add_argument("--timeline", help="实施计划与里程碑")
    parser.add_argument("--risks", help="风险与应对措施")
    parser.add_argument("--next-steps", dest="next_steps", help="后续行动与落地建议")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="交互式输入所有字段",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    data = {key: getattr(args, key) or "" for key, _ in SECTION_PROMPTS}
    if args.author:
        data["author"] = args.author
    if args.interactive or not args.title:
        collected = collect_inputs()
        data.update({k: v for k, v in collected.items() if v})
    if not data.get("title"):
        data["title"] = "方案文档"
    data["created_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    document = build_document(data)
    output_path = args.output
    if not output_path:
        safe_name = slugify(data["title"])
        date_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"proposal_{safe_name}_{date_suffix}.md"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(document)

    print(f"\n方案文档已生成：{output_path}")
    print("可在 Markdown 查看器中打开查看或继续编辑。")


if __name__ == "__main__":
    main()
