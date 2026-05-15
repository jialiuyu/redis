# 方案文档生成智能体

该智能体用于接收用户输入，并将其转换为结构化的方案文档（Markdown）。

## 文件说明

- `tools/proposal_agent.py`：交互式 CLI 脚本，按提示收集方案信息并生成文档。

## 使用方式

1. 进入仓库根目录：

```bash
cd /Users/jialiuyu/outter_project/redis_hpc_dev/hpc-redis
```

2. 运行交互式生成：

```bash
python3 tools/proposal_agent.py --interactive
```

3. 按提示输入：

- 背景与现状
- 目标与期望成果
- 范围与边界
- 关键需求与约束
- 解决方案概要
- 架构设计与关键组件
- 实施计划与里程碑
- 风险与应对措施
- 后续行动与落地建议

4. 输出文件示例：

```bash
proposal_<slug>_<timestamp>.md
```

## 选项

- `--author`：指定文档作者。
- `--output`：指定输出文件路径。
- `--title`：方案标题。
- `--interactive`：进入交互式输入模式。

## 示例

```bash
python3 tools/proposal_agent.py --interactive --author "张三"
```

> 该脚本可快速将用户输入整理成标准方案文档，适合项目规划、设计评审、解决方案汇报等场景。
