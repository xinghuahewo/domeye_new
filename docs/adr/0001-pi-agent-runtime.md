---
status: accepted
date: 2026-09-08
---

# 采用 Pi Agent Runtime

国家中断 Agent 采用 Pi Agent Runtime。用户在本轮选型中明确指定了这一运行时；本记录表示选型已接受，接入和产品验收尚未完成。

## 选择依据

本期交付范围与验收条件以 [Spec Issue #1](https://github.com/xinghuahewo/domeye_new/issues/1) 为准，产品边界与领域术语以 [CONTEXT.md](../../CONTEXT.md) 为准。Pi 官方 Agent 核心提供自定义工具、消息状态和事件流，能够作为多轮查询与回答的运行基础；这是支持该选择的技术依据，不是对本项目运行效果的验收结论。[官方 Agent 文档](https://github.com/earendil-works/pi/blob/main/packages/agent/README.md)

## 本次验证的模型选择

用户已明确指定本次最小真实调用链验证使用 DeepSeek V4 Pro。调用模型标识为 `deepseek-v4-pro`，服务地址为 `https://api.deepseek.com`；标识与地址以 [DeepSeek 官方文档](https://api-docs.deepseek.com/) 为依据。凭据通过仓库外专用配置注入，不写入源码或文档。具体 API 别名对应的不可变模型修订仍为 Unknown。

本次候选实现、运行证据与验收结果统一见 [Spike Issue #12](https://github.com/xinghuahewo/domeye_new/issues/12)。这一选择不表示正式 Agent 接入或回答验收已经完成。

## 尚未确定

- 正式接入采用的 Pi 包、接入层及固定版本。
- 正式接入的模型配置管理。
- 部署进程、与现有后端及前端的通信方式。
- 业务工具合同、执行限制，以及对话恢复和数据引用的具体实现。

上述事项需要在后续最小接入验证中确定。库的默认行为不能替代 [项目执行范围](../../AGENTS.md#执行范围)；用户交互的待实现要求由 Spec Issue 维护，产品能力边界仍由 CONTEXT.md 维护。

当前系统结构见 [系统结构与数据流](../architecture/系统结构与数据流.md)。本次决策不改变其中对当前实现的描述。
