# 分诊标签配置

工程技能使用以下五种分诊角色。本表是角色与 GitHub 标签字符串的权威映射。

| 技能角色 | GitHub 标签 | 含义 |
|---|---|---|
| `needs-triage` | `needs-triage` | 等待维护者评估 |
| `needs-info` | `needs-info` | 等待补充必要信息 |
| `ready-for-agent` | `ready-for-agent` | 需求和验收条件已明确，可由 Agent 接手 |
| `ready-for-human` | `ready-for-human` | 需要人工实施 |
| `wontfix` | `wontfix` | 已决定不处理 |

技能要求应用某个分诊角色时，使用对应的 GitHub 标签字符串。
标签不替代 [仓库协作约定](../../AGENTS.md#执行范围) 中的任务授权。

本表定义标签名称，不表示远端标签已经创建。
