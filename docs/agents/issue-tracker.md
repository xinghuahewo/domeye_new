# Issue 跟踪配置

本项目的问题和 Spec Issue 使用 [xinghuahewo/domeye_new 的 GitHub Issues](https://github.com/xinghuahewo/domeye_new/issues)。

文档与 Issue 的职责划分以 [README 的文档路由](../../README.md#文档导航与权威位置) 为准。执行范围遵守 [仓库协作约定](../../AGENTS.md#执行范围)。

代码主仓库为服务器 SSH origin；GitHub 目前仅承载任务进度。同步 Issue 不包含将源码、凭据或真实数据公开上传。`gh api` 路径也显式使用 `repos/xinghuahewo/domeye_new/`。写入前先查已有任务，优先更新对应任务，避免重复建单。

## 操作方式

使用已完成 GitHub 身份认证的 `gh` CLI。
所有 Issue 命令显式指定 `--repo xinghuahewo/domeye_new`，避免依赖当前目录或默认远端。

正文先写入 UTF-8 文件，再通过 `--body-file` 传入，保留真实换行。
以下命令中的编号、标题、标签和正文路径需替换为本次任务的实际值。

| 操作 | 命令 |
|---|---|
| 创建 | `gh issue create --repo xinghuahewo/domeye_new --title "标题" --body-file /tmp/domeye-issue.md` |
| 阅读正文与讨论 | `gh issue view <编号> --repo xinghuahewo/domeye_new --comments` |
| 读取结构化内容 | `gh issue view <编号> --repo xinghuahewo/domeye_new --json number,title,body,labels,comments,assignees` |
| 列出待处理问题 | `gh issue list --repo xinghuahewo/domeye_new --state open --json number,title,labels,assignees` |
| 发表评论 | `gh issue comment <编号> --repo xinghuahewo/domeye_new --body-file /tmp/domeye-comment.md` |
| 更新正文 | `gh issue edit <编号> --repo xinghuahewo/domeye_new --body-file /tmp/domeye-issue.md` |
| 添加或移除标签 | `gh issue edit <编号> --repo xinghuahewo/domeye_new --add-label "标签"` 或 `--remove-label "标签"` |
| 关闭 | `gh issue close <编号> --repo xinghuahewo/domeye_new` |

分诊标签使用 [角色映射表](triage-labels.md)。
需要处理更多条目时应分页读取，不把一次列表结果当作完整清单。

## 技能用语

- “发布到 Issue 跟踪系统”：创建 GitHub Issue。
- “获取相关任务”：读取对应 Issue 的正文、标签和评论。
- `PRs as a request surface: no`：不将外部 PR 纳入需求分诊队列。
- GitHub 的 Issue 与 PR 共用编号空间；遇到来源不明的编号，先确认对象类型。

## Wayfinder 工作流

使用 Wayfinder 时按以下约定组织工作；普通 Issue 不要求建立地图。

- 地图：一个带 `wayfinder:map` 标签的 Issue，记录线索、决策链接和待查问题。
- 子任务：使用 `wayfinder:research`、`wayfinder:prototype`、`wayfinder:grilling` 或 `wayfinder:task` 标签，并关联为地图的子 Issue。
- 子 Issue 不可用时：在地图正文中维护任务列表，子任务正文用 `Part of #<地图编号>` 引用地图。
- 阻塞关系：优先使用 GitHub 原生 Issue 依赖。调用依赖 API 时使用阻塞项的数字数据库 ID，不使用 Issue 编号或 node_id。
- 原生依赖不可用时：在子任务正文使用 `Blocked by: #<编号>` 记录阻塞项；所有阻塞项关闭后才能视为解除阻塞。
- 选择下一项：按地图顺序，选择未关闭、没有未关闭阻塞项且尚未分配负责人的子任务。
- 认领：为选定任务设置负责人；使用 `--add-assignee @me` 表示当前执行者认领。
- 完成：先评论结果和证据，再关闭任务，并在地图补充结果链接。正式决策仍以对应 ADR 为权威。
