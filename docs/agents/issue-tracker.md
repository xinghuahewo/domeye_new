# 任务跟踪：GitHub Issues

任务与规格存放于 `xinghuahewo/domeye_new` 的 GitHub Issues，使用 gh CLI。

代码 origin 是 SSH 远端，不能据此自动推断 GitHub 仓库。
所有 gh issue 操作显式指定 `--repo xinghuahewo/domeye_new`；
gh api 路径显式使用 `repos/xinghuahewo/domeye_new/`。

本配置说明操作位置和方法，不额外授予远端写入权限。
创建、评论、改标签、认领或关闭任务须在用户授权范围内。

## 常用操作

- 读取：`gh issue view <编号> --repo xinghuahewo/domeye_new --json number,title,body,state,labels,comments`
- 列表：`gh issue list --repo xinghuahewo/domeye_new --state open --json number,title,body,labels,assignees`，按需增加标签和状态筛选。
- 创建：`gh issue create --repo xinghuahewo/domeye_new --title "<标题>" --body-file <正文文件>`
- 评论：`gh issue comment <编号> --repo xinghuahewo/domeye_new --body-file <正文文件>`
- 标签：`gh issue edit <编号> --repo xinghuahewo/domeye_new --add-label "<标签>"`；移除使用 `--remove-label`。
- 关闭：`gh issue close <编号> --repo xinghuahewo/domeye_new --comment "<结论>"`

skill 要求“发布到任务跟踪系统”时，对应创建 GitHub Issue；
要求“获取相关任务”时，读取正文、标签及相关评论。
写入前先查找现有任务，优先更新明确对应的任务，避免重复建单。

## PR 请求入口

PRs as a request surface: no.

GitHub 的 Issue 与 PR 共用编号；遇到类型不明的引用，先只读核实。

## Wayfinder 约定

仅在实际使用 wayfinder 且获得相应授权时应用：

- 地图：一个带 `wayfinder:map` 标签的 Issue，保存笔记、已定决策和待澄清事项。
- 子任务：优先用 GitHub sub-issue 关联；不可用时，在地图正文使用任务列表，并在子任务开头写 `Part of #<地图编号>`。
- 类型标签：`wayfinder:research`、`wayfinder:prototype`、`wayfinder:grilling`、`wayfinder:task`。
- 阻塞关系：优先使用 GitHub 原生 Issue dependencies；不可用时，在子任务开头写 `Blocked by: #<编号>`。所有阻塞项关闭后才可推进。
- 下一项：按地图顺序选择没有未关闭阻塞项、且尚未分配执行者的开放子任务。
- 认领：在授权范围内使用 `gh issue edit <编号> --repo xinghuahewo/domeye_new --add-assignee @me`。
- 收口：将结果写回任务，达到验收条件后关闭，并在地图中留下结论和链接。

配置本身不创建地图、任务、依赖或标签。
