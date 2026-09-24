import { Type } from 'typebox';
import { createDocs } from './docs.mjs';
import { loadSearchSpec, SPEC_TYPES } from './spec.mjs';
import { createRequest, runCode, EXECUTION_LIMITS } from './runtime/executor.mjs';
import { createRequestPolicy } from './runtime/request-policy.mjs';
import { makeSourceReceipt } from './runtime/source-receipt.mjs';
import { truncateResponse } from './runtime/truncate.mjs';

export const EXECUTE_TYPES = `type QueryValue = string | number | boolean;
declare const domeye: {
  request(input: {method: "GET"; path: string; query?: Record<string, QueryValue>}):
    Promise<{status: number; body: unknown; headers?: Record<string, string>}>;
};`;
// 虚构响应：类别互斥、同端点桶可加；字段与路径须按实际 search 结果选择。
export const CALCULATION_EXAMPLE = `async () => {
  const response = await domeye.request({method: "GET", path: "/从发现结果选定的完整路径",
    query: {start_time: "已确定的请求开始", end_time: "已确定的请求结束", version: "已确认版本"}});
  if (response.status !== 200 || response.body?.state !== "available") return response;
  const {scope, total, series} = response.body;
  if (!Array.isArray(series) || series.some(row => !Array.isArray(row?.buckets))) {
    return {...response, reason: "所需明细结构未齐，尚不能计算"};
  }
  const intervals = new Map();
  for (const row of series) for (const bucket of row.buckets) {
    if (!bucket?.start || !bucket?.end_exclusive) return {...response, reason: "分桶端点未齐"};
    intervals.set(JSON.stringify([bucket.start, bucket.end_exclusive]),
      {start: bucket.start, end_exclusive: bucket.end_exclusive});
  }
  const timeCandidates = [...intervals.values()].map(interval => {
    const parts = series.map(row => row.buckets.filter(bucket =>
      bucket.start === interval.start && bucket.end_exclusive === interval.end_exclusive));
    const complete = parts.every(matches => matches.length === 1 && Number.isFinite(matches[0].value));
    return {...interval, value: complete ? parts.reduce((sum, matches) => sum + matches[0].value, 0) : null};
  });
  const groups = [
    {dimension: "类型", candidates: series.map(row => ({name: row.name, value: row.total}))},
    {dimension: "时段开始数", candidates: timeCandidates}
  ];
  return groups.map(({dimension, candidates}) => {
    if (!candidates.length || candidates.some(row => !Number.isFinite(row.value))) {
      return {scope, dimension, candidates, ranked: null, reason: "比较所需值未齐"};
    }
    const ranked = candidates.map(row => ({...row})).sort((a, b) => b.value - a.value);
    for (let i = 0; i < ranked.length; i++) {
      const row = ranked[i];
      row.rank = i > 0 && row.value === ranked[i - 1].value ? ranked[i - 1].rank : i + 1;
      row.percent = Number.isFinite(total) && total > 0 ? 100 * row.value / total : null;
      row.isMajority = Number.isFinite(total) && total > 0 ? row.value > total / 2 : null;
    }
    return {scope, dimension, total, ranked};
  });
}`;

// 虚构资源读数；只供工具说明引用，不是运行时资格检查或答案评分。
export const COMPARISON_EXAMPLE = `async () => {
  // 本示例已确认同一资源定义、单位与观察范围；活动和事件另按其实际窗口条件判断。
  // 每侧版本和末态时间都必须来自返回该侧值的响应；label 仅为文件标签。
  const scope = {metric: "虚构资源量", unit: "个", collector: "虚构观察点"};
  const pairs = [
    {object: "完整时点示例", left: {value: 12, version: "example-v1", state_at: "2000-01-01T01:00:00Z"},
      right: {value: 8, version: "example-v1", state_at: "2000-01-01T02:00:00Z"}},
    {object: "仅有标签示例", left: {value: 12, version: null, state_at: null, label: "01:00"},
      right: {value: 8, version: null, state_at: null, label: "02:00"}},
    {object: "缺值示例", left: {value: null, version: "example-v1", state_at: "2000-01-01T01:00:00Z"},
      right: {value: 3, version: "example-v1", state_at: "2000-01-01T02:00:00Z"}}
  ];
  return {scope, results: pairs.map(row => {
    const {left, right} = row, missing = [];
    if (!left.version || left.version !== right.version) missing.push("两侧自身同版证据");
    if (!left.state_at || !right.state_at) missing.push("两侧实际资源时点");
    if (!Number.isFinite(left.value) || !Number.isFinite(right.value)) missing.push("两侧已知数值");
    if (missing.length) {
      return {...row, comparison: null, missing, note: "分别陈述已有样本，变化尚不能确认"};
    }
    const delta = right.value - left.value;
    return {...row, missing, comparison: {delta, changePercent: left.value > 0 ? 100 * delta / left.value : null}};
  })};
}`;

// 只供工具说明引用；不增加 search 的运行时能力。
export const SEARCH_DISCOVERY_EXAMPLE = `async () => {
  const keyword = "资源"; // 替换为本题的业务关键词。
  return Object.entries(spec.paths)
    .filter(([path, {get: op}]) => op &&
      [path, op.summary, op.description].some(text => text?.toLowerCase().includes(keyword.toLowerCase())))
    .map(([path, {get: op}]) => ({method: "GET", path,
      summary: op.summary, description: op.description, deprecated: op.deprecated,
      responseFields: Object.keys(op.responses?.["200"]?.content?.["application/json"]?.schema?.properties ?? {})}));
}`;

export const SEARCH_PARAMETERS_EXAMPLE = `async () => {
  const paths = ["/从发现结果选定的完整路径"]; // 可一次读取多个适用候选。
  return paths.map(path => {
    const op = spec.paths[path]?.get;
    if (!op) throw new Error("接口不存在，请核对发现结果");
    const schema = op.responses?.["200"]?.content?.["application/json"]?.schema;
    return {path, method: "GET", description: op.description, parameters: op.parameters,
      responseType: schema?.type, fields: Object.keys((schema?.type === "array" ? schema.items : schema)?.properties ?? {})};
  });
}`;

export const SEARCH_PROJECTION_EXAMPLE = `async () => {
  // 按已发现的结构选取本题所需字段；复合字段的原始子树可直接返回。
  const path = "/从发现结果选定的完整路径";
  const field = "从已发现结构选定的字段名";
  const status = "200", media = "application/json";
  const op = spec.paths[path]?.get;
  const response = op?.responses?.[status];
  const subtree = response?.content?.[media]?.schema?.properties?.[field];
  if (subtree === undefined) throw new Error("字段不在所选位置，请核对响应结构");
  return {path, method: "GET", parameters: op.parameters, status, media, responseDescription: response.description,
    availableFields: Object.keys(response.content[media].schema.properties ?? {}),
    field, schema: subtree};
}`;

export const SEARCH_PROJECTION_GUIDANCE = '先按本题指标含义发现接口，返回方法、完整路径、业务摘要与响应字段名；含义不清或关键词无命中时，用 docs 确认指标后再选择入口。首次选择一个响应的内容时，先看该对象完整字段名，再读取适用子树；只查询猜测的字段名会漏掉已有汇总。一次代码可合并参数、响应说明与所需子树，优先利用已经提供的相关统计，避免逐个筛选重复取同一汇总。已有结构可复用；所选子树保留原有描述、单位、时间、可空和复合分支。properties 只列直接字段，复合分支仍需按实际结构读取；省略不表示不存在。示例路径和字段位置须按实际规范调整。返回超过 24000 字符会截断并附 TRUNCATED 提示，之后用更具体的代码收窄；截断片段不是完整结构。';

export async function createTools({ apiBaseUrl, specFile, docsConfig, onEvidence = () => {} }) {
  const spec = await loadSearchSpec(specFile);
  const docs = await createDocs(docsConfig);
  const paths = Object.entries(spec.paths).filter(([,item]) => item.get).map(([path]) => path);
  const policy = createRequestPolicy({spec,
    request:createRequest({ baseUrl: apiBaseUrl, paths, onResponse: value => onEvidence('http', value) }),
    onEvidence:value => onEvidence('request_policy',value)
  });
  const request = policy.request;
  const definitions = [
    {
      name: 'docs', label: '查询业务说明',
      description: '查询 Domeye 业务说明，返回标题、固定版本来源和相关原文，不生成标准答案。query 使用要解释的指标或字段，以及要核实的含义、单位或判断条件。首次解释指标时，读到它的计量对象与限制才算取得说明；命中其他指标或只有接口导航时，围绕缺少的定义继续检索。已有且适用的原文可复用。search 的参数结构不替代业务说明；无命中不证明业务不支持，实际范围还需查询接口。',
      parameters: Type.Object({query:Type.String({minLength:1,pattern:'\\S'})},{additionalProperties:false}),
      call: (params,signal) => docs(params,signal)
    },
    {
      name: 'search', label: '查找业务接口',
      description: `查询同版 OpenAPI spec，本地 $ref 已在注入前展开。输入 code 为返回 JSON 值的 JavaScript async 箭头函数，本工具不访问业务数据。
单次代码执行最多 ${EXECUTION_LIMITS.timeoutMs / 1000} 秒。
类型：
${SPEC_TYPES}
spec.paths 的键为完整路径。spec.components 仍可查询；参数和响应已可直接读到完整结构。
保留业务描述、单位、时间和 deprecated 说明。当前规范没有 tags，不按标签猜测。按本题需要的输出证据比较候选：标签、计数、实际区间各自提供不同依据。
${SEARCH_PROJECTION_GUIDANCE}
发现入口：
${SEARCH_DISCOVERY_EXAMPLE}
读取选定接口的参数：
${SEARCH_PARAMETERS_EXAMPLE}
读取选定响应中的字段结构：
${SEARCH_PROJECTION_EXAMPLE}`,
      parameters: Type.Object({code:Type.String({minLength:1,pattern:'\\S'})},{additionalProperties:false}),
      call: ({code},signal) => runCode({code,spec,signal})
    },
    {
      name: 'execute', label: '读取并整理数据',
      description: `用 JavaScript 读取业务数据并完成回答需要的计算。code 是返回 JSON 值的 async 箭头函数；可以组合请求，也可以直接用已取得的数值计算。
单次代码执行最多 ${EXECUTION_LIMITS.timeoutMs / 1000} 秒（包括等待请求），最多 ${EXECUTION_LIMITS.maxRequests} 次 domeye.request 调用；被请求策略拦截的调用也计数。
可用类型：
${EXECUTE_TYPES}
完整路径、参数和正文结构从 search 取得。路径中的 {参数} 用实际取得的值逐段 encodeURIComponent 后替换。请求保留用户时间窗，响应覆盖另行说明。每次请求先检查 status 和业务 state；读取失败时直接 return 原始响应，仅在成功且所需值确实存在时计算，保留 null。返回超过 24000 字符会附 TRUNCATED 提示；之后用代码减少返回字段、聚合或缩小查询，不能把截断片段当完整结果。
可选 headers 使用小写键，仅保留本次 HTTP 中前缀 x-domeye-result- 下的 state、version、start、end-exclusive、coverage 五项，缺失不补齐。它们描述服务端交付上下文，是否适用于正文结果须按接口来源核对，首末范围不是每条样本的实际窗口。
从参与数据的定义、单位、版本和实际窗口确认能否比较，再计算并 return 要引用的新增合计、比例、差值和选择结果。每个最多／最少都在本题要求的候选范围内计算；不同分组维度分别比较，局部排名不代表总体排名。已有原始值可直接引用；返回结果保留对象、请求窗、覆盖及可比范围，依据不足的项目保持未知。环境没有 Node、fetch 或外部模块。
工具另附每次 HTTP 的请求/范围回执 responses。按各回执的 request、source 和 scope 分别解释各来源；某来源没有覆盖或未确认版本时，不向它借用其他来源的 coverage 或版本。它们不是代码返回字段的自动血缘，组合结果时由代码保留各自归属。requestControl.recoveryStopped 列出的结果族本轮已无法再恢复，按附带说明结束这些目标的取证并答复。
资源两端比较示例：输入和字段组织均为虚构，仅示范完整时点、只有标签和缺值三种情形。真实使用时从各自响应带入原值与身份，条件未齐时返回样本及缺项，齐备时返回计算；不必为纯计算重复 HTTP：
${COMPARISON_EXAMPLE}
请求与排名示例：以下虚构合同的 body 含 state、scope（对象、口径、单位、版本、请求及覆盖）、total 和 series；类别互斥，同端点分桶可加。直接用响应明细生成时段合计，再分别比较类型和时段；总体取原始 total。真实调用以 search 确认的结构为准，自由选择所需运算和返回形状。窗口保留真实端点，确需时长时由代码换算并返回单位：
${CALCULATION_EXAMPLE}`,
      parameters: Type.Object({code:Type.String({minLength:1,pattern:'\\S',description:'返回 JSON 值的 async 箭头函数；return 本题各目标的读取结果或实际算出的比较结果。'})},{additionalProperties:false}),
      call: ({code},signal,observers) => runCode({code,request:(input,options) => request(input,{...options,...observers}),signal})
    }
  ];
  return {
    tools: definitions.map(({call,...definition}) => ({...definition,execute:async (id,params,signal) => {
      const events = [], responses = [];
      const requestControl = () => {
        const state=policy.snapshot();
        // 仅显示现有策略已经确定的本轮恢复终态；不改变请求权限或原始正文。
        const recoveryStopped=Object.entries(state.families)
          .filter(([,family])=>family.conflict && family.refreshUsed)
          .map(([family])=>({family,reason:'recovery_exhausted',action:'answer_unavailable',
            note:'此结果族本轮的发现机会已使用，当前版本冲突仍未解决，无法再恢复这部分查询。停止为恢复这部分结果继续搜索或尝试其他入口，直接简要说明未取得可用于本次回答的结果。旧读数仅是上次读取，不能确认当前有效。其他独立问题目标仍可继续。'}));
        return {
          note:'responses 是本次工具调用内各次 HTTP 的请求/范围回执；scope 仅摘录各自正文，缺失不从其他响应补入。代码结果原样在前一块；回执不自动证明其中任意字段的来源。纯计算没有新的 HTTP 回执。工具失败时仅保留已观察到的回执，不能视为代码成功。成功确认新版本后整题重取；recoveryStopped 所列族则本轮无法恢复。',
          responses,events,restartRequired:state.restartRequired,invalidatedEvidence:state.invalidatedEvidence,
          pendingReplay:state.pendingReplay,unresolvedFailures:state.unresolvedFailures,recoveryStopped
        };
      };
      try {
        const value = await call(params,signal,{
          onPolicyEvent:event => events.push(event),
          onResponse:(event,response) => responses.push(makeSourceReceipt({toolCallId:id,event,response}))
        });
        if (definition.name==='docs' && value?.error) throw Object.assign(new Error(value.error.message),{kind:value.error.kind});
        onEvidence('tool_result',{id,name:definition.name,params,value});
        const content = [{type:'text',text:['search','execute'].includes(definition.name) ? truncateResponse(value) : JSON.stringify(value)}];
        if (definition.name==='execute') content.push({type:'text',text:JSON.stringify({requestControl:requestControl()})});
        return {content,details:value};
      } catch (error) {
        const detail = {kind:error.kind ?? (definition.name==='docs'?'retrieval':'code'),message:error.message};
        onEvidence('tool_error',{id,name:definition.name,params,error:detail});
        // Pi 通过异常标记工具失败；收到的 HTTP 错误没有走到这里，正文仍是普通结果。
        throw new Error(JSON.stringify({error:detail,...(definition.name==='execute'?{requestControl:requestControl()}: {})}));
      }
    }})),
    beginTurn: () => policy.beginTurn(),
    close: async () => docs.close?.()
  };
}
