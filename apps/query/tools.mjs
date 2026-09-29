import { Type } from 'typebox';
import { createDocs } from './docs.mjs';
import { loadSpecDocument, resolveLocalRefs, SPEC_TYPES } from './spec.mjs';
import { createRequest, runCode, EXECUTION_LIMITS, ToolFailure } from './runtime/executor.mjs';
import { createRequestPolicy } from './runtime/request-policy.mjs';
import { makeSourceReceipt } from './runtime/source-receipt.mjs';
import { truncateResponse } from './runtime/truncate.mjs';
import { collectOperations } from './api-operations.mjs';

export const EXECUTE_TYPES = `type QueryValue = string | number | boolean;
declare const domeye: {
  api: Record<string, (params?: Record<string, QueryValue>) => Promise<{status: number; body: unknown; headers?: Record<string, string>}>>;
  request(input: {method: "GET"; path: string; query?: Record<string, QueryValue>}):
    Promise<{status: number; body: unknown; headers?: Record<string, string>}>;
  readResult(toolCallId: string): Promise<unknown>;
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

// 工具说明中的发现与收窄示例；使用 search 的通用规范导航函数。
export const SEARCH_DISCOVERY_EXAMPLE = `async () => {
  const keyword = "资源"; // 替换为本题的业务关键词。
  return Object.entries(spec.paths)
    .filter(([path, {get: op}]) => op &&
      [path, op.summary, op.description].some(text => text?.toLowerCase().includes(keyword.toLowerCase())))
    .map(([path, {get: op}]) => ({method: "GET", path,
      summary: op.summary, description: op.description, deprecated: op.deprecated,
      parameters: op.parameters,
      responseStructure: schemaTools.outline(op.responses?.["200"]?.content?.["application/json"]?.schema)}));
}`;

export const SEARCH_PARAMETERS_EXAMPLE = `async () => {
  const paths = ["/从发现结果选定的完整路径"]; // 可一次读取多个适用候选。
  const contracts = paths.map(path => {
    const op = spec.paths[path]?.get;
    if (!op) throw new Error("接口不存在，请核对发现结果");
    const schema = op.responses?.["200"]?.content?.["application/json"]?.schema;
    return {path, method: "GET", description: op.description, parameters: op.parameters,
      responseDescription: op.responses?.["200"]?.description, schema,
      responseStructure: schemaTools.outline(schema)};
  });
  // 合同较小时一次读齐；过宽时保留完整字段目录，再按需要选原始子树。
  if (JSON.stringify(contracts).length <= 24000) return contracts;
  return contracts.map(({schema, ...catalog}) => ({...catalog, requiresProjection: true}));
}`;

export const SEARCH_PROJECTION_EXAMPLE = `async () => {
  // 按已发现的结构选取本题所需字段；复合字段的原始子树可直接返回。
  const path = "/从发现结果选定的完整路径";
  const schemaPath = ["从目录选定的结构路径"]; // 例如分支路径后接 "properties" 和字段名。
  const status = "200", media = "application/json";
  const op = spec.paths[path]?.get;
  const response = op?.responses?.[status];
  const subtree = schemaTools.select(response?.content?.[media]?.schema, schemaPath);
  return {path, method: "GET", parameters: op.parameters, status, media, responseDescription: response.description,
    schemaPath, schema: subtree};
}`;

export const SEARCH_PROJECTION_GUIDANCE = '按本题指标含义发现接口，返回方法、完整路径、业务摘要、参数与响应结构；含义不清或关键词无命中时，用 docs 确认指标后再选择入口。选定接口后一次取得参数、响应说明和本题需要的结构；完整小合同已返回时，直接取数，不为已知字段再调用 search。schemaTools.outline(schema) 列出直接字段、oneOf/anyOf/allOf 分支及数组元素，每个节点的 schemaPath 均相对于传入的 schema。各分支分别保留，不合并字段或 required，也不自动认定适用分支。目录用于定位，不能替代原合同约束；用 schemaTools.select(schema, schemaPath) 原样读取子树，字段路径接在节点路径后，例如 [...node.schemaPath, "properties", field]。缺失路径明确报错。较宽合同可在同一段代码中选择所需子树；只有信息确实不足才另行补查。优先利用已有相关统计，避免逐个筛选重复取同一汇总。所选子树保留原有描述、单位、时间、可空和复合分支。示例路径须按实际规范调整。返回超过 24000 字符会截断并附 TRUNCATED 提示，截断片段不是完整结构。';

export async function createTools({ apiBaseUrl, specFile, docsConfig, onEvidence = () => {} }) {
  const sourceSpec = await loadSpecDocument(specFile);
  const spec = resolveLocalRefs(sourceSpec);
  const operations = collectOperations(spec);
  const docs = await createDocs(docsConfig);
  const paths = Object.entries(spec.paths).filter(([,item]) => item.get).map(([path]) => path);
  const policy = createRequestPolicy({spec,
    request:createRequest({ baseUrl: apiBaseUrl, paths, onResponse: value => onEvidence('http', value) }),
    onEvidence:value => onEvidence('request_policy',value)
  });
  const request = policy.request;
  const docTexts = new Map();
  // 仅保存本题 execute 的完整返回；每题原有的 20 次工具、单结果 4 MiB 额度仍适用。
  const results = new Map();
  let turn = 0, closed = false;
  const docsForModel = (value,toolCallId) => !Array.isArray(value?.results)?value:{...value,results:value.results.map(item=>{
    if(typeof item.source!=='string' || typeof item.text!=='string')return item;
    const key=JSON.stringify([item.source,item.text]),previous=docTexts.get(key);
    if(previous){const {text,...identity}=item;return {...identity,textReference:previous};}
    docTexts.set(key,{toolCallId,source:item.source});return item;
  })};
  const requestContext = () => ({requiredVersions:policy.requiredVersions(),
    note:'这些路径后续读取须显式传入对应 version；只提示已确认的参数，不证明数据完整或允许重试。未列路径不借用这些版本；发现、冲突和失败仍按原规则处理。'});
  const definitions = [
    {
      name: 'docs', label: '查询业务说明',
      description: '查询 Domeye 业务说明，返回标题、固定版本来源和相关原文，不生成标准答案。query 使用要解释的指标或字段，以及要核实的含义、单位或判断条件。首次解释指标时，读到它的计量对象与限制才算取得说明；命中其他指标或只有接口导航时，围绕缺少的定义继续检索。已有且适用的原文可复用。同一题内完全相同来源和原文以 textReference 指向此前工具调用，沿用该调用原文；新问题重新返回全文。search 的参数结构不替代业务说明；无命中不证明业务不支持，实际范围还需查询接口。',
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
先从下面目录选择本题要调用的操作，一次 api.describe([操作名,...]) 取得所需参数、说明和响应合同。responseState=complete 时，responseContract.schema 与 references 共同给出完整响应合同；遇到 $ref，按完整引用字符串在 references 中找定义，同一类型只保留一份，不需要逐层搜索。只有 responseState=outline 时才按目录收窄；not_declared 表示规范未声明响应结构。
api.schema(操作名, schemaPath) 原样读取需要的响应子树；可以在同一次 search 中读取多个操作及其字段。api.list() 返回完整操作目录。目录和结构不足时，仍可查询 spec 与 schemaTools；各工具都不代替业务定义。
操作目录（来自本次同版合同；标记 deprecated 的入口保留原限制）：
${Object.values(operations).map(op=>`${JSON.stringify(op.operationId)}：${op.summary??op.path}${op.deprecated?'（deprecated）':''}`).join('\n')}
${SEARCH_PROJECTION_GUIDANCE}`,
      parameters: Type.Object({code:Type.String({minLength:1,pattern:'\\S'})},{additionalProperties:false}),
      call: ({code},signal) => runCode({code,spec,sourceSpec,operations,signal})
    },
    {
      name: 'execute', label: '读取并整理数据',
      description: `用 JavaScript 读取业务数据并完成回答需要的计算。code 是返回 JSON 值的 async 箭头函数；可以组合请求，也可以直接用已取得的数值计算。
单次代码执行最多 ${EXECUTION_LIMITS.timeoutMs / 1000} 秒（包括等待请求），操作调用、domeye.request 和 domeye.readResult 合计最多 ${EXECUTION_LIMITS.maxRequests} 次宿主调用；被拦截的调用也计数。
可用类型：
${EXECUTE_TYPES}
优先使用 search 返回的 domeye.api[操作名](params)：参数按合同名称直接传入对象，由调用器检查标量结构并填充路径及查询参数；返回值保持原始 {status,body,headers?}。操作名、参数及响应字段以 api.describe/api.schema 为准。目录、标识和版本等依赖值从前一步实际响应读取，在同一段代码中传给下一步；不把未知字段或目录值猜成常量。原始 domeye.request 继续可用，其完整路径、参数和正文结构同样从 search 取得。
search 附带的 requestContext.requiredVersions 和 execute 回执列明已确认版本，写请求时显式带入对应路径的 version。操作调用不自动补版本或恢复失败。使用 request 时，路径中的 {参数} 用实际取得的值逐段 encodeURIComponent 后替换；所有查询参数传原值，由宿主统一进行 URL 编码，不要预编码查询参数。请求保留用户时间窗，响应覆盖另行说明。每次请求先检查 status 和业务 state；读取失败时直接 return 原始响应，仅在成功且所需值确实存在时计算，保留 null。所需结构已知时，在同一次代码中取数并完成本题要求的计算。返回超过 24000 字符会附 TRUNCATED 提示；不能把片段当完整结果。后续筛选或计算用 await domeye.readResult(resultReference.toolCallId) 读取本题此前 execute 的完整返回，再 return 所需内容，无需重复 HTTP；形状保持该次代码的返回值。复用不更新版本、不证明覆盖完整或业务可比，原请求范围仍以该次回执为准；版本失效、读取故障或进入新问题后不能复用。
可选 headers 使用小写键，仅保留本次 HTTP 中前缀 x-domeye-result- 下的 state、version、start、end-exclusive、coverage 五项，缺失不补齐。它们描述服务端交付上下文，是否适用于正文结果须按接口来源核对，首末范围不是每条样本的实际窗口。
从参与数据的定义、单位、版本和实际窗口确认能否比较，再计算并 return 要引用的新增合计、比例、差值和选择结果。每个最多／最少都在本题要求的候选范围内计算；不同分组维度分别比较，局部排名不代表总体排名。已有原始值可直接引用；返回结果保留对象、请求窗、覆盖及可比范围，依据不足的项目保持未知。环境没有 Node、fetch 或外部模块。
工具另附每次 HTTP 的请求/范围回执 responses。按各回执的 request、source 和 scope 分别解释各来源；某来源没有覆盖或未确认版本时，不向它借用其他来源的 coverage 或版本。它们不是代码返回字段的自动血缘，组合结果时由代码保留各自归属。requestControl.recoveryStopped 列出的结果族本轮已无法再恢复，按附带说明结束这些目标的取证并答复。
资源两端比较示例：输入和字段组织均为虚构，仅示范完整时点、只有标签和缺值三种情形。真实使用时从各自响应带入原值与身份，条件未齐时返回样本及缺项，齐备时返回计算；不必为纯计算重复 HTTP：
${COMPARISON_EXAMPLE}
请求与排名示例：以下虚构合同的 body 含 state、scope（对象、口径、单位、版本、请求及覆盖）、total 和 series；类别互斥，同端点分桶可加。直接用响应明细生成时段合计，再分别比较类型和时段；总体取原始 total。真实调用以 search 确认的结构为准，自由选择所需运算和返回形状。窗口保留真实端点，确需时长时由代码换算并返回单位：
${CALCULATION_EXAMPLE}`,
      parameters: Type.Object({code:Type.String({minLength:1,pattern:'\\S',description:'返回 JSON 值的 async 箭头函数；return 本题各目标的读取结果或实际算出的比较结果。'})},{additionalProperties:false}),
      call: ({code},signal,observers) => runCode({code,operations,request:(input,options) => request(input,{...options,...observers}),readResult:observers.readResult,signal})
    }
  ];
  // 与首轮模型请求重叠准备本地嵌入；失败仍由实际 docs 调用报告，不阻塞其他工具。
  void docs.warmup?.();
  return {
    tools: definitions.map(({call,...definition}) => ({...definition,execute:async (id,params,signal) => {
      const events = [], responses = [], reusedResults = [];
      const executionTurn = turn;
      const assertCurrent = () => {
        if (closed || executionTurn !== turn) throw new ToolFailure('policy', '执行所属问题已结束或会话已关闭。');
      };
      const requestControl = () => {
        const state=policy.snapshot();
        // 仅显示现有策略已经确定的本轮恢复终态；不改变请求权限或原始正文。
        const recoveryStopped=Object.entries(state.families)
          .filter(([,family])=>family.conflict && family.refreshUsed)
          .map(([family])=>({family,reason:'recovery_exhausted',action:'answer_unavailable',
            note:'此结果族本轮的发现机会已使用，当前版本冲突仍未解决，无法再恢复这部分查询。停止为恢复这部分结果继续搜索或尝试其他入口，直接简要说明未取得可用于本次回答的结果。旧读数仅是上次读取，不能确认当前有效。其他独立问题目标仍可继续。'}));
        return {
          note:'responses 是本次工具调用内各次 HTTP 的请求/范围回执；scope 仅摘录各自正文，缺失不从其他响应补入。reusedResults 引用本题此前的完整返回及原 HTTP 回执，不代表新请求，范围说明沿用被引用的工具调用。代码结果原样在前一块；回执不自动证明其中任意字段的来源。纯计算没有新的 HTTP 回执。工具失败时仅保留已观察到的回执，不能视为代码成功。成功确认新版本后整题重取；recoveryStopped 所列族则本轮无法恢复。',
          requiredVersions:policy.requiredVersions(),responses,reusedResults,events,restartRequired:state.restartRequired,invalidatedEvidence:state.invalidatedEvidence,
          pendingReplay:state.pendingReplay,unresolvedFailures:state.unresolvedFailures,recoveryStopped
        };
      };
      try {
        assertCurrent();
        const value = await call(params,signal,{
          readResult: toolCallId => {
            assertCurrent();
            if (typeof toolCallId !== 'string' || !results.has(toolCallId)) throw new ToolFailure('input', '本题没有该 execute 的完整结果；请使用本题回执中的 resultReference.toolCallId。');
            const saved = results.get(toolCallId);
            policy.assertReusable(saved.responseIds);
            const reference = {toolCallId, responseIds: saved.responseIds};
            if (!reusedResults.some(item => item.toolCallId === toolCallId)) reusedResults.push(reference);
            onEvidence('result_reuse', {id, ...reference});
            return JSON.parse(saved.json);
          },
          onPolicyEvent:event => events.push(event),
          onResponse:(event,response) => responses.push(makeSourceReceipt({toolCallId:id,event,response}))
        });
        assertCurrent();
        if (definition.name==='docs' && value?.error) throw Object.assign(new Error(value.error.message),{kind:value.error.kind});
        if (definition.name==='execute') results.set(id, {json:JSON.stringify(value),
          responseIds:[...new Set([...responses.map(response => response.id), ...reusedResults.flatMap(item => item.responseIds)])]});
        onEvidence('tool_result',{id,name:definition.name,params,value});
        const content = [{type:'text',text:['search','execute'].includes(definition.name) ? truncateResponse(value, definition.name==='execute' ? {toolCallId:id} : {}) : JSON.stringify(docsForModel(value,id))}];
        if (definition.name==='search') content.push({type:'text',text:JSON.stringify({requestContext:requestContext()})});
        if (definition.name==='execute') content.push({type:'text',text:JSON.stringify({resultReference:{toolCallId:id},requestControl:requestControl()})});
        return {content,details:value};
      } catch (error) {
        const detail = {kind:error.kind ?? (definition.name==='docs'?'retrieval':'code'),message:error.message};
        onEvidence('tool_error',{id,name:definition.name,params,error:detail});
        // Pi 通过异常标记工具失败；收到的 HTTP 错误没有走到这里，正文仍是普通结果。
        throw new Error(JSON.stringify({error:detail,...(definition.name==='execute'?{requestControl:requestControl()}: {})}));
      }
    }})),
    beginTurn: () => {const state=policy.beginTurn();turn++;docTexts.clear();results.clear();return state;},
    close: async () => {closed=true;results.clear();await docs.close?.();}
  };
}
