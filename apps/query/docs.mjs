import { fork } from 'node:child_process';
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { readFile, access, stat } from 'node:fs/promises';
import { posix, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { CONTRACT_SOURCE_COMMIT } from './source.mjs';
import { runtimePaths } from './runtime/settings.mjs';

const self = fileURLToPath(import.meta.url);
export const DOCS_SOURCE_COMMIT = CONTRACT_SOURCE_COMMIT;
export const DOCS_EMBED_MODEL = 'hf:Qwen/Qwen3-Embedding-0.6B-GGUF/Qwen3-Embedding-0.6B-Q8_0.gguf';
const failure = (kind, message) => ({ error: { kind, message } });
const notReady = detail => new Error(`文档检索环境尚未就绪：${detail}。改写 query 无法解决，请先报告当前不可检索，待环境准备或修复完成后再调用 docs。`);

// 本项目固定语料按 rules/ 分类存放共同规则。只沿已交付片段的直接引用补文。
// 单独导出纯路径选择器，便于验证外部链接、路径边界、去重与数量上限。
export function directRuleLinks(primary, knownPaths) {
  const alreadyIncluded = new Set(primary.map(item => item.path));
  const linked = new Map();
  for (const item of primary) {
    for (const match of item.text.matchAll(/(?<!!)\[[^\]\n]*\]\(\s*(?:<([^>\n]+)>|([^\s)]+))\s*\)/g)) {
      const href = match[1] ?? match[2];
      if (/^(?:[a-z][a-z0-9+.-]*:|\/|#)/i.test(href)) continue;
      let relative;
      try { relative = decodeURIComponent(href.split('#')[0]); } catch { continue; }
      if (/^(?:[a-z][a-z0-9+.-]*:|\/)/i.test(relative) || /[\\\0?]/.test(relative)) continue;
      const path = posix.normalize(posix.join(posix.dirname(item.path), relative));
      if (!path.startsWith('docs/usage/rules/') || !path.endsWith('.md') || alreadyIncluded.has(path)) continue;
      if (!knownPaths.has(path)) throw new Error(`引用的规则文档不在固定清单：${path}`);
      if (!linked.has(path)) {
        if (linked.size >= 2) continue;
        linked.set(path, { path, relatedTo: [] });
      }
      const relation = linked.get(path).relatedTo;
      if (!relation.includes(item.source)) relation.push(item.source);
    }
  }
  return [...linked.values()];
}

async function requirePreparedFile(path, description) {
  try { await access(path, constants.R_OK); }
  catch { throw notReady(description); }
}

/**
 * 返回 docs({query}, signal)；docs.close() 取消尚未完成的调用。
 * QMD、索引和模型从已准备的宿主目录读取。同一检索器复用隔离进程，串行执行查询。
 * warmup() 仅准备本地嵌入上下文；取消、超时或失败后淘汰进程，不自动重放请求。
 */
export async function createDocs(config = {}) {
  const paths = runtimePaths();
  const runtimeDir = resolve(config.runtimeDir ?? paths.runtimeDir);
  const settings = {
    runtimeDir,
    corpusDir: resolve(config.corpusDir ?? paths.corpusDir),
    manifestPath: resolve(config.manifestPath ?? paths.manifestPath),
    dbPath: resolve(config.dbPath ?? resolve(runtimeDir, 'index.sqlite')),
    timeoutMs: config.timeoutMs ?? 45_000,
    maxResults: config.maxResults ?? 3,
    maxLines: config.maxLines ?? 80,
    minScore: config.minScore ?? 0.45,
  };
  for (const key of ['timeoutMs', 'maxResults', 'maxLines']) {
    if (!Number.isSafeInteger(settings[key]) || settings[key] < 1) throw new TypeError(`${key} 必须为正整数。`);
  }
  if (!Number.isFinite(settings.minScore) || settings.minScore < -1 || settings.minScore > 1) {
    throw new TypeError('minScore 必须为 -1 到 1 之间的向量相似度。');
  }
  const queue = [];
  let closed = false, worker, active, sequence = 0;
  const runtimeEnv = Object.fromEntries(
    ['PATH', 'HOME', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TMPDIR', 'TMP', 'TEMP', 'SystemRoot']
      .filter(name => process.env[name] !== undefined)
      .map(name => [name, process.env[name]]),
  );

  const finish = (job, value) => {
    if (job.settled) return;
    job.settled = true;
    clearTimeout(job.timer);
    job.signal?.removeEventListener('abort', job.cancel);
    job.resolve(value);
  };
  const retire = (owner, abort = false) => {
    if (owner.retiring) return;
    owner.retiring = true;
    owner.killTimer = setTimeout(() => owner.child.kill('SIGKILL'), 500);
    owner.killTimer.unref();
    if (abort || !owner.child.connected) owner.child.kill('SIGTERM');
    else owner.child.send({type:'close'}, error => {if(error)owner.child.kill('SIGTERM');});
  };
  const stop = (job, message) => {
    if (job.settled) return;
    const value = failure('retrieval', message);
    if (active === job && worker) {
      job.terminal = value;
      retire(worker, true);
    } else finish(job, value);
  };
  const spawn = () => {
    const child = fork(self, ['--qmd-worker'], {
      execArgv: [], stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      env: {...runtimeEnv,
        XDG_CACHE_HOME: resolve(runtimeDir, 'cache'),
        XDG_CONFIG_HOME: resolve(runtimeDir, 'config'),
        QMD_EMBED_MODEL: DOCS_EMBED_MODEL, QMD_EMBED_PARALLELISM: '1',
        ...(process.platform === 'darwin' ? {GGML_METAL_NO_RESIDENCY:'1'} : {})}
    });
    let resolveDone;
    const owner = {child,stderr:'',retiring:false,done:new Promise(done=>{resolveDone=done;})};
    child.stderr.on('data', data => {owner.stderr=(owner.stderr+data.toString()).slice(-1600);});
    child.on('error', error => {owner.error=`无法启动或通信 QMD：${error.message}`;});
    child.on('message', message => {
      if (worker!==owner || !active || message?.type!=='result' || message.id!==active.id || active.terminal) return;
      if (message.value?.error) {
        active.terminal = message.value;
        retire(owner);
        return;
      }
      const job=active;active=undefined;
      finish(job,message.value);
      pump();
    });
    child.on('close', (code, exitSignal) => {
      clearTimeout(owner.killTimer);
      if (worker===owner) {
        worker=undefined;
        if (active) {
          const job=active;active=undefined;
          finish(job,job.terminal ?? failure('retrieval', owner.error ??
            `QMD 未完成检索（${exitSignal ?? code}）。${owner.stderr.trim()}`));
        }
      }
      resolveDone();
      pump();
    });
    return owner;
  };
  function pump() {
    if (closed || active || worker?.retiring) return;
    while(queue[0]?.settled)queue.shift();
    const job=queue.shift();
    if (!job) return;
    active=job;
    try {worker ??= spawn();}
    catch(error){active=undefined;finish(job,failure('retrieval',`无法启动 QMD：${error.message}`));pump();return;}
    const owner=worker;
    try {
      owner.child.send({type:job.type,id:job.id,settings,query:job.query}, error => {
        if(error && active===job)stop(job,`无法向 QMD 发送请求：${error.message}`);
      });
    } catch(error){stop(job,`无法向 QMD 发送请求：${error.message}`);}
  }
  const request = (type, query, signal) => {
    if (closed) return Promise.resolve(failure('retrieval','文档检索器已关闭。'));
    if (signal?.aborted) return Promise.resolve(failure('retrieval','文档检索已取消。'));
    return new Promise(resolveResult=>{
      const job={id:++sequence,type,query,signal,resolve:resolveResult,settled:false};
      job.cancel=()=>stop(job,'文档检索已取消。');
      // 排队也计入原有总时限；取消排队项不终止其他查询。
      job.timer=setTimeout(()=>stop(job,`文档检索超过 ${settings.timeoutMs} 毫秒，已终止。`),settings.timeoutMs);
      signal?.addEventListener('abort',job.cancel,{once:true});
      queue.push(job);
      if(signal?.aborted)job.cancel();
      pump();
    });
  };
  const docs = async (input, signal) => {
    if (!input || typeof input !== 'object' || Array.isArray(input) ||
        Object.keys(input).some(key => key !== 'query') ||
        typeof input.query !== 'string' || !input.query.trim() || input.query.length > 4000) {
      return failure('input', 'docs 仅接收非空 query 字符串，最长 4000 字符。');
    }
    return request('query',input.query.trim(),signal);
  };
  docs.warmup = signal => request('warmup',undefined,signal);
  docs.close = async () => {
    closed=true;
    for(const job of queue)finish(job,failure('retrieval','文档检索器已关闭，当前检索已取消。'));
    queue.length=0;
    if (active) active.terminal=failure('retrieval','文档检索器已关闭，当前检索已取消。');
    if (worker) {
      const owner=worker;
      retire(owner,Boolean(active));
      await owner.done;
    }
  };
  return docs;
}

async function retrieve({ settings, query, type }, getStore) {
  const { runtimeDir, corpusDir, manifestPath, dbPath, maxResults, maxLines, minScore } = settings;
  await requirePreparedFile(dbPath, 'QMD 索引尚未建立或不可读取');
  // 检索时缺模型应明确失败，不在一次工具调用里偷偷启动长时间下载。
  await requirePreparedFile(resolve(runtimeDir, 'cache/qmd/models/hf_Qwen_Qwen3-Embedding-0.6B-Q8_0.gguf'), 'Qwen3 嵌入模型尚未准备完成或不可读取');
  let manifest;
  try { manifest = JSON.parse(await readFile(manifestPath, 'utf8')); }
  catch { throw notReady('文档来源清单缺失、不可读取或格式无效'); }
  if (manifest.sourceCommit !== DOCS_SOURCE_COMMIT) throw notReady('文档清单提交与工具合同不一致，需要准备同版文档');
  if (!Array.isArray(manifest.files) || !manifest.files.length) throw notReady('文档来源清单没有可用文件');
  const files = new Map(manifest.files.map(file => [file.path, file]));
  const store = await getStore({runtimeDir,dbPath});
  // QMD SDK 默认健康检查模型可能不是自定义模型，显式检查本次 Qwen3 索引。
  const health = store.internal.getStatus(DOCS_EMBED_MODEL);
  if (!health.hasVectorIndex || health.needsEmbedding !== 0 || health.totalDocuments !== files.size) {
    throw notReady('向量索引未完成嵌入或文档数与固定清单不一致');
  }
  if (type==='warmup') {
    // QMD 2.8.3 的嵌入上下文初始化；不调用生成模型、不查询业务数据。
    await store.internal.llm.ensureEmbedContext();
    return {ready:true};
  }
  const hits = await store.searchVector(query, { limit: maxResults, collection: 'domeye' });
  // 非空且完整的向量集合应有候选；QMD 将部分嵌入错误转换为 null，不能冒充无命中。
  if (!hits.length && files.size) throw new Error('QMD 未产生向量候选，请检查嵌入模型和索引。');
  if (hits.some(hit => !Number.isFinite(hit.score))) throw new Error('QMD 返回了无效相似度。');
  // 向量容易遗漏精确术语。复用 QMD 的关键词检索补充原查询及其词组，
  // 不使用领域词表、答案规则或新的模型；片段数量与分数门槛保持不变。
  const lexicalQueries = [...new Set([query, ...query.split(/[\s,，。;；:：!?！？]+/u)]
    .filter(term => term.length >= 2))].slice(0, 8);
  const lexical = new Map();
  for (const term of lexicalQueries) {
    const matches = await store.searchLex(term, { limit: maxResults, collection: 'domeye' });
    for (const hit of matches) {
      if (!Number.isFinite(hit.score)) throw new Error('QMD 返回了无效关键词分数。');
      if (hit.score < minScore) continue;
      const previous = lexical.get(hit.filepath);
      if (!previous || hit.score > previous.score) lexical.set(hit.filepath, { ...hit, matchedTerm: term });
    }
  }
  // 关键词与向量分数不混排：先给精确术语命中的原文，再补向量候选。
  const selected = new Map();
  for (const hit of [...lexical.values()].sort((a, b) => b.score - a.score)
    .concat(hits.filter(hit => hit.score >= minScore))) {
    if (!selected.has(hit.filepath) && selected.size < maxResults) selected.set(hit.filepath, hit);
  }
  const results = [], primary = [];
  async function original(path) {
    const source = files.get(path);
    const localPath = resolve(corpusDir, path);
    if (!source || !localPath.startsWith(corpusDir + sep)) throw new Error('文档未包含在固定清单中。');
    let raw;
    try { raw = await readFile(localPath, 'utf8'); }
    catch { throw notReady(`文档的固定原文不可读取：${path}`); }
    if (createHash('sha256').update(raw).digest('hex') !== source.sha256) throw new Error(`原文已变化：${path}`);
    const indexed = await store.getDocumentBody('qmd://domeye/' + path);
    if (indexed !== raw) throw new Error(`索引与固定原文不一致：${path}`);
    const lines = raw.split('\n');
    if (lines.at(-1) === '') lines.pop();
    return { raw, lines };
  }
  async function excerpt(path, title, lines, start, end) {
    const text = await store.getDocumentBody('qmd://domeye/' + path, { fromLine: start + 1, maxLines: end - start });
    if (!text || text !== lines.slice(start, end).join('\n')) throw new Error(`原文行区间校验失败：${path}`);
    return { title, source: `domeye@${DOCS_SOURCE_COMMIT}:${path}:L${start + 1}-L${end}`, text };
  }
  for (const hit of selected.values()) {
    const prefix = 'qmd://domeye/';
    if (!hit.filepath.startsWith(prefix)) throw new Error('QMD 返回了范围之外的文档。');
    const path = hit.filepath.slice(prefix.length);
    const { raw, lines } = await original(path);
    const chunkPos = hit.matchedTerm !== undefined
      ? Math.max(0, raw.toLowerCase().indexOf(hit.matchedTerm.toLowerCase())) : hit.chunkPos;
    if (!Number.isSafeInteger(chunkPos) || chunkPos < 0 || chunkPos > raw.length) {
      throw new Error(`索引片段位置无效：${path}`);
    }
    const chunkLine = raw.slice(0, chunkPos).split('\n').length - 1;
    // 命中末段时向前补足已有行预算，避免略超上限就丢掉同篇前部定义。
    const start = Math.max(0, Math.min(chunkLine - 3, lines.length - maxLines));
    const end = Math.min(lines.length, start + maxLines);
    const result = await excerpt(path, hit.title, lines, start, end);
    results.push(result); primary.push({ path, source: result.source, text: result.text });
  }
  // 不遍历补充结果中的链接；保留 QMD 排名结果，并最多追加两篇原文。
  for (const link of directRuleLinks(primary, files)) {
    const { raw, lines } = await original(link.path);
    const title = /^#\s+(.+)$/m.exec(raw)?.[1] ?? link.path;
    const result = await excerpt(link.path, title, lines, 0, Math.min(lines.length, maxLines, 80));
    results.push({ ...result, relatedTo: link.relatedTo });
  }
  return { results };
}

if (process.argv[2] === '--qmd-worker' && process.send) {
  let store, identity;
  const getStore = async ({runtimeDir,dbPath}) => {
    const info=await stat(dbPath),current=`${info.dev}:${info.ino}`;
    // 原子替换索引后重新打开；不让常驻连接继续消费已移走的旧文件。
    if(store && identity!==current){await store.close();store=undefined;}
    if(!store){
      try {
        const sdkUrl=pathToFileURL(resolve(runtimeDir,'node_modules/@tobilu/qmd/dist/index.js'));
        const {createStore}=await import(sdkUrl.href);
        store=await createStore({dbPath});identity=current;
      } catch {throw notReady('QMD 依赖不可用或索引无法打开');}
    }
    return store;
  };
  process.on('message',async message=>{
    if(message?.type==='close'){
      try{await store?.close();process.exit(0);}catch{process.exit(1);}
    }
    let value;
    try{value=await retrieve(message,getStore);}
    catch(error){value=failure('retrieval',`文档检索失败：${error.message}`);}
    if(process.connected)process.send({type:'result',id:message.id,value});
  });
  // 父进程退出后不遗留常驻模型；正常 close 优先显式释放 SDK。
  process.once('disconnect',()=>process.exit(0));
}
