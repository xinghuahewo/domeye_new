import { createServer } from 'node:http';
import { readFile, readdir, mkdir, open, realpath } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createDomeyeAgent, loadModelConfig } from './agent.mjs';
import { PROJECT_DATASETS, selectDataset, publicDataset } from './datasets.mjs';
import { runtimePaths, chatAddress } from './runtime/settings.mjs';

const appDir = dirname(fileURLToPath(import.meta.url));
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const toolLabels = { docs: '查阅业务说明', search: '查找接口', execute: '读取与整理数据' };
const statuses = new Set(['running', 'completed', 'cancelled', 'failed']);
const assets = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/app.css', ['app.css', 'text/css; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
]);
class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

// 只向页面提供问题、回答和状态；原始取证继续由已有 Agent 保存在宿主。
export async function createChatServer({ modelConfig, apiBaseUrl,
  historyDir = runtimePaths().historyDir, host = '127.0.0.1', publicOrigin,
  createAgent = createDomeyeAgent } = {}) {
  const address = chatAddress(host, publicOrigin);
  if (!modelConfig || typeof modelConfig.apiKey !== 'string' || !modelConfig.apiKey) throw new Error('须提供有效的宿主模型配置。');
  await mkdir(historyDir, { recursive: true, mode: 0o700 });
  const historyRoot = await realpath(historyDir);
  const clean = value => typeof value === 'string' ? value.split(modelConfig.apiKey).join('[已隐藏凭据]') : '';
  const failureReason = turn => turn?.status === 'failed' && /^402\s*:/.test(turn.error ?? '')
    ? '模型服务余额不足，本次回答未完成。补充 DeepSeek 额度后再试。' : '';
  const publicTurn = turn => ({
    question: clean(turn?.question), answer: clean(turn?.answer),
    status: statuses.has(turn?.status) ? turn.status : 'failed',
    failureReason: failureReason(turn),
    startedAt: clean(turn?.started_at), completedAt: clean(turn?.completed_at),
  });
  let agent, selectedDataset, changing = false, changePromise, activeRun, shuttingDown = false, closePromise, origin;
  const busy = () => changing || Boolean(activeRun);
  const state = () => ({ id: agent?.id ?? null, dataset:publicDataset(selectedDataset), busy: busy(), turns: (agent?.turns ?? []).map(publicTurn) });

  function publish(event) {
    const run = activeRun;
    if (!run || run.response.destroyed || run.finished) return;
    if (run.response.writableLength > 1024 * 1024) { run.agent.stop(); run.response.destroy(); return; }
    run.response.write(JSON.stringify(event) + '\n');
  }
  function textDelta(delta, flush = false) {
    const run = activeRun;
    if (!run) return;
    run.pendingText = clean(run.pendingText + delta);
    // 留下可能跨流片段的凭据前缀，防止分片绕过已知凭据脱敏。
    const count = flush ? run.pendingText.length : Math.max(0, run.pendingText.length - modelConfig.apiKey.length + 1);
    if (count) { publish({ type: 'text', text: run.pendingText.slice(0, count) }); run.pendingText = run.pendingText.slice(count); }
  }
  function onEvent(event) {
    if (event.type === 'answer_final') {
      textDelta(event.text);
    } else if (event.type === 'tool_execution_start' || event.type === 'tool_execution_end') {
      const label = toolLabels[event.toolName];
      if (label) publish({ type: 'tool', label, state: event.type === 'tool_execution_start' ? 'running' : event.isError ? 'failed' : 'done' });
    }
  }
  function newSession(datasetId) {
    if (busy()) throw new HttpError(409, '当前回答或会话切换尚未结束。');
    let dataset;
    try { dataset = selectDataset(datasetId, datasetId === 'three-day' ? undefined : apiBaseUrl); }
    catch { throw new HttpError(400, '请选择已登记的本项目结果批次。'); }
    changing = true;
    changePromise = (async () => {
      try {
        const previous = agent; agent = undefined;
        await previous?.close();
        if (shuttingDown) throw new HttpError(503, '问数服务正在关闭。');
        const created = await createAgent({ modelConfig, datasetId:dataset.id, apiBaseUrl:dataset.apiBaseUrl, historyDir: historyRoot, onEvent });
        if (shuttingDown || !uuid.test(created.id)) {
          await created.close();
          throw new HttpError(503, '会话没有启动，问数服务正在关闭或会话标识无效。');
        }
        agent = created;
        selectedDataset = dataset;
        return state();
      } finally { changing = false; }
    })();
    return changePromise;
  }
  async function history(id) {
    if (!uuid.test(id)) throw new HttpError(404, '没有找到这份会话记录。');
    let file;
    try {
      file = await open(join(historyRoot, id + '.json'), constants.O_RDONLY | constants.O_NOFOLLOW);
      const info = await file.stat();
      if (!info.isFile() || info.size > 32 * 1024 * 1024) throw new HttpError(404, '这份会话记录暂时无法打开。');
      const value = JSON.parse(await file.readFile('utf8'));
      if (value.id !== id || !Array.isArray(value.turns)) throw new Error('记录格式不符。');
      // 旧记录没有批次标识时保持未标注，不能按当前默认值重写其来源。
      const dataset = value.dataset && typeof value.dataset.id === 'string' && typeof value.dataset.label === 'string'
        ? {id:clean(value.dataset.id),label:clean(value.dataset.label),description:clean(value.dataset.description)} : null;
      return { id, readOnly: true, dataset, turns: value.turns.map(publicTurn), updatedAt: info.mtime.toISOString() };
    } catch (error) {
      if (error instanceof HttpError) throw error;
      throw new HttpError(404, '没有找到可读取的会话记录。');
    } finally { await file?.close(); }
  }
  async function listHistory() {
    const names = (await readdir(historyRoot)).filter(name => name.endsWith('.json') && uuid.test(name.slice(0, -5)));
    const records = [];
    for (const name of names) {
      try {
        const item = await history(name.slice(0, -5));
        if (item.turns.length) records.push({ id: item.id, title: item.turns[0].question.slice(0, 70),
          dataset:item.dataset, updatedAt: item.updatedAt, count: item.turns.length, status: item.turns.at(-1).status });
      } catch { /* 不向页面泄露无效文件或路径。 */ }
    }
    return records.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)).slice(0, 80);
  }
  async function body(req) {
    if (!/^application\/json(?:\s*;|$)/i.test(req.headers['content-type'] ?? '')) throw new HttpError(415, '请求须使用 JSON。');
    let size = 0; const chunks = [];
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 24_000) throw new HttpError(413, '问题太长，请精简后再发送。');
      chunks.push(chunk);
    }
    try {
      const value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
      return value;
    } catch { throw new HttpError(400, '请求内容不是有效的 JSON 对象。'); }
  }
  function checkKeys(value, keys) {
    if (Object.keys(value).some(key => !keys.includes(key))) throw new HttpError(400, '请求包含未支持的字段。');
  }
  function current(id) {
    if (!agent || id !== agent.id) throw new HttpError(409, '当前会话已变化，请刷新后继续。');
  }
  async function chat(req, res, input) {
    checkKeys(input, ['sessionId', 'question']);
    if (typeof input.question !== 'string' || !input.question.trim() || input.question.length > 4000) throw new HttpError(400, '请输入不超过 4000 字的问题。');
    if (busy()) throw new HttpError(409, '上一条回答尚未结束，请等待或停止。');
    current(input.sessionId);
    const run = { agent, response: res, pendingText: '', finished: false, promise: null };
    activeRun = run;
    res.writeHead(200, { 'content-type': 'application/x-ndjson; charset=utf-8', 'x-accel-buffering': 'no' });
    res.flushHeaders();
    const disconnect = () => { if (!run.finished && !res.writableEnded) run.agent.stop(); };
    res.on('close', disconnect);
    publish({ type: 'start', sessionId: agent.id });
    run.promise = (async () => {
      try {
        const turn = await run.agent.ask(input.question.trim());
        textDelta('', true);
        publish({ type: 'done', sessionId: run.agent.id, turn: publicTurn(turn) });
      } catch {
        publish({ type: 'error', message: '本次回答未完成。可以重新提问，或查看已保存的会话。' });
      } finally {
        run.finished = true;
        res.off('close', disconnect);
        if (activeRun === run) activeRun = undefined;
        if (!res.destroyed) res.end();
      }
    })();
    await run.promise;
  }

  const server = createServer((req, res) => { void route(req, res); });
  server.headersTimeout = 10_000;
  server.requestTimeout = 15_000;
  server.keepAliveTimeout = 5000;
  server.maxConnections = 16;
  async function route(req, res) {
    res.setHeader('cache-control', 'no-store');
    res.setHeader('x-content-type-options', 'nosniff');
    res.setHeader('referrer-policy', 'no-referrer');
    res.setHeader('content-security-policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'");
    const json = (status, value) => { res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(value)); };
    try {
      if (shuttingDown) throw new HttpError(503, '问数服务正在关闭。');
      if (req.headers.host !== new URL(origin).host) throw new HttpError(403, '请从配置的问数地址打开页面。');
      if (req.headers.origin && req.headers.origin !== origin) throw new HttpError(403, '仅接受本页面发起的请求。');
      if (req.method === 'POST' && req.headers.origin !== origin) throw new HttpError(403, '仅接受本页面发起的请求。');
      const url = new URL(req.url, origin);
      if (req.method === 'GET' && assets.has(url.pathname)) {
        const [file, contentType] = assets.get(url.pathname);
        const content = await readFile(join(appDir, 'web', file));
        res.writeHead(200, { 'content-type': contentType }); res.end(content); return;
      }
      if (req.method === 'GET' && url.pathname === '/api/healthz') return json(200, { status:'ok', service:'domeye-query', busy:busy() });
      if (req.method === 'GET' && url.pathname === '/api/session') return json(200, state());
      if (req.method === 'GET' && url.pathname === '/api/datasets') return json(200, {datasets:PROJECT_DATASETS.map(publicDataset)});
      if (req.method === 'GET' && url.pathname === '/api/history') return json(200, { records: await listHistory() });
      if (req.method === 'GET' && url.pathname.startsWith('/api/history/')) return json(200, await history(url.pathname.slice('/api/history/'.length)));
      if (req.method === 'POST' && url.pathname === '/api/session') {
        const input = await body(req); checkKeys(input, ['datasetId']);
        const result = await newSession(input.datasetId); result.busy = false;
        return json(200, result);
      }
      if (req.method === 'POST' && url.pathname === '/api/stop') {
        const input = await body(req); checkKeys(input, ['sessionId']); current(input.sessionId);
        agent.stop(); return json(200, { stopping: Boolean(activeRun) });
      }
      if (req.method === 'POST' && url.pathname === '/api/chat') return await chat(req, res, await body(req));
      throw new HttpError(404, '没有找到这个页面或入口。');
    } catch (error) {
      if (res.destroyed) return;
      if (res.headersSent) res.end();
      else json(error.status ?? 500, { error: error instanceof HttpError ? error.message : '问数服务暂时不可用，请检查宿主运行状态。' });
    }
  }
  return {
    server,
    async listen(port = 28684) {
      if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('端口须为有效整数。');
      await new Promise((done, reject) => {
        server.once('error', reject);
        server.listen(port, address.host, () => { server.off('error', reject); origin = address.publicOrigin ?? `http://127.0.0.1:${server.address().port}`; done(); });
      });
      return origin;
    },
    close() {
      return closePromise ??= (async () => {
        shuttingDown = true;
        activeRun?.agent.stop();
        const errors = [];
        try { await changePromise; } catch (error) { if (error.status !== 503) errors.push(error); }
        try { await activeRun?.promise; await agent?.close(); } catch (error) { errors.push(error); }
        agent = undefined;
        if (server.listening) await new Promise(done => { server.close(done); server.closeAllConnections(); });
        if (errors.length) throw new Error('聊天已关闭，Agent 收尾未完整完成。');
      })();
    },
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const modelConfig = await loadModelConfig(process.env.DOMEYE_MODEL_CONFIG);
    const chat = await createChatServer({ modelConfig,
      apiBaseUrl: process.env.DOMEYE_QUERY_API_BASE_URL,
      historyDir: runtimePaths().historyDir,
      host: process.env.DOMEYE_CHAT_HOST,
      publicOrigin: process.env.DOMEYE_CHAT_ORIGIN });
    const address = await chat.listen(Number(process.env.DOMEYE_CHAT_PORT ?? 28684));
    console.log(`Domeye 问数：${address}`);
    let closing = false;
    const close = async () => {
      if (closing) return; closing = true;
      try { await chat.close(); } catch { console.error('关闭时部分 Agent 收尾未完成。'); process.exitCode = 1; }
    };
    process.on('SIGINT', close); process.on('SIGTERM', close);
  } catch { console.error('问数服务未启动。请检查模型配置、依赖、历史目录或端口；配置内容不会输出。'); process.exitCode = 1; }
}
