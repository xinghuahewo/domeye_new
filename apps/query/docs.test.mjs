import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rename, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createDocs, DOCS_SOURCE_COMMIT } from './docs.mjs';

// 经过实际 docs 子进程与原文校验；只替换外部 QMD SDK，不连接业务数据或模型。
async function fixture({ documents, vector, lexical = {}, control = {}, config = {} }, run) {
  const root = await mkdtemp(resolve(tmpdir(), 'domeye-docs-'));
  const runtimeDir = resolve(root, 'runtime');
  const corpusDir = resolve(root, 'corpus');
  const sdk = resolve(runtimeDir, 'node_modules/@tobilu/qmd/dist');
  const manifestPath = resolve(root, 'manifest.json');
  let docs;
  try {
    await mkdir(sdk, { recursive: true });
    await mkdir(resolve(runtimeDir, 'cache/qmd/models'), { recursive: true });
    await writeFile(resolve(runtimeDir, 'index.sqlite'), '人工索引占位');
    await writeFile(resolve(runtimeDir, 'cache/qmd/models/hf_Qwen_Qwen3-Embedding-0.6B-Q8_0.gguf'), '人工模型占位');
    const files = [];
    for (const [path, body] of Object.entries(documents)) {
      const target = resolve(corpusDir, path);
      await mkdir(resolve(target, '..'), { recursive: true });
      await writeFile(target, body);
      files.push({ path, sha256: createHash('sha256').update(body).digest('hex') });
    }
    await writeFile(manifestPath, JSON.stringify({ sourceCommit: DOCS_SOURCE_COMMIT, files }));
    await writeFile(resolve(sdk, 'package.json'), JSON.stringify({ type: 'module' }));
    await writeFile(resolve(sdk, 'input.json'), JSON.stringify({ documents, vector, lexical, control }));
    await writeFile(resolve(sdk, 'index.js'), `
      import {readFile,appendFile} from 'node:fs/promises';
      import {existsSync} from 'node:fs';
      const input=JSON.parse(await readFile(new URL('./input.json',import.meta.url),'utf8'));
      const trace=(type,extra={})=>appendFile(new URL('./trace.jsonl',import.meta.url),JSON.stringify({type,pid:process.pid,...extra})+'\\n');
      export async function createStore(){
        await trace('open');
        return {
          internal:{getStatus:()=>({hasVectorIndex:true,needsEmbedding:0,totalDocuments:Object.keys(input.documents).length}),
            llm:{ensureEmbedContext:async()=>{await trace('warm');}}},
          searchVector:async query=>{
            await trace('query',{query});
            if(input.control.block===query)Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,30_000);
            if(input.control.crash===query)process.exit(12);
            if(input.control.gate===query)while(!existsSync(new URL('./release',import.meta.url)))await new Promise(r=>setTimeout(r,5));
            return input.control.empty===query?[]:input.vector;
          },
          searchLex:async query=>input.lexical[query] ?? [],
          getDocumentBody:async(uri,options)=>{
            const body=input.documents[uri.slice('qmd://domeye/'.length)];
            if(!options)return body;
            const lines=body.split('\\n');
            if(lines.at(-1)==='')lines.pop();
            return lines.slice(options.fromLine-1,options.fromLine-1+options.maxLines).join('\\n');
          },
          close:async()=>{await trace('close');}
        };
      }
    `);
    docs = await createDocs({ runtimeDir, corpusDir, manifestPath,...config });
    await run(docs,{root,runtimeDir,corpusDir,manifestPath,
      release:()=>writeFile(resolve(sdk,'release'),'人工查询可以继续'),
      trace:async()=>{
        // 子进程可能刚创建文件或尚未写完末行，只解析已经结束的记录。
        try{return (await readFile(resolve(sdk,'trace.jsonl'),'utf8')).split('\n').slice(0,-1).map(JSON.parse);}
        catch(error){if(error.code==='ENOENT')return [];throw error;}
      }});
  } finally {
    await docs?.close();
    await rm(root, { recursive: true, force: true });
  }
}

const hit = (path, score, extra = {}) => ({ filepath: 'qmd://domeye/' + path, title: path, score, ...extra });
const simple=()=>({documents:{'docs/usage/metrics/canary.md':'# 人工定义\n'},vector:[hit('docs/usage/metrics/canary.md',0.8,{chunkPos:0})]});
async function waitFor(check){
  const deadline=performance.now()+5000;
  while(performance.now()<deadline){if(await check())return;await new Promise(done=>setTimeout(done,5));}
  assert.fail('人工检索没有到达预期阶段');
}
const gone=pid=>{try{process.kill(pid,0);return false;}catch(error){if(error.code==='ESRCH')return true;throw error;}};

test('同一检索器的连续查询复用一个 QMD 进程和 store，关闭后释放',async()=>{
  const path='docs/usage/metrics/canary.md';
  await fixture({documents:{[path]:'# 人工定义\n'},vector:[hit(path,0.8,{chunkPos:0})]},async(docs,{trace})=>{
    const first=await docs({query:'第一次查询'});
    const second=await docs({query:'第二次查询'});
    assert.equal(first.error,undefined);assert.deepEqual(second,first);
    const events=await trace();
    assert.equal(events.filter(e=>e.type==='open').length,1,'每次检索仍重新加载 QMD');
    assert.equal(new Set(events.filter(e=>e.type==='query').map(e=>e.pid)).size,1);
    assert.equal(events.filter(e=>e.type==='close').length,0,'下一次查询前已释放模型');
    await docs.close();
    assert.equal((await trace()).filter(e=>e.type==='close').length,1);
  });
});

test('预热只准备嵌入上下文，不执行查询；随后继续复用同一进程',async()=>{
  await fixture(simple(),async(docs,{trace})=>{
    assert.deepEqual(await docs.warmup(),{ready:true});
    assert.deepEqual((await trace()).map(e=>e.type),['open','warm']);
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    assert.equal((await trace()).filter(e=>e.type==='open').length,1);
  });
});

test('取消原生阻塞查询会结束进程；已排队的另一请求冷启且不重放旧请求',async()=>{
  await fixture({...simple(),control:{block:'原生阻塞'}},async(docs,{trace})=>{
    const controller=new AbortController();
    const first=docs({query:'原生阻塞'},controller.signal);
    await waitFor(async()=>(await trace()).some(e=>e.type==='query'));
    const pid=(await trace()).find(e=>e.type==='open').pid;
    const next=docs({query:'后续请求'});
    controller.abort();
    assert.match((await first).error.message,/取消/);
    assert.equal(gone(pid),true);
    assert.equal((await next).error,undefined);
    const events=await trace();
    assert.equal(events.filter(e=>e.type==='open').length,2);
    assert.deepEqual(events.filter(e=>e.type==='query').map(e=>e.query),['原生阻塞','后续请求']);
  });
});

test('取消排队项不杀死当前查询，也不会把后续结果错配给取消项',async()=>{
  await fixture({...simple(),control:{gate:'等待放行'}},async(docs,{trace,release})=>{
    const first=docs({query:'等待放行'});
    await waitFor(async()=>(await trace()).some(e=>e.type==='query'));
    const controller=new AbortController(),cancelled=docs({query:'排队取消'},controller.signal);
    const last=docs({query:'后续请求'});
    controller.abort();
    assert.match((await cancelled).error.message,/取消/);
    assert.equal((await trace()).filter(e=>e.type==='open').length,1);
    await release();
    assert.equal((await first).error,undefined);assert.equal((await last).error,undefined);
    assert.deepEqual((await trace()).filter(e=>e.type==='query').map(e=>e.query),['等待放行','后续请求']);
  });
});

test('超时终止原生阻塞，下一次检索可以恢复；超时不是空结果',async()=>{
  await fixture({...simple(),control:{block:'原生阻塞'},config:{timeoutMs:1500}},async(docs,{trace})=>{
    const result=await docs({query:'原生阻塞'});
    assert.match(result.error.message,/超过 1500/);assert.equal(result.results,undefined);
    const pid=(await trace()).find(e=>e.type==='open').pid;assert.equal(gone(pid),true);
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    assert.equal((await trace()).filter(e=>e.type==='open').length,2);
  });
});

test('进程崩溃只令当前请求失败，后续冷启，不自动重试同一查询',async()=>{
  await fixture({...simple(),control:{crash:'模拟崩溃'}},async(docs,{trace})=>{
    const result=await docs({query:'模拟崩溃'});assert.match(result.error.message,/12/);
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    assert.deepEqual((await trace()).filter(e=>e.type==='query').map(e=>e.query),['模拟崩溃','正常查询']);
    assert.equal((await trace()).filter(e=>e.type==='open').length,2);
  });
});

test('热进程每次仍校验原文；原文变化和无向量候选均不得返回旧结果',async()=>{
  await fixture({...simple(),control:{empty:'空候选'}},async(docs,{corpusDir})=>{
    const first=await docs({query:'正常查询'});assert.equal(first.error,undefined);
    const file=resolve(corpusDir,'docs/usage/metrics/canary.md');
    await writeFile(file,'# 被改变的人工定义\n');
    assert.match((await docs({query:'正常查询'})).error.message,/原文已变化/);
    await writeFile(file,'# 人工定义\n');
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    const empty=await docs({query:'空候选'});assert.match(empty.error.message,/未产生向量候选/);
    assert.equal(empty.results,undefined);
  });
});

test('原子替换索引后重新打开 store，不持有旧文件的连接',async()=>{
  await fixture(simple(),async(docs,{runtimeDir,trace})=>{
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    const replacement=resolve(runtimeDir,'replacement.sqlite');
    await writeFile(replacement,'人工替换索引');await rename(replacement,resolve(runtimeDir,'index.sqlite'));
    assert.equal((await docs({query:'正常查询'})).error,undefined);
    assert.equal((await trace()).filter(e=>e.type==='open').length,2);
    assert.equal((await trace()).filter(e=>e.type==='close').length,1);
  });
});

test('关闭取消在途与排队请求，释放进程；重复关闭或再查询不会重启',async()=>{
  await fixture({...simple(),control:{block:'原生阻塞'}},async(docs,{trace})=>{
    const first=docs({query:'原生阻塞'});await waitFor(async()=>(await trace()).some(e=>e.type==='query'));
    const queued=docs({query:'排队请求'}),pid=(await trace()).find(e=>e.type==='open').pid;
    await docs.close();
    assert.match((await first).error.message,/关闭/);assert.match((await queued).error.message,/关闭/);
    assert.equal(gone(pid),true);await docs.close();
    assert.match((await docs({query:'关闭之后'})).error.message,/关闭/);
    assert.match((await docs.warmup()).error.message,/关闭/);
    assert.equal((await trace()).filter(e=>e.type==='open').length,1);
  });
});

test('所属进程退出后不遗留闲置的 QMD 子进程',async()=>{
  await fixture(simple(),async(_docs,{runtimeDir,corpusDir,manifestPath,trace})=>{
    const script=`import {createDocs} from ${JSON.stringify(new URL('./docs.mjs',import.meta.url).href)};
      const docs=await createDocs(${JSON.stringify({runtimeDir,corpusDir,manifestPath})});
      const result=await docs({query:'正常查询'});process.exit(result.error?1:0);`;
    const owner=spawn(process.execPath,['--input-type=module','--eval',script],{stdio:'ignore'});
    let pid;
    try{
      assert.deepEqual(await once(owner,'close'),[0,null]);
      pid=(await trace()).find(e=>e.type==='open').pid;
      await waitFor(()=>gone(pid));
    }finally{owner.kill();if(pid && !gone(pid))process.kill(pid,'SIGKILL');}
  });
});

test('向量遗漏术语时仍通过 QMD 关键词取得同版原文，不返回标准答案', async () => {
  const path = 'docs/usage/metrics/canary.md';
  const body = '# 青桐术语\n\ncanary_metric 是人工检索样例，单位为件。\n';
  await fixture({
    documents: { [path]: body, 'docs/usage/metrics/other.md': '# 无关定义\n别的内容。\n' },
    vector: [hit('docs/usage/metrics/other.md', 0.40, { chunkPos: 0 })],
    lexical: { canary_metric: [hit(path, 0.8)], '青桐术语': [hit(path, 0.8)] }
  }, async docs => {
    const result = await docs({ query: 'canary_metric 青桐术语 定义' });
    assert.equal(result.error, undefined);
    assert.deepEqual(result.results.map(row => row.text), [body.trimEnd()]);
    assert.equal(result.results[0].source, `domeye@${DOCS_SOURCE_COMMIT}:${path}:L1-L3`);
  });
});

test('命中略超行数上限文档的末尾时向前补齐窗口，保留同篇前部定义', async () => {
  const path = 'docs/usage/metrics/long.md';
  const lines = ['# 较长说明', ...Array.from({ length: 80 }, (_, i) => `第 ${i + 2} 行原文`)];
  lines[9] = '指标定义：这里保留前部说明。';
  const body = lines.join('\n') + '\n';
  await fixture({ documents: { [path]: body }, vector: [hit(path, 0.8, { chunkPos: body.indexOf('第 80 行') })] }, async docs => {
    const result = await docs({ query: '末段说明' });
    assert.equal(result.error, undefined);
    assert.equal(result.results[0].text, lines.slice(1).join('\n'));
    assert.equal(result.results[0].text.split('\n').length, 80);
    assert.equal(result.results[0].source, `domeye@${DOCS_SOURCE_COMMIT}:${path}:L2-L81`);
  });
});

test('向量执行没有候选时仍显式报错，不用关键词结果掩盖底层检索失败', async () => {
  const path = 'docs/usage/metrics/canary.md';
  await fixture({ documents: { [path]: '# 青桐术语\n' }, vector: [], lexical: { '青桐术语': [hit(path, 0.9)] } }, async docs => {
    const result = await docs({ query: '青桐术语' });
    assert.equal(result.error.kind, 'retrieval');
    assert.equal(result.results, undefined);
  });
});
