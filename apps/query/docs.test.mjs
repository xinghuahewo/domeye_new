import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { createDocs, DOCS_SOURCE_COMMIT } from './docs.mjs';

// 经过实际 docs 子进程与原文校验；只替换外部 QMD SDK，不连接业务数据或模型。
async function fixture({ documents, vector, lexical = {} }, run) {
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
    await writeFile(resolve(sdk, 'input.json'), JSON.stringify({ documents, vector, lexical }));
    await writeFile(resolve(sdk, 'index.js'), `
      import {readFile} from 'node:fs/promises';
      const input=JSON.parse(await readFile(new URL('./input.json',import.meta.url),'utf8'));
      export async function createStore(){
        return {
          internal:{getStatus:()=>({hasVectorIndex:true,needsEmbedding:0,totalDocuments:Object.keys(input.documents).length})},
          searchVector:async()=>input.vector,
          searchLex:async query=>input.lexical[query] ?? [],
          getDocumentBody:async(uri,options)=>{
            const body=input.documents[uri.slice('qmd://domeye/'.length)];
            if(!options)return body;
            const lines=body.split('\\n');
            if(lines.at(-1)==='')lines.pop();
            return lines.slice(options.fromLine-1,options.fromLine-1+options.maxLines).join('\\n');
          },
          close:async()=>{}
        };
      }
    `);
    docs = await createDocs({ runtimeDir, corpusDir, manifestPath });
    await run(docs);
  } finally {
    await docs?.close();
    await rm(root, { recursive: true, force: true });
  }
}

const hit = (path, score, extra = {}) => ({ filepath: 'qmd://domeye/' + path, title: path, score, ...extra });

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
