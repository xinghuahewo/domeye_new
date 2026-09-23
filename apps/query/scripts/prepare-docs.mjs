import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { DOCS_SOURCE_COMMIT, DOCS_EMBED_MODEL } from '../docs.mjs';
import { runtimePaths } from '../runtime/settings.mjs';

const run = promisify(execFile);
const { stateDir, runtimeDir, corpusDir, manifestPath, dbPath } = runtimePaths();
const command = process.argv[2];

if (command === 'snapshot') {
  const sourceRepo = process.argv[3];
  if (!sourceRepo) throw new Error('用法：node apps/query/scripts/prepare-docs.mjs snapshot /绝对路径/源码仓库');
  const repo = resolve(sourceRepo);
  const { stdout } = await run('git', ['-C', repo, 'ls-tree', '-r', '--name-only', DOCS_SOURCE_COMMIT, '--', 'docs/usage', 'CONTEXT.md']);
  const paths = stdout.trim().split('\n').filter(Boolean);
  if (!paths.length) throw new Error('指定提交中没有所需业务文档。');
  const files = [];
  for (const path of paths) {
    const { stdout: raw } = await run('git', ['-C', repo, 'show', `${DOCS_SOURCE_COMMIT}:${path}`], {encoding:'buffer', maxBuffer:10_000_000});
    const target = resolve(corpusDir, path);
    await mkdir(dirname(target), { recursive:true });
    await writeFile(target, raw);
    const text = raw.toString('utf8');
    files.push({ path, sha256:createHash('sha256').update(raw).digest('hex'), lines:text.split('\n').length - (text.endsWith('\n') ? 1 : 0) });
  }
  await mkdir(stateDir, { recursive:true });
  await writeFile(manifestPath, JSON.stringify({sourceCommit:DOCS_SOURCE_COMMIT, sourceRepository:repo, files},null,2)+'\n');
  console.log(JSON.stringify({sourceCommit:DOCS_SOURCE_COMMIT, documents:files.length}));
} else {
  if (!['update', 'embed', 'lex', 'vector', 'status'].includes(command)) {
    throw new Error('用法：prepare-docs.mjs snapshot <源码仓库> | update | embed | lex <查询> | vector <查询> | status');
  }
  process.env.XDG_CACHE_HOME = resolve(runtimeDir, 'cache');
  process.env.XDG_CONFIG_HOME = resolve(runtimeDir, 'config');
  process.env.QMD_EMBED_MODEL = DOCS_EMBED_MODEL;
  process.env.QMD_EMBED_PARALLELISM = '1';
  if (process.platform === 'darwin') process.env.GGML_METAL_NO_RESIDENCY = '1';
  const { createStore } = await import(pathToFileURL(resolve(runtimeDir, 'node_modules/@tobilu/qmd/dist/index.js')).href);
  const store = await createStore({
    dbPath,
    config:{models:{embed:DOCS_EMBED_MODEL}, collections:{domeye:{path:corpusDir, pattern:'**/*.md'}}},
  });
  try {
    if (command === 'update') console.log(JSON.stringify(await store.update(),null,2));
    else if (command === 'embed') console.log(JSON.stringify(await store.embed({onProgress:progress => console.error(JSON.stringify(progress))}),null,2));
    else if (command === 'status') console.log(JSON.stringify(store.internal.getStatus(DOCS_EMBED_MODEL),null,2));
    else {
      const query = process.argv[3];
      if (!query?.trim()) throw new Error('查询不能为空。');
      const search = command === 'lex' ? store.searchLex : store.searchVector;
      const hits = await search(query, {limit:4, collection:'domeye'});
      console.log(JSON.stringify(hits.map(({filepath,score,title,chunkPos}) => ({filepath,score,title,chunkPos})),null,2));
    }
  } finally {
    await store.close();
  }
}
