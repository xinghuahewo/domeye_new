import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runtimePaths, chatAddress } from './settings.mjs';

test('宿主状态配置供准备、检索与历史共用，未设置时位于 Git 外', () => {
  const paths = runtimePaths({ DOMEYE_QUERY_STATE_DIR:'/tmp/domeye-state', DOMEYE_HISTORY_DIR:'/tmp/domeye-history' });
  assert.equal(paths.runtimeDir, '/tmp/domeye-state/qmd-runtime');
  assert.equal(paths.corpusDir, '/tmp/domeye-state/docs-corpus');
  assert.equal(paths.manifestPath, '/tmp/domeye-state/docs-manifest.json');
  assert.equal(paths.dbPath, '/tmp/domeye-state/qmd-runtime/index.sqlite');
  assert.equal(paths.historyDir, '/tmp/domeye-history');
  assert.ok(runtimePaths({}).stateDir.endsWith('/.local/query'));
});

test('非本机监听须有明确页面来源，不允许带路径或凭据的来源', () => {
  assert.deepEqual(chatAddress(), { host:'127.0.0.1' });
  assert.throws(() => chatAddress('0.0.0.0'), /DOMEYE_CHAT_ORIGIN/);
  for (const origin of ['https://example.org/query', 'http://user:pass@example.org', 'file:///tmp/', 'http://example.org?x=1', 'http://example.org/#x']) {
    assert.throws(() => chatAddress('127.0.0.1', origin));
  }
  assert.deepEqual(chatAddress('10.99.8.16', 'http://10.99.8.16:28684/'), {host:'10.99.8.16',publicOrigin:'http://10.99.8.16:28684'});
});
