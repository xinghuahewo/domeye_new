import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');

// 运行状态由宿主选择，不进入模型工具参数；发布代码和可写状态分别存放。
export function runtimePaths(env = process.env) {
  const stateDir = resolve(env.DOMEYE_QUERY_STATE_DIR ?? resolve(repositoryRoot, '.local/query'));
  const runtimeDir = resolve(stateDir, 'qmd-runtime');
  return {
    stateDir, runtimeDir,
    corpusDir: resolve(stateDir, 'docs-corpus'),
    manifestPath: resolve(stateDir, 'docs-manifest.json'),
    dbPath: resolve(runtimeDir, 'index.sqlite'),
    historyDir: resolve(env.DOMEYE_HISTORY_DIR ?? resolve(stateDir, 'history')),
  };
}

export function chatAddress(host = '127.0.0.1', publicOrigin) {
  if (typeof host !== 'string' || !host.trim()) throw new Error('监听地址不能为空。');
  if (!publicOrigin) {
    if (host !== '127.0.0.1') throw new Error('非本机监听必须明确配置 DOMEYE_CHAT_ORIGIN。');
    return { host };
  }
  const url = new URL(publicOrigin);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password ||
      url.pathname !== '/' || url.search || url.hash) throw new Error('页面来源必须是完整的 HTTP(S) origin，不能带路径或凭据。');
  return { host, publicOrigin: url.origin };
}
