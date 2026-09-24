// 只保留可能跨分片的已知凭据前缀；正常文字无需等待固定长度缓冲。
export function createTextRedactor(secret) {
  let pending = '';
  const clean = value => value.split(secret).join('[已隐藏凭据]');
  return {
    push(text) {
      pending = clean(pending + text);
      let held = Math.min(secret.length - 1, pending.length);
      while (held > 0 && !secret.startsWith(pending.slice(-held))) held--;
      const ready = pending.slice(0, pending.length - held);
      pending = pending.slice(pending.length - held);
      return ready;
    },
    finish() { const ready = clean(pending); pending = ''; return ready; },
  };
}
