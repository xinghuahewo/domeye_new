// 只保留可能跨分片的已知凭据前缀；正常文字无需等待固定长度缓冲。
export function createTextRedactor(secret) {
  const secrets = [...new Set((Array.isArray(secret)?secret:[secret]).filter(value=>typeof value==='string' && value.length))];
  let pending = '';
  const clean = value => secrets.reduce((text,key)=>text.split(key).join('[已隐藏凭据]'),value);
  return {
    push(text) {
      pending = clean(pending + text);
      let held = 0;
      for (const key of secrets) {
        let length = Math.min(key.length - 1, pending.length);
        while (length > held && !key.startsWith(pending.slice(-length))) length--;
        held = Math.max(held,length);
      }
      const ready = pending.slice(0, pending.length - held);
      pending = pending.slice(pending.length - held);
      return ready;
    },
    finish() { const ready = clean(pending); pending = ''; return ready; },
  };
}
