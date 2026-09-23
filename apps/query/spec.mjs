import { readFile } from 'node:fs/promises';

export const SPEC_TYPES = `interface Operation {
  summary?: string;
  description?: string;
  tags?: string[];
  deprecated?: boolean;
  parameters?: Array<{name: string; in: string; required?: boolean; description?: string; schema?: unknown}>;
  responses?: Record<string, {description?: string; content?: Record<string, {schema?: unknown}>}>;
}
declare const spec: {
  paths: Record<string, {get?: Operation; [method: string]: unknown}>;
  components: {schemas: Record<string, unknown>; parameters: Record<string, unknown>};
};`;

// search 消费的派生视图：只展开同一规范内的引用，不联网、不修改原规范。
export function resolveLocalRefs(document) {
  function target(ref) {
    if (typeof ref !== 'string' || !ref.startsWith('#/')) throw new Error('search 规范只支持文件内 JSON Pointer 引用。');
    let value = document;
    for (const part of decodeURIComponent(ref.slice(2)).split('/')) {
      const key = part.replace(/~1/g, '/').replace(/~0/g, '~');
      if (!value || typeof value !== 'object' || !Object.hasOwn(value, key)) throw new Error(`search 规范引用不存在：${ref}`);
      value = value[key];
    }
    return value;
  }
  function expand(value, chain = []) {
    if (Array.isArray(value)) return value.map(item => expand(item, chain));
    if (!value || typeof value !== 'object') return value;
    if (!Object.hasOwn(value, '$ref')) return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, expand(item, chain)]));
    const ref = value.$ref;
    if (chain.includes(ref)) throw new Error(`search 规范含循环引用，尚未展开：${ref}`);
    const resolved = expand(target(ref), [...chain, ref]);
    const siblings = Object.fromEntries(Object.entries(value).filter(([key]) => key !== '$ref').map(([key, item]) => [key, expand(item, chain)]));
    const annotations = Object.fromEntries(Object.entries(siblings).filter(([key]) => ['description', 'summary'].includes(key)));
    const constraints = Object.fromEntries(Object.entries(siblings).filter(([key]) => !['description', 'summary'].includes(key)));
    // 结构约束与目标同时适用，不能用浅合并覆盖掉目标的 properties/type 等限制。
    const combined = Object.keys(constraints).length ? { allOf: [resolved, constraints] } : resolved;
    if (!Object.keys(annotations).length) return combined;
    return { ...(combined && typeof combined === 'object' ? combined : { allOf: [combined] }), ...annotations };
  }
  return expand(document);
}

export async function loadSearchSpec(specFile = 'openapi.json') {
  return resolveLocalRefs(JSON.parse(await readFile(new URL('./data/' + specFile, import.meta.url), 'utf8')));
}
