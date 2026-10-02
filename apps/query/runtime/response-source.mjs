// 只按已知业务合同定位来源，不递归寻找同名 version，也不借通用响应头推断正文版本。
export const DELIVERY_STATISTICS = new Map([
  ['/api/v1/features/countries/comparison', ['country-window-comparison/v1']],
  ['/api/v1/features/countries/series', ['country-feature-series/v1', 'country-feature-series/v2', 'country-feature-series/v3', 'country-feature-series/v4']],
  ...['country-as', 'country-prefix', 'as-prefix', 'global-as', 'global-prefix']
    .map(kind => [`/api/v1/features/outages/${kind}`, ['outage-series/v2']]),
]);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function responseSource(path, body) {
  const resolved = path === '/api/v2/events/resolve' && body?.schema_version === 'country-outage-delivery/v1';
  const record = resolved ? body.event : body;
  const interpretation = DELIVERY_STATISTICS.get(path);
  const versionPath = interpretation
    ? (interpretation.includes(body?.metadata?.interpretation_version) && typeof body.metadata.collector_id === 'string'
      && body.metadata.collector_id.trim() ? ['metadata', 'version'] : null)
    : resolved ? ['event', 'version'] : ['version'];
  let version = versionPath ? body : null;
  for (const key of versionPath ?? []) version = object(version) && Object.hasOwn(version, key) ? version[key] : null;
  return { record, version: version ?? null, versionPath };
}

// 原字段的直接摘录；点分路径保留封装位置，不将事件观测时间混成整批覆盖。
export function sourceMetadata(path, body) {
  const entries = [];
  const core = ['/api/v1/core-overview', '/api/v1/core-overview/record'].includes(path);
  const statistics = DELIVERY_STATISTICS.get(path)?.includes(body?.metadata?.interpretation_version);
  if ((core || statistics) && object(body) && Object.hasOwn(body, 'metadata')) entries.push(['metadata', body.metadata]);
  if (core && object(body?.item) && Object.hasOwn(body.item, 'lifecycle')) entries.push(['item.lifecycle', body.item.lifecycle]);
  if (path === '/api/v2/events/resolve' && body?.schema_version === 'country-outage-delivery/v1') {
    if (object(body.event) && Object.hasOwn(body.event, 'metadata')) entries.push(['event.metadata', body.event.metadata]);
    if (object(body.event?.item) && Object.hasOwn(body.event.item, 'lifecycle')) entries.push(['event.item.lifecycle', body.event.item.lifecycle]);
  }
  return entries;
}
