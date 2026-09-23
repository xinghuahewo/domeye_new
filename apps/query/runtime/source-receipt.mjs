import { isDeepStrictEqual } from 'node:util';

// 只摘录一次实际 HTTP 的来源与正文自有范围，不推断任意 JavaScript 返回值的字段血缘。
const SCOPE_FIELDS = [
  'version', 'state', 'query', 'coverage', 'scope', 'scope_kind', 'scope_note',
  'start_time', 'end_time', 'timezone', 'window_boundary',
];
const SOURCE_FIELDS = ['family', 'binding', 'expectedVersion', 'responseVersion', 'versionAssurance', 'epoch'];
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const bytesOf = value => Buffer.byteLength(JSON.stringify(value), 'utf8');

/**
 * event 与 response 必须由同一次 request-policy response 回调一起传入。
 * 函数能拒绝事件类型、状态或返回版本不符，不能凭相同值证明两个输入属于同一次请求。
 * maxScopeBytes 限制完整 scope JSON 的 UTF-8 字节数，至少为容纳 {} 的 2 字节。
 * 超预算按整字段省略，继续检查后续字段；不改写原始 request、response 或事件。
 */
export function makeSourceReceipt({ toolCallId, event, response, maxScopeBytes = 32768 }) {
  if (typeof toolCallId !== 'string' || !toolCallId.trim()) throw new TypeError('来源回执须绑定工具调用标识。');
  if (!isObject(event) || event.type !== 'response' || !Object.hasOwn(event, 'id') ||
      !Object.hasOwn(event, 'turn') || !isObject(event.request)) {
    throw new TypeError('来源回执只接受含 id、turn 和 request 的实际 response 事件。');
  }
  if (!isObject(response) || !Object.hasOwn(response, 'status') || !Object.hasOwn(event, 'status') ||
      response.status !== event.status) {
    throw new TypeError('来源回执的事件状态须与同次原始 HTTP 响应状态一致。');
  }
  if (!Number.isSafeInteger(maxScopeBytes) || maxScopeBytes < 2) throw new RangeError('范围预算须为至少 2 字节的安全整数。');
  // 对齐 request-policy 的 responseVersion 记录规则；仅核对配对完整性，不重新判定版本保证。
  const bodyVersion = isObject(response.body) && Object.hasOwn(response.body, 'version') ? response.body.version ?? null : null;
  if (Object.hasOwn(event, 'responseVersion') && !isDeepStrictEqual(event.responseVersion, bodyVersion)) {
    throw new TypeError('来源回执的事件返回版本须与同次原始 HTTP 正文一致。');
  }

  const source = {};
  for (const field of SOURCE_FIELDS) if (Object.hasOwn(event, field)) source[field] = structuredClone(event[field]);
  if (isObject(event.request.query) && Object.hasOwn(event.request.query, 'version')) {
    source.requestVersion = structuredClone(event.request.query.version);
  }
  const scope = {}, omittedFields = [];
  const body = response.body;
  const bodyType = body === null ? 'null' : Array.isArray(body) ? 'array' : typeof body;
  if (isObject(body)) {
    for (const field of SCOPE_FIELDS) {
      if (!Object.hasOwn(body, field)) continue;
      const value = structuredClone(body[field]);
      if (JSON.stringify(value) === undefined) throw new TypeError('范围字段须能作为完整 JSON 值摘录。');
      if (bytesOf({ ...scope, [field]: value }) > maxScopeBytes) omittedFields.push(field);
      else scope[field] = value;
    }
  }
  return {
    toolCallId, id: structuredClone(event.id), turn: structuredClone(event.turn),
    request: structuredClone(event.request), status: response.status, source,
    note: '仅对应这次 HTTP 响应，不是任意 JavaScript 返回值的字段血缘；不能从值相同推导来源。版本未确认不能借其他响应的覆盖，版本已确认也不等于跨入口语义可比。',
    scope,
    scopeInfo: {
      bodyType, omittedFields, scopeBytes: bytesOf(scope), maxScopeBytes,
      ...(!isObject(body) ? { note: '正文不是对象，没有可摘录的顶层范围字段。' } : {}),
    },
  };
}
