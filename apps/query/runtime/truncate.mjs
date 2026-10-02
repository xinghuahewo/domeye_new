// 对照 Cloudflare mcp/src/truncate.ts：按 JS 字符长度限制模型可见文本。
// token 数仅按每 token 四字符估算，不是实际 tokenizer 计数。
const CHARS_PER_TOKEN = 4;
const MAX_TOKENS = 6000;
const MAX_CHARS = MAX_TOKENS * CHARS_PER_TOKEN;

export function truncateResponse(content, {toolCallId} = {}) {
  const text = typeof content === 'string' ? content : JSON.stringify(content, null, 2);
  if (text.length <= MAX_CHARS) return text;
  if (typeof content !== 'string') {
    // 先尝试完整的紧凑 JSON；不改字段、值、字符串内部空白或字符额度。
    const compact = JSON.stringify(content);
    if (compact.length <= MAX_CHARS) return compact;
  }
  const estimatedTokens = Math.ceil(text.length / CHARS_PER_TOKEN);
  const guidance = toolCallId
    ? `完整返回已保留。本题下一次 execute 用 await domeye.readResult(${JSON.stringify(toolCallId)}) 读取后筛选或计算，无需重复 HTTP。`
    : '请使用更具体的查询减少返回内容。';
  return `${text.slice(0, MAX_CHARS)}\n\n--- TRUNCATED ---\n响应约 ${estimatedTokens.toLocaleString()} tokens（估算上限：${MAX_TOKENS.toLocaleString()}）。${guidance}`;
}
