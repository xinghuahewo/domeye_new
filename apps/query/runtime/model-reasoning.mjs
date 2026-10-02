import { createTextRedactor } from './text-stream.mjs';

// 只记录提供方实际返回的文本；与模型上下文和公开事件分开。
export function createModelReasoning(modelRoundId, secret) {
  const record = {model_round_id:modelRoundId,state:'receiving',blocks:[]};
  const blocks = new Map();
  let finished = false;
  const blockAt = index => {
    if (!blocks.has(index)) {
      const value = {content_index:index,text:''};
      record.blocks.push(value);
      blocks.set(index,{value,redactor:createTextRedactor(secret)});
    }
    return blocks.get(index);
  };
  return {
    record,
    observe(event) {
      if (finished || event.type!=='thinking_delta' || typeof event.delta!=='string' || !event.delta) return;
      const block = blockAt(event.contentIndex);
      block.value.text += block.redactor.push(event.delta);
    },
    finish(message) {
      if (finished) return;
      finished = true;
      // 结束消息有完整块时以它为准；失败丢失内容时仍保留已经收到的片段。
      for (const [index,part] of (message?.content ?? []).entries()) {
        if (part.type!=='thinking' || typeof part.thinking!=='string' || !part.thinking) continue;
        const block = blockAt(index);
        block.redactor = createTextRedactor(secret);
        block.value.text = block.redactor.push(part.thinking);
      }
      for (const block of blocks.values()) block.value.text += block.redactor.finish();
      record.blocks.sort((a,b)=>a.content_index-b.content_index);
      record.state = !record.blocks.length ? 'not_returned'
        : ['stop','toolUse'].includes(message?.stopReason) ? 'recorded' : 'partial';
    },
  };
}
