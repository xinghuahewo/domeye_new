import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createModels} from '@earendil-works/pi-ai';
import {deepseekProvider} from '@earendil-works/pi-ai/providers/deepseek';
import {configureModel,configuredThinkingLevel} from './model-options.mjs';

test('Pro 的 low/high 经锁定 Pi 序列化为对应请求；不改变默认、输出额度或全局模型',async()=>{
  const models=createModels();models.setProvider(deepseekProvider());
  const known=models.getModel('deepseek','deepseek-v4-pro');
  const original=structuredClone(known);
  for(const requested of [undefined,'high','low']){
    const {model,thinkingLevel}=configureModel(known,{thinkingLevel:requested});
    let payload;
    const stream=models.streamSimple(model,{messages:[{role:'user',content:'人工请求测试',timestamp:0}]},{
      apiKey:'synthetic-key',reasoning:thinkingLevel,maxTokens:32768,maxRetries:0,
      fetch:async(_url,options)=>{
        payload=JSON.parse(options.body);
        return new Response('data: '+JSON.stringify({choices:[{index:0,delta:{content:'人工响应'},finish_reason:'stop'}]})+'\n\ndata: [DONE]\n\n',
          {headers:{'content-type':'text/event-stream'}});
      }
    });
    assert.equal((await stream.result()).stopReason,'stop');
    assert.equal(payload.reasoning_effort,requested ?? 'high','实际请求不能被 Pi 静默升级');
    assert.deepEqual(payload.thinking,{type:'enabled'});
    assert.equal(payload.max_tokens,32768);
    assert.equal(payload.model,'deepseek-v4-pro');
    assert.deepEqual(known,original,'会话修正不得修改提供方全局目录');
  }
});

test('未知推理档位明确拒绝，其他已登记模型不套用 Pro 兼容修正',()=>{
  for(const level of ['fast','off','medium',true,3])assert.throws(()=>configuredThinkingLevel({thinkingLevel:level}),/thinkingLevel/);
  const known={id:'synthetic-other',provider:'deepseek',thinkingLevelMap:{low:null,high:'high'}};
  assert.deepEqual(configureModel(known,{}).model.thinkingLevelMap,known.thinkingLevelMap);
});
