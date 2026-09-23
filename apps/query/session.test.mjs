import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';

// 用固定模型结束消息和可控写入延迟复现取消竞态；不作为真实问答验收。
test('保存期间停止：拒绝重入，返回并保存 cancelled，批处理据此停止', async () => {
  let beginWrite, finishWrite;
  const writing=new Promise(resolve=>{beginWrite=resolve;});
  const release=new Promise(resolve=>{finishWrite=resolve;});
  const saved=[];
  const fileMock=mock.module('node:fs/promises',{namedExports:{
    ...fs,
    mkdir:async()=>{},
    writeFile:async(_path,body)=>{
      saved.push(JSON.parse(body));
      if(saved.length===1){beginWrite();await release;}
    }
  }});
  class FakeAgent {
    constructor(options){this.options=options;this.state={messages:[]};}
    subscribe(){}
    async prompt(question){
      const message={role:'assistant',stopReason:'stop',content:[{type:'text',text:question+'：固定测试响应'}]};
      this.state.messages.push(message);await this.options.finishTurn?.({message});
    }
    abort(){}
    async waitForIdle(){}
  }
  const mocks=[
    fileMock,
    mock.module('@earendil-works/pi-agent-core',{namedExports:{Agent:FakeAgent}}),
    mock.module('@earendil-works/pi-ai',{namedExports:{createModels:()=>({setProvider(){},getModel(){return {id:'fixture-model'};}})}}),
    mock.module('@earendil-works/pi-ai/providers/deepseek',{namedExports:{deepseekProvider:()=>({})}}),
    mock.module(new URL('./tools.mjs',import.meta.url).href,{namedExports:{createTools:async()=>({tools:[],close:async()=>{}})}}),
  ];
  try {
    const {createDomeyeAgent}=await import('./agent.mjs?session-test');
    const agent=await createDomeyeAgent({modelConfig:{model:'fixture-model',apiKey:'synthetic-test-key'},historyDir:'/synthetic-history'});
    const answering=agent.ask('第一题');
    await writing;
    assert.equal(agent.running,true);
    assert.equal(agent.turns[0].answer,'','保存完成前轮询不公开候选文字');
    await assert.rejects(agent.ask('不应启动的新问题'),/尚未结束/);
    agent.stop();
    finishWrite();
    const result=await answering;
    assert.equal(result.status,'cancelled');
    assert.equal(agent.running,false);
    assert.equal(saved.length,2);
    assert.equal(saved.at(-1).turns.length,1);
    assert.equal(saved.at(-1).turns[0].status,'cancelled');
    assert.equal(saved.at(-1).turns[0].cancelled,true);
    assert.equal(saved.at(-1).turns[0].answer,'');
    await agent.close();
  } finally {for(const item of mocks.reverse())item.restore();}
});
