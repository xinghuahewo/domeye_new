import { Agent } from '@earendil-works/pi-agent-core';
import { createModels } from '@earendil-works/pi-ai';
import { deepseekProvider } from '@earendil-works/pi-ai/providers/deepseek';
import { readFile, stat, mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { createTools } from './tools.mjs';
import { CONTRACT_SOURCE_COMMIT } from './source.mjs';
import { selectDataset, publicDataset } from './datasets.mjs';

const textOf = message => (message?.content ?? []).filter(part => part.type === 'text').map(part => part.text).join('\n');
// DeepSeek 的推理与正文共用输出额度；保持有界，不自动续写 length 结果。
const maxOutputTokens = 32768;
// Pi 要求 streamFn 用结束事件表达取消；停止后不再进入模型提供方。
function stoppedStream(model) {
  const message = {role:'assistant',content:[],api:model.api,provider:model.provider,model:model.id,
    stopReason:'aborted',errorMessage:'本轮已停止。',timestamp:Date.now(),
    usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}};
  return {async *[Symbol.asyncIterator](){yield {type:'error',reason:'aborted',error:message};},async result(){return message;}};
}

export async function loadModelConfig(path) {
  if (!path) throw new Error('请通过 DOMEYE_MODEL_CONFIG 指定宿主的独立模型配置。');
  const info = await stat(path);
  if (!info.isFile() || (info.mode & 0o077)) throw new Error('模型配置须为仅当前用户可读写的独立普通文件。');
  const config = JSON.parse(await readFile(path,'utf8'));
  if (config.provider !== 'deepseek' || !config.model || !config.apiKey) throw new Error('模型配置须提供 DeepSeek 的 provider、model、apiKey。');
  if (config.baseUrl && new URL(config.baseUrl).origin !== 'https://api.deepseek.com') throw new Error('当前验证只连接配置中的 DeepSeek 官方 API。');
  return config;
}

export async function createDomeyeAgent({ modelConfig, datasetId, apiBaseUrl, docsConfig, historyDir, onEvent=()=>{} }) {
  const dataset = selectDataset(datasetId, apiBaseUrl);
  apiBaseUrl = dataset.apiBaseUrl;
  const models = createModels();
  models.setProvider(deepseekProvider());
  const known = models.getModel('deepseek',modelConfig.model);
  if (!known) throw new Error('锁定 Pi 版本未登记所配置的 DeepSeek 模型。');
  const model = {...known,baseUrl:modelConfig.baseUrl ?? known.baseUrl};
  const systemPrompt = await readFile(new URL('./agent-instructions.md',import.meta.url),'utf8') +
    `\n\n当前会话的数据：${dataset.label}。${dataset.description} 工具已绑定该批次的读取入口和接口规范。只使用本项目这一批结果；日期、覆盖和版本从实际响应发现，不能从批次名称推定。旧项目 55 天数据不在范围内。用户若要换批次，请告知在页面选择数据并新建会话。业务定义和限制按需用 docs 读取。\n`;
  const apiSpecHash = createHash('sha256').update(await readFile(new URL('./data/' + dataset.specFile, import.meta.url))).digest('hex');
  const apiSourceSnapshot = dataset.apiSourceSnapshot ? JSON.parse(await readFile(new URL('./data/' + dataset.apiSourceSnapshot, import.meta.url),'utf8')) : null;
  const sessionId = randomUUID();
  const turns = [];
  let current, busy = false, toolCalls = 0;
  const redact = value => JSON.parse(JSON.stringify(value).split(modelConfig.apiKey).join('[已隐藏凭据]'));
  const evidence = (type,value) => {
    if (current) current.events.push({at:new Date().toISOString(),type,value:redact(value)});
  };
  const registered = await createTools({apiBaseUrl,specFile:dataset.specFile,docsConfig,onEvidence:evidence});
  const agent = new Agent({
    initialState:{systemPrompt,model,thinkingLevel:'high',tools:registered.tools},
    streamFn:(selected,context,options) => {
      if (options?.signal?.aborted || current?.cancelled || current?.failure) return stoppedStream(selected);
      return models.streamSimple(selected,context,{...options,apiKey:modelConfig.apiKey,maxTokens:maxOutputTokens});
    },
    sessionId, toolExecution:'sequential',
    beforeToolCall:async (_context,signal) => {
      if (signal?.aborted) return {block:true,reason:'本轮已停止；当前取数未完成。',terminate:true};
      if (++toolCalls>20) {
        if(current)current.failure='本轮超过工具调用上限，取数未完成。';
        return {block:true,reason:'本轮超过工具调用上限；当前取数未完成。',terminate:true};
      }
    }
  });
  agent.subscribe(event => {
    if (event.type==='tool_execution_start') evidence(event.type,{toolCallId:event.toolCallId,toolName:event.toolName,args:event.args});
    if (event.type==='message_end') {
      const message = {...event.message};
      if (Array.isArray(message.content)) message.content=message.content.filter(part=>part.type!=='thinking');
      evidence('message',message);
    }
    // 正文在正常结束且保存成功后发布；工具状态实时显示，私有思考不公开。
    if (event.type==='tool_execution_start' || event.type==='tool_execution_end') {
      onEvent({type:event.type,toolCallId:event.toolCallId,toolName:event.toolName,isError:event.isError});
    }
  });
  async function save() {
    if (!historyDir) return;
    await mkdir(historyDir,{recursive:true,mode:0o700});
    await writeFile(resolve(historyDir,sessionId+'.json'),JSON.stringify({
      id:sessionId,provider:'deepseek',model:model.id,pi:'0.87.0',api_base_url:apiBaseUrl,
      model_options:{thinking_level:'high',max_output_tokens:maxOutputTokens},
      agent_instructions:systemPrompt,
      answer_review:{enabled:false},
      dataset:{...publicDataset(dataset),source_run:dataset.sourceRun},
      source_commit:dataset.apiSourceCommit,api_source_snapshot:apiSourceSnapshot,
      api_spec_sha256:apiSpecHash,contract_source_commit:CONTRACT_SOURCE_COMMIT,
      tool_definitions:registered.tools.map(({name,label,description,parameters})=>({name,label,description,parameters})),
      tools:['docs','search','execute'],turns
    },null,2)+'\n',{mode:0o600});
  }
  function stop() {
    if (current) {
      current.stop_requested_at ??= new Date().toISOString();
      current.cancelled=true;
      current.answer='';
      if(current.status!=='failed')current.status='cancelled';
    }
    agent.abort();
  }
  return {
    id:sessionId,
    dataset:publicDataset(dataset),
    get running(){return busy;},
    async ask(question) {
      if (busy) throw new Error('当前回答或记录保存尚未结束，请先等待或停止。');
      busy=true;
      const turn={question,started_at:new Date().toISOString(),status:'running',events:[]};
      current=turn;turns.push(turn);toolCalls=0;
      const first=agent.state.messages.length;
      try {
        registered.beginTurn?.();
        await agent.prompt(question);
        const messages=agent.state.messages.slice(first);
        const last=[...messages].reverse().find(message=>message.role==='assistant');
        const endedNormally=last?.stopReason==='stop' && !(last.content ?? []).some(part=>part.type==='toolCall');
        turn.answer=endedNormally && !turn.cancelled && !turn.failure ? redact(textOf(last)) : '';
        turn.stopReason=last?.stopReason;
        turn.status=turn.cancelled?'cancelled':turn.failure?'failed':last?.stopReason==='aborted'?'cancelled':!endedNormally || !turn.answer?'failed':'completed';
        if(turn.failure)turn.error=turn.failure;
        else if(last?.stopReason==='length')turn.error='模型输出达到长度限制，本轮未完整回答。';
        else if(turn.status==='failed')turn.error='本轮未取得正常结束的完整回答。';
        if (agent.state.errorMessage && !turn.failure) turn.error=redact(agent.state.errorMessage);
      } catch(error) {
        turn.status=turn.cancelled?'cancelled':'failed';turn.error=redact(error.message);
      } finally {
        if(turn.cancelled || turn.status==='failed')turn.answer='';
        turn.completed_at=new Date().toISOString();
        try {
          const cancelledBeforeSave=Boolean(turn.cancelled);
          await save();
          // 保存期间仍允许停止；第一次写入可能已序列化，需补存取消状态。
          if(turn.cancelled && !cancelledBeforeSave)await save();
        }
        catch(error){turn.status='failed';turn.answer='';turn.error='历史记录保存失败：'+redact(error.message);}
        finally {current=null;busy=false;}
      }
      if (turn.status==='completed') onEvent({type:'answer_final',text:turn.answer});
      return turn;
    },
    stop,
    close:async()=>{stop();await agent.waitForIdle();await registered.close();},
    get turns(){return structuredClone(turns).map((turn,index) => busy && index===turns.length-1
      ? {...turn,answer:'',status:current?.cancelled?'cancelled':'running'} : turn);}
  };
}
