import { Agent } from '@earendil-works/pi-agent-core';
import { createModels } from '@earendil-works/pi-ai';
import { deepseekProvider } from '@earendil-works/pi-ai/providers/deepseek';
import { readFile, stat, mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { createTools } from './tools.mjs';
import { CONTRACT_SOURCE_COMMIT } from './source.mjs';
import { selectDataset, publicDataset } from './datasets.mjs';
import { createTextRedactor } from './runtime/text-stream.mjs';
import { configureModel, configuredThinkingLevel } from './runtime/model-options.mjs';
import { createModelTiming } from './runtime/model-timing.mjs';
import { createModelReasoning } from './runtime/model-reasoning.mjs';
import { runtimePaths } from './runtime/settings.mjs';

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
  configuredThinkingLevel(config);
  return config;
}

export async function createDomeyeAgent({ modelConfig, datasetId, apiBaseUrl, docsConfig, historyDir=runtimePaths().historyDir, onEvent=()=>{} }) {
  if (typeof historyDir!=='string' || !historyDir.trim()) throw new Error('须提供宿主历史目录以保存模型推理与取证记录。');
  const dataset = selectDataset(datasetId, apiBaseUrl);
  apiBaseUrl = dataset.apiBaseUrl;
  const models = createModels();
  models.setProvider(deepseekProvider());
  const known = models.getModel('deepseek',modelConfig.model);
  if (!known) throw new Error('锁定 Pi 版本未登记所配置的 DeepSeek 模型。');
  const {model,thinkingLevel} = configureModel(known,modelConfig);
  const systemPrompt = await readFile(new URL('./agent-instructions.md',import.meta.url),'utf8') +
    `\n\n当前会话的数据：${dataset.label}。${dataset.description} 工具已绑定该批次的读取入口和接口规范。只使用本项目这一批结果；日期、覆盖和版本从实际响应发现，不能从批次名称推定。旧项目 55 天数据不在范围内。用户若要换批次，请告知在页面选择数据并新建会话。业务定义和限制按需用 docs 读取。\n`;
  const apiSpecHash = createHash('sha256').update(await readFile(new URL('./data/' + dataset.specFile, import.meta.url))).digest('hex');
  const apiSourceSnapshot = dataset.apiSourceSnapshot ? JSON.parse(await readFile(new URL('./data/' + dataset.apiSourceSnapshot, import.meta.url),'utf8')) : null;
  const sessionId = randomUUID();
  const turns = [];
  let current, busy = false, toolCalls = 0, startedClock, modelTiming, modelObserver, modelReasoning;
  let textBlocks = new Map();
  const elapsed = () => Math.round((performance.now() - startedClock) * 1000) / 1000;
  const delta = (contentIndex, text) => {
    if (text && current && !current.cancelled && !current.failure) {
      modelTiming.first_public_text_ms ??= elapsed();
      onEvent({type:'answer_delta',messageId:modelTiming.id,contentIndex,text});
    }
  };
  const redact = value => JSON.parse(JSON.stringify(value).split(modelConfig.apiKey).join('[已隐藏凭据]'));
  const evidence = (type,value) => {
    if (current) current.events.push({at:new Date().toISOString(),type,value:redact(value)});
  };
  const registered = await createTools({apiBaseUrl,specFile:dataset.specFile,docsConfig,onEvidence:evidence});
  const agent = new Agent({
    initialState:{systemPrompt,model,thinkingLevel,tools:registered.tools},
    streamFn:(selected,context,options) => {
      if (options?.signal?.aborted || current?.cancelled || current?.failure) return stoppedStream(selected);
      modelTiming = {id:current.timings.models.length + 1,started_ms:elapsed(),first_token_ms:null,
        first_text_ms:null,first_public_text_ms:null,ended_ms:null};
      current.timings.models.push(modelTiming);
      modelObserver = createModelTiming(modelTiming,elapsed);
      modelReasoning = createModelReasoning(modelTiming.id,modelConfig.apiKey);
      current.reasoning.push(modelReasoning.record);
      textBlocks = new Map();
      onEvent({type:'answer_start',messageId:modelTiming.id});
      return models.streamSimple(selected,context,{...options,apiKey:modelConfig.apiKey,maxTokens:maxOutputTokens,
        fetch:modelObserver.wrapFetch(options?.fetch ?? globalThis.fetch)});
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
    if (event.type==='message_update' && current && modelTiming) {
      const part = event.assistantMessageEvent;
      modelObserver.observe(part);
      modelReasoning.observe(part);
      if (part.type==='text_delta' && part.delta && !current.cancelled && !current.failure) {
        modelTiming.first_text_ms ??= elapsed();
        if (!textBlocks.has(part.contentIndex)) textBlocks.set(part.contentIndex,createTextRedactor(modelConfig.apiKey));
        delta(part.contentIndex,textBlocks.get(part.contentIndex).push(part.delta));
      }
    }
    if (event.type==='tool_execution_start') evidence(event.type,{toolCallId:event.toolCallId,toolName:event.toolName,args:event.args});
    if (event.type==='message_end') {
      const message = {...event.message};
      if (message.role==='assistant') modelReasoning?.finish(message);
      if (Array.isArray(message.content)) message.content=message.content.filter(part=>part.type!=='thinking');
      evidence('message',message);
      if (message.role==='assistant' && current && modelTiming) {
        modelTiming.ended_ms=elapsed();
        modelTiming.stop_reason=message.stopReason;
        modelObserver.finish(message);
        if (message.stopReason==='stop' && !current.cancelled && !current.failure) {
          for (const [index, redactor] of textBlocks) delta(index,redactor.finish());
        }
        textBlocks.clear();
        onEvent({type:'answer_end',messageId:modelTiming.id,stopReason:current.cancelled?'aborted':message.stopReason});
      }
    }
    // 正文是尚未完成的预览；正常结束并保存后才确认最终答案。思考不转发。
    if (event.type==='tool_execution_start' || event.type==='tool_execution_end') {
      if (current) {
        if(event.type==='tool_execution_start')current.timings.tools.push({id:event.toolCallId,name:event.toolName,started_ms:elapsed(),ended_ms:null});
        else {
          const timing=current.timings.tools.find(item=>item.id===event.toolCallId);
          if(timing){timing.ended_ms=elapsed();timing.is_error=Boolean(event.isError);}
        }
      }
      onEvent({type:event.type,toolCallId:event.toolCallId,toolName:event.toolName,isError:event.isError});
    }
  });
  async function save() {
    await mkdir(historyDir,{recursive:true,mode:0o700});
    await writeFile(resolve(historyDir,sessionId+'.json'),JSON.stringify({
      id:sessionId,provider:'deepseek',model:model.id,pi:'0.87.0',api_base_url:apiBaseUrl,
      model_options:{thinking_level:thinkingLevel,max_output_tokens:maxOutputTokens},
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
      current.timings.final_message_id=null;current.timings.final_first_text_ms=null;
      if(current.status!=='failed')current.status='cancelled';
      if(modelTiming)onEvent({type:'answer_end',messageId:modelTiming.id,stopReason:'aborted'});
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
      startedClock=performance.now();modelTiming=undefined;modelReasoning=undefined;textBlocks=new Map();
      const turn={id:randomUUID(),question,started_at:new Date().toISOString(),status:'running',events:[],
        reasoning:[],
        timings:{clock:'monotonic_ms_since_turn_start',models:[],tools:[],generation_end_ms:null,
          save_started_ms:null,save_finished_ms:null,final_message_id:null,final_first_text_ms:null}};
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
        if(turn.status==='completed' && modelTiming){
          turn.timings.final_message_id=modelTiming.id;
          turn.timings.final_first_text_ms=modelTiming.first_public_text_ms;
        }
        if(turn.failure)turn.error=turn.failure;
        else if(last?.stopReason==='length')turn.error='模型输出达到长度限制，本轮未完整回答。';
        else if(turn.status==='failed')turn.error='本轮未取得正常结束的完整回答。';
        if (agent.state.errorMessage && !turn.failure) turn.error=redact(agent.state.errorMessage);
      } catch(error) {
        turn.status=turn.cancelled?'cancelled':'failed';turn.error=redact(error.message);
      } finally {
        // 断流或取消没有完整结束消息时，也保留已经收到的推理并标为不完整。
        modelReasoning?.finish();
        if(turn.cancelled || turn.status==='failed')turn.answer='';
        turn.timings.generation_end_ms=elapsed();
        turn.completed_at=new Date().toISOString();
        try {
          const cancelledBeforeSave=Boolean(turn.cancelled);
          turn.timings.save_started_ms=elapsed();
          await save();
          // 保存期间仍允许停止；第一次写入可能已序列化，需补存取消状态。
          if(turn.cancelled && !cancelledBeforeSave)await save();
        }
        catch(error){turn.status='failed';turn.answer='';turn.error='历史记录保存失败：'+redact(error.message);}
        finally {
          // 落盘结束时间只能在写入返回后测得，随本轮返回；磁盘快照该值保持 null。
          turn.timings.save_finished_ms=elapsed();
          if(turn.status!=='completed'){turn.timings.final_message_id=null;turn.timings.final_first_text_ms=null;}
          current=null;busy=false;textBlocks.clear();
        }
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
