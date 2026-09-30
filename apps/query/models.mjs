import { readFile, stat } from 'node:fs/promises';
import { deepseekProvider } from '@earendil-works/pi-ai/providers/deepseek';
import { zaiProvider } from '@earendil-works/pi-ai/providers/zai';
import { configuredThinkingLevel } from './runtime/model-options.mjs';

const catalog = [
  {id:'deepseek-v4-pro',label:'DeepSeek V4 Pro',provider:'deepseek',model:'deepseek-v4-pro'},
  {id:'deepseek-flash',label:'DeepSeek V4.1 Flash',provider:'deepseek',model:'deepseek-flash'},
  {id:'glm-5.3-flashx',label:'GLM-5.3-FlashX',provider:'zai',model:'glm-5.3-flashx'},
];
const endpoints = {deepseek:'https://api.deepseek.com',zai:'https://open.bigmodel.cn/api/paas/v4'};

export function publicModel(config) {
  if (!config || typeof config.provider!=='string' || typeof config.model!=='string') return null;
  const known = catalog.find(item=>item.provider===config.provider && item.model===config.model);
  return known ? {...known} : {id:config.model,label:config.model,provider:config.provider,model:config.model};
}

// 只接受宿主登记；页面只传模型 ID，不能注入地址、凭据或请求档位。
export function modelRegistry(config) {
  const entries = config?.models ?? (config ? [config] : []);
  if (!Array.isArray(entries) || !entries.length) throw new Error('须提供非空模型登记。');
  const models = entries.map(entry=>{
    const known = catalog.find(item=>item.model===entry?.model && item.provider===entry?.provider);
    if (!known || typeof entry.apiKey!=='string' || !entry.apiKey.trim()) throw new Error('模型登记须使用支持的提供方、模型和非空 apiKey。');
    const baseUrl = (entry.baseUrl ?? endpoints[known.provider]).replace(/\/$/,'');
    if (baseUrl!==endpoints[known.provider]) throw new Error('模型地址须为该提供方的官方 API 入口。');
    return Object.freeze({provider:known.provider,model:known.model,apiKey:entry.apiKey,baseUrl,
      thinkingLevel:configuredThinkingLevel(entry)});
  });
  if (new Set(models.map(item=>item.model)).size!==models.length) throw new Error('模型登记不能重复。');
  const defaultModel = config.models ? config.defaultModel : config.model;
  if (!models.some(item=>item.model===defaultModel)) throw new Error('默认模型须为已配置的模型。');
  return Object.freeze({defaultModel,models:Object.freeze(models)});
}

export function selectModel(registry, id = registry.defaultModel) {
  const model = registry.models.find(item=>item.model===id);
  if (!model) throw new Error('请选择已配置的模型。');
  return model;
}

export function listModels(registry) {
  return catalog.map(item=>({...item,available:registry.models.some(config=>config.model===item.id)}));
}

export async function loadModelRegistry(path) {
  if (!path) throw new Error('请通过 DOMEYE_MODEL_CONFIG 指定宿主的独立模型配置。');
  const info = await stat(path);
  if (!info.isFile() || (info.mode & 0o077)) throw new Error('模型配置须为仅当前用户可读写的独立普通文件。');
  return modelRegistry(JSON.parse(await readFile(path,'utf8')));
}

// CLI 与既有隔离验证保留单模型入口；省略 ID 时选择宿主默认值。
export async function loadModelConfig(path, id) {
  return selectModel(await loadModelRegistry(path), id);
}

export function queryProvider(provider) {
  if (provider==='deepseek') return deepseekProvider();
  if (provider!=='zai') throw new Error('未登记的模型提供方。');
  const adapter = zaiProvider();
  const models = adapter.getModels();
  const flash = models.find(model=>model.id==='glm-5.3-flash');
  if (!flash) throw new Error('锁定 SDK 缺少 GLM 传输适配。');
  // FlashX 复用 Flash 的文本/工具协议，不借用其费率。SDK 只接受 number；
  // 未登记的费用用 NaN 传播未知，JSON 历史中为 null，不能记成免费或实际账单。
  const cost = {input:NaN,output:NaN,cacheRead:NaN,cacheWrite:NaN};
  return {...adapter,getModels:()=>[...models,{...flash,id:'glm-5.3-flashx',name:'GLM-5.3-FlashX',baseUrl:endpoints.zai,cost}]};
}
