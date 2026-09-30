import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,chmod,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {modelRegistry,listModels,selectModel,loadModelRegistry,loadModelConfig} from './models.mjs';

const pro={provider:'deepseek',model:'deepseek-v4-pro',apiKey:'synthetic-ds-key'};
const flash={...pro,model:'deepseek-flash'};
const glm={provider:'zai',model:'glm-5.3-flashx',apiKey:'synthetic-glm-key'};

test('三模型登记与默认选择；公开目录没有地址或密钥，旧配置保持原默认值',()=>{
  const registry=modelRegistry({defaultModel:pro.model,models:[pro,flash,glm]});
  assert.equal(selectModel(registry).model,pro.model);
  assert.equal(selectModel(registry,glm.model).baseUrl,'https://open.bigmodel.cn/api/paas/v4');
  assert.equal(selectModel(registry,flash.model).thinkingLevel,'high');
  assert.deepEqual(listModels(registry).map(item=>[item.id,item.available]),[
    ['deepseek-v4-pro',true],['deepseek-flash',true],['glm-5.3-flashx',true],
  ]);
  const publicJson=JSON.stringify(listModels(registry));
  for(const secret of [pro.apiKey,glm.apiKey,'baseUrl','thinkingLevel'])assert.ok(!publicJson.includes(secret));
  const legacy=modelRegistry({...pro,thinkingLevel:'low'});
  assert.equal(selectModel(legacy).thinkingLevel,'low');
  assert.equal(listModels(legacy).filter(item=>item.available).length,1);
  for(const id of [null,'glm-5.3-flashx','unknown',{},''])assert.throws(()=>selectModel(legacy,id));
});

test('拒绝未配置默认值、重复登记、伪造提供方、非官方入口及未知档位',()=>{
  for(const config of [null,{models:[]},
    {models:[pro],defaultModel:glm.model},{models:[pro,pro],defaultModel:pro.model},
    {...glm,provider:'deepseek'},{...pro,apiKey:''},{...pro,model:'ds-4.1-pro'},
    {...pro,baseUrl:'https://api.deepseek.com/other'},
    {...glm,baseUrl:'https://open.bigmodel.cn.evil.test/api/paas/v4'},
    {...glm,baseUrl:'https://secret@open.bigmodel.cn/api/paas/v4'},
    {...pro,thinkingLevel:'fast'},
  ])assert.throws(()=>modelRegistry(config));
});

test('宿主文件权限与 CLI 选择；没有凭据的模型不会回退到默认模型',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'domeye-models-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const path=join(dir,'models.json');
  await writeFile(path,JSON.stringify({defaultModel:pro.model,models:[pro,glm]}),{mode:0o600});
  assert.equal((await loadModelRegistry(path)).models.length,2);
  assert.equal((await loadModelConfig(path,glm.model)).provider,'zai');
  await assert.rejects(loadModelConfig(path,flash.model),/请选择已配置/);
  await chmod(path,0o644);await assert.rejects(loadModelRegistry(path),/仅当前用户/);
});
