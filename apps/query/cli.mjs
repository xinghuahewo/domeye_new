import { createInterface } from 'node:readline/promises';
import { readFile } from 'node:fs/promises';
import { createDomeyeAgent, loadModelConfig } from './agent.mjs';
import { runtimePaths } from './runtime/settings.mjs';

const args=process.argv.slice(2);
const option=name=>args.includes(name)?args[args.indexOf(name)+1]:undefined;
const modelConfig=await loadModelConfig(process.env.DOMEYE_MODEL_CONFIG);
const agent=await createDomeyeAgent({
  modelConfig,datasetId:option('--dataset') ?? process.env.DOMEYE_QUERY_DATASET,
  apiBaseUrl:process.env.DOMEYE_QUERY_API_BASE_URL,
  historyDir:runtimePaths().historyDir,
  onEvent:event=>{
    if(event.type==='answer_final') process.stdout.write(event.text);
    if(event.type==='tool_execution_start') process.stderr.write(`\n[${event.toolName}]\n`);
  }
});
const interrupt=()=>{if(agent.running)agent.stop();else process.exit(0);};
process.on('SIGINT',interrupt);
async function ask(question) {
  process.stdout.write('\n用户：'+question+'\n\n');
  const result=await agent.ask(question);
  process.stdout.write('\n');
  process.stderr.write(JSON.stringify({session:agent.id,status:result.status,error:result.error})+'\n');
  if(result.status==='failed')process.exitCode=1;
  return result;
}
try {
  if(option('--questions')) {
    const questions=JSON.parse(await readFile(option('--questions'),'utf8'));
    if(!Array.isArray(questions)||questions.some(q=>typeof q!=='string'))throw new Error('问题文件须为字符串数组。');
    for(const question of questions){const result=await ask(question);if(result.status!=='completed')break;}
  } else if(option('--prompt')) await ask(option('--prompt'));
  else {
    const input=createInterface({input:process.stdin,output:process.stdout});
    input.on('SIGINT',interrupt);
    try {
      while(true){const question=await input.question('\n请输入问题（/quit 退出，Ctrl-C 停止）：');if(question==='/quit')break;if(question.trim())await ask(question);}
    } finally {input.close();}
  }
} finally {await agent.close();}
