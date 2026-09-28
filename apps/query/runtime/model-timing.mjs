// 客户端接收时刻，不代表提供方内部排队、预填充或 GPU 运算用时。
// 只保留计数和时间；不保留增量正文、推理、工具参数、请求正文或请求头。
const kindNames = ['thinking','toolcall','text'];
const emptyKind = () => ({first_delta_ms:null,last_delta_ms:null,delta_count:0,utf16_units:0});
export function createModelTiming(round, now) {
  const stream = round.stream = {version:1,first_delta_type:null,last_delta_ms:null,
    kinds:Object.fromEntries(kindNames.map(kind=>[kind,emptyKind()])),
    phases:[],omitted_phases:0,max_delta_gap:null,usage:null,usage_source:'sdk_normalized'};
  round.requests = [];
  let previous, phase;
  return {
    observe(event) {
      const kind = event.type.endsWith('_delta') ? event.type.slice(0,-6) : '';
      if(!kindNames.includes(kind) || typeof event.delta!=='string' || !event.delta.length)return;
      const at=now(),stats=stream.kinds[kind];
      round.first_token_ms ??= at;
      stream.first_delta_type ??= kind;stream.last_delta_ms=at;
      stats.first_delta_ms ??= at;stats.last_delta_ms=at;stats.delta_count++;stats.utf16_units+=event.delta.length;
      if(previous){
        const gap={from_kind:previous.kind,to_kind:kind,from_ms:previous.at,to_ms:at,ms:Math.round((at-previous.at)*1000)/1000};
        if(!stream.max_delta_gap || gap.ms>stream.max_delta_gap.ms)stream.max_delta_gap=gap;
      }
      if(!phase || phase.kind!==kind){
        phase={kind,...emptyKind()};
        if(stream.phases.length<256)stream.phases.push(phase);else stream.omitted_phases++;
      }
      phase.first_delta_ms ??= at;phase.last_delta_ms=at;phase.delta_count++;phase.utf16_units+=event.delta.length;
      previous={kind,at};
    },
    finish(message) {
      // 此 DeepSeek SDK 只有解析 usage chunk 后才添加 reasoning 字段。
      // SDK 会将部分缺失字段归零，因此这些是归一化值，不能当成原始字段存在性证明。
      if(!Object.hasOwn(message.usage??{},'reasoning'))return;
      stream.usage=Object.fromEntries(['input','output','reasoning','cacheRead','cacheWrite','totalTokens']
        .map(key=>[key,Number.isFinite(message.usage[key])?message.usage[key]:null]));
    },
    wrapFetch(fetcher) {
      return async(input,init)=>{
        const request={model_round_id:round.id,attempt:round.requests.length+1,started_ms:now(),
          headers_received_ms:null,status:null,outcome:'pending',
          request_bytes:typeof init?.body==='string'?Buffer.byteLength(init.body):null};
        round.requests.push(request);
        try{
          const response=await fetcher(input,init);
          request.headers_received_ms=now();request.status=response.status;request.outcome='response';
          return response; // 不读取、复制或包装响应流，保持 SDK 的消费和取消行为。
        }catch(error){
          request.ended_ms=now();request.outcome=init?.signal?.aborted?'aborted':'network_error';
          throw error;
        }
      };
    }
  };
}
