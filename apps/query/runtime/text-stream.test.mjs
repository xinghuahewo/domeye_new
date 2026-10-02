import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createTextRedactor} from './text-stream.mjs';

test('已知凭据在每个分割位置及单字符分片都被隐藏，正常短句立即通过',()=>{
  const key='synthetic-abab-key';
  for(let cut=0;cut<=key.length;cut++){
    const stream=createTextRedactor(key);
    assert.equal(stream.push('短句'),'短句');
    const result=stream.push(key.slice(0,cut))+stream.push(key.slice(cut)) +stream.finish();
    assert.equal(result,'[已隐藏凭据]');
  }
  const stream=createTextRedactor(key);
  assert.equal([...('前'+key+'中'+key+'尾')].map(char=>stream.push(char)).join('')+stream.finish(),'前[已隐藏凭据]中[已隐藏凭据]尾');
});

test('正常结束释放未构成凭据的尾部前缀，不丢字',()=>{
  const stream=createTextRedactor('ababac');
  assert.equal(stream.push('答案aba'),'答案');
  assert.equal(stream.push('x'),'abax');
  assert.equal(stream.push('ab'),'');
  assert.equal(stream.finish(),'ab');
});

test('多提供方凭据在同一条流中跨分片隐藏，切换模型后仍保护全部已登记密钥',()=>{
  const keys=['synthetic-ds-key','synthetic-glm-key'];
  const source='开始'+keys[1]+'然后'+keys[0]+'结束';
  for(let cut=0;cut<source.length;cut++){
    const stream=createTextRedactor(keys);
    assert.equal(stream.push(source.slice(0,cut))+stream.push(source.slice(cut))+stream.finish(),'开始[已隐藏凭据]然后[已隐藏凭据]结束');
  }
});
