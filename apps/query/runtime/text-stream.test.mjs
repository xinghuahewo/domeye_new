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
