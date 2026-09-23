'use strict';
const $ = id => document.getElementById(id);
const ui = Object.fromEntries(['dataset','session-dataset','new-session','refresh-history','current-session','history','session-title','connection','notice','reading-pane','empty','messages','read-only','back-current','composer','question','send','stop','composer-hint'].map(id => [id, $(id)]));
let session = { id: null, busy: false, turns: [] }, readingId = null, streaming = false, switching = false, records = [], poll, syncVersion = 0;
function notice(message = '') { ui.notice.textContent = message; ui.notice.hidden = !message; }
async function api(path, value) {
  const response = await fetch(path, value === undefined ? {} : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(value) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || '请求没有完成，请稍后再试。');
  return data;
}
function text(tag, value, className) { const node = document.createElement(tag); node.textContent = value; if (className) node.className = className; return node; }
function inline(node, value) {
  let cursor = 0;
  for (const match of value.matchAll(/\*\*([^*]+)\*\*|`([^`]+)`/g)) {
    node.append(document.createTextNode(value.slice(cursor, match.index)));
    node.append(text(match[1] ? 'strong' : 'code', match[1] || match[2])); cursor = match.index + match[0].length;
  }
  node.append(document.createTextNode(value.slice(cursor)));
}
// 以 DOM 和 textContent 显示有限 Markdown；任何原始 HTML 都作为普通文字。
function markdown(target, source) {
  target.replaceChildren(); target.classList.remove('streaming');
  const lines = source.split('\n');
  const isTableRule = line => /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(line ?? '');
  const cells = line => line.trim().replace(/^\|/,'').replace(/\|$/,'').split('|').map(cell => cell.trim());
  for (let i = 0; i < lines.length;) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    if (/^```/.test(line)) {
      const contents = []; i++;
      while (i < lines.length && !/^```/.test(lines[i])) contents.push(lines[i++]);
      if (i < lines.length) i++;
      const pre = document.createElement('pre'); pre.append(text('code', contents.join('\n'))); target.append(pre); continue;
    }
    if (isTableRule(lines[i + 1])) {
      const wrap = text('div','', 'table-wrap'), table = document.createElement('table'), head = document.createElement('thead'), body = document.createElement('tbody');
      const row = (items, tag) => { const tr = document.createElement('tr'); for (const value of items) { const td = document.createElement(tag); inline(td, value); tr.append(td); } return tr; };
      head.append(row(cells(line), 'th')); i += 2;
      while (i < lines.length && lines[i].includes('|') && lines[i].trim()) body.append(row(cells(lines[i++]), 'td'));
      table.append(head, body); wrap.append(table); target.append(wrap); continue;
    }
    const heading = /^(#{1,4})\s+(.+)$/.exec(line);
    if (heading) { const node = document.createElement(heading[1].length < 3 ? 'h3' : 'h4'); inline(node,heading[2]); target.append(node); i++; continue; }
    const bullet = /^\s*([-*]|\d+[.)])\s+(.+)$/.exec(line);
    if (bullet) {
      const ordered = /^\d/.test(bullet[1]), list = document.createElement(ordered ? 'ol' : 'ul');
      while (i < lines.length) { const item = /^\s*([-*]|\d+[.)])\s+(.+)$/.exec(lines[i]); if (!item || /^\d/.test(item[1]) !== ordered) break; const li = document.createElement('li'); inline(li,item[2]); list.append(li); i++; }
      target.append(list); continue;
    }
    const paragraph = [line]; i++;
    while (i < lines.length && lines[i].trim() && !/^(#{1,4}\s|```|\s*([-*]|\d+[.)])\s)/.test(lines[i]) && !isTableRule(lines[i + 1])) paragraph.push(lines[i++]);
    const p = document.createElement('p'); inline(p,paragraph.join('\n')); target.append(p);
  }
}
function turnNode(turn) {
  const article = text('article','','turn');
  article.append(text('h2',turn.question,'question'),text('div','Domeye','answer-label'));
  const answer = text('div','','answer'); markdown(answer,turn.answer || '');
  const tool = text('div','','tool-status'), status = text('div','','turn-status');
  if (turn.status === 'cancelled') { status.textContent = '已停止 · 本次回答未完成'; status.classList.add('warning'); }
  if (turn.status === 'failed') { status.textContent = turn.failureReason || '回答未完成，可以继续追问或重新尝试。'; status.classList.add('warning'); }
  article.append(answer,tool,status); return { article,answer,tool,status };
}
function paintTurns(turns) {
  ui.messages.replaceChildren(); ui.empty.hidden = turns.length > 0;
  for (const turn of turns) ui.messages.append(turnNode(turn).article);
}
function scrollToBottom(force = false) {
  const pane = ui['reading-pane'];
  if (force || pane.scrollHeight - pane.scrollTop - pane.clientHeight < 180) pane.scrollTop = pane.scrollHeight;
}
function controls() {
  const busy = session.busy || streaming || switching;
  ui['new-session'].disabled = busy; ui['refresh-history'].disabled = busy;
  ui.dataset.disabled = busy;
  ui['current-session'].disabled = streaming || switching;
  ui.composer.hidden = Boolean(readingId); ui['read-only'].hidden = !readingId;
  ui.question.disabled = busy; ui.send.hidden = session.busy || streaming; ui.send.disabled = busy || !session.id || !ui.question.value.trim();
  ui.stop.hidden = !session.busy && !streaming; ui.stop.disabled = switching;
  ui.connection.textContent = session.busy || streaming ? '正在回答' : '已连接';
  ui['current-session'].classList.toggle('selected',!readingId);
  ui['current-session'].querySelector('span').textContent = session.busy ? '回答中' : '可继续';
  for (const button of ui.history.querySelectorAll('button')) button.disabled = busy;
}
function paintHistory() {
  ui.history.replaceChildren();
  if (!records.length) ui.history.append(text('p','还没有保存的会话','history-empty'));
  for (const item of records) {
    const button = text('button','','history-item'); button.type = 'button'; button.classList.toggle('selected',readingId === item.id);
    button.append(text('strong',item.title || '未命名会话'),text('small',new Date(item.updatedAt).toLocaleDateString('zh-CN',{month:'short',day:'numeric'}) + ' · ' + item.count + ' 次提问'),text('small',item.dataset?.label || '早期记录 · 数据批次未标注'));
    button.addEventListener('click',()=>openHistory(item.id)); ui.history.append(button);
  }
  controls();
}
async function refreshHistory() { const data = await api('/api/history'); records = data.records; paintHistory(); }
async function syncCurrent() {
  const version = ++syncVersion, updated = await api('/api/session');
  if (version !== syncVersion) return;
  session = updated;
  if (!readingId && !streaming) { paintTurns(session.turns); ui['session-title'].textContent = '当前会话'; ui['session-dataset'].textContent = session.dataset?.label || ''; }
  controls();
  clearTimeout(poll);
  if (session.busy && !streaming) poll = setTimeout(()=>syncCurrent().catch(error=>notice(error.message)),1200);
}
async function openHistory(id) {
  if (streaming || session.busy || switching) return;
  switching = true; controls(); notice();
  try {
    const data = await api('/api/history/' + encodeURIComponent(id)); readingId = id;
    ui['session-dataset'].textContent = data.dataset?.label || '早期记录 · 数据批次未标注';
    paintTurns(data.turns); ui['session-title'].textContent = data.turns[0]?.question || '历史会话'; paintHistory(); scrollToBottom(true);
  } catch (error) { notice(error.message); } finally { switching = false; controls(); }
}
async function backCurrent() {
  if (streaming || switching) return;
  switching = true; controls();
  try { readingId = null; notice(); await syncCurrent(); paintHistory(); scrollToBottom(true); }
  finally { switching = false; controls(); }
}
async function newSession() {
  if (streaming || session.busy || switching) return;
  syncVersion++; clearTimeout(poll);
  switching = true; controls(); notice();
  try { session = await api('/api/session',{datasetId:ui.dataset.value || 'completed-files'}); readingId = null; ui.question.value = ''; paintTurns([]); ui['session-title'].textContent = '当前会话'; ui['session-dataset'].textContent = session.dataset?.label || ''; await refreshHistory(); }
  catch (error) { notice(error.message); }
  finally { switching = false; controls(); ui.question.focus(); }
}
async function submit(event) {
  event.preventDefault();
  const question = ui.question.value.trim();
  if (!question || !session.id || session.busy || streaming || switching || readingId) return;
  syncVersion++; clearTimeout(poll);
  notice(); streaming = true; session.busy = true; controls(); ui.empty.hidden = true;
  const nodes = turnNode({question,answer:'',status:'running'}); nodes.answer.classList.add('streaming'); nodes.tool.textContent = '正在理解问题'; nodes.tool.classList.add('running');
  ui.messages.append(nodes.article); ui.question.value = ''; scrollToBottom(true);
  let answer = '', completed = false;
  try {
    const response = await fetch('/api/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sessionId:session.id,question})});
    if (!response.ok) throw new Error((await response.json()).error || '请求没有开始。');
    const reader = response.body.getReader(), decoder = new TextDecoder(); let pending = '';
    const receive = item => {
      if (item.type === 'text') { answer += item.text; nodes.answer.textContent = answer; }
      if (item.type === 'tool') { nodes.tool.textContent = item.label + (item.state === 'running' ? '…' : item.state === 'failed' ? ' · 暂未完成' : ' · 已完成'); nodes.tool.classList.toggle('running',item.state === 'running'); }
      if (item.type === 'done') { const replacement = turnNode(item.turn); nodes.article.replaceWith(replacement.article); completed = true; notice(); }
      if (item.type === 'error') throw new Error(item.message);
      scrollToBottom();
    };
    try {
      while (true) { const {value,done} = await reader.read(); pending += decoder.decode(value,{stream:!done}); let end; while ((end=pending.indexOf('\n')) >= 0) { const line = pending.slice(0,end); pending = pending.slice(end+1); if (line) receive(JSON.parse(line)); } if (done) break; }
      if (!completed) throw new Error('连接已结束，本次回答可能尚未完成。');
    } finally { await reader.cancel().catch(()=>{}); reader.releaseLock(); }
  } catch (error) { nodes.tool.classList.remove('running'); nodes.status.textContent = '回答未完成'; nodes.status.classList.add('warning'); notice(error.message); }
  finally {
    streaming = false;
    try { await syncCurrent(); await refreshHistory(); } catch { session.busy = false; notice('暂时无法连接问数服务。请确认入口仍在运行，再刷新页面。'); }
    controls(); ui.question.focus();
  }
}
ui.composer.addEventListener('submit',submit);
ui.question.addEventListener('input',controls);
ui.question.addEventListener('keydown',event=>{if(event.key==='Enter'&&!event.shiftKey&&!event.isComposing){event.preventDefault();ui.composer.requestSubmit();}});
ui.stop.addEventListener('click',async()=>{ui.stop.disabled=true;notice('已请求停止，正在结束当前读取并保存记录。');try{await api('/api/stop',{sessionId:session.id});}catch(error){notice(error.message);ui.stop.disabled=false;}});
ui['new-session'].addEventListener('click',newSession);
ui['back-current'].addEventListener('click',()=>backCurrent().catch(error=>notice(error.message)));
ui['current-session'].addEventListener('click',()=>backCurrent().catch(error=>notice(error.message)));
ui['refresh-history'].addEventListener('click',()=>refreshHistory().catch(error=>notice(error.message)));
(async()=>{try{
  const {datasets} = await api('/api/datasets');
  for (const item of datasets) { const option = text('option',item.label); option.value = item.id; ui.dataset.append(option); }
  await syncCurrent();
  if (session.dataset) ui.dataset.value = session.dataset.id;
  if(!session.id)await newSession();await refreshHistory();
}catch(error){notice(error.message);ui.connection.textContent='未连接';}})();
