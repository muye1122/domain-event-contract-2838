import React, { useEffect, useState } from 'react';

const API = '/api/v1';

async function api(p, options) {
  const res = await fetch(API + p, options);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!res.ok) {
    const err = new Error(data?.message || '请求失败');
    err.code = data?.code;
    err.status = res.status;
    throw err;
  }
  return data;
}
const post = (p, body) => api(p, {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}),
});

const ACTIONS = [
  ['sign-contract', '签署合同'], ['assign-lead', '分配线索'], ['recycle-lead', '回收线索'],
  ['approve-quote', '审批报价'], ['change-room-status', '房间状态变更'],
  ['publish-demand', '发布需求'], ['approve-certification', '认证通过'],
];
const EVENT_TYPES = [
  'contract.signed', 'lead.assigned', 'lead.recycled', 'quote.approved',
  'room.status_changed', 'demand.published', 'certification.approved',
];

function Panel({ title, children, testid, note }) {
  return (
    <section className="panel" data-testid={testid}>
      <h3>{title}</h3>
      {note && <p className="tiny">{note}</p>}
      {children}
    </section>
  );
}
function ErrorLine({ msg }) { return msg ? <div className="error" data-testid="error-line">{msg}</div> : null; }

function ContractsPanel() {
  const [data, setData] = useState(null);
  const [detail, setDetail] = useState(null);
  const [msg, setMsg] = useState('');
  useEffect(() => { api('/contracts').then(setData).catch((e) => setMsg(e.message)); }, []);
  const loadSchema = async (t) => {
    setMsg('');
    try { const r = await api('/contracts/' + encodeURIComponent(t)); setDetail(r.schema); }
    catch (e) { setMsg(e.message); }
  };
  return (
    <div>
      <Panel title="领域事件契约 · JSON Schema" testid="panel-contracts"
        note="7 种事件各有独立 schema；公共字段 event_id / event_type / version / occurred_at / org_id / site_id / payload。schema 允许未知字段（v1 消费者前向兼容）。">
        {!data ? <div className="loading">加载中…</div> : (
          <table data-testid="contract-table">
            <thead><tr><th>事件类型</th><th>标题</th><th>公共必填</th><th>payload 必填</th><th>操作</th></tr></thead>
            <tbody>
              {data.contracts.map((c) => (
                <tr key={c.event_type} data-testid={'contract-row-' + c.event_type}>
                  <td className="mono">{c.event_type}</td><td>{c.title}</td>
                  <td className="mono tiny">{c.required.join(', ')}</td>
                  <td className="mono tiny">{c.payload_required.join(', ')}</td>
                  <td><button data-testid={'contract-schema-btn-' + c.event_type} onClick={() => loadSchema(c.event_type)}>查看 schema</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <ErrorLine msg={msg} />
        {detail && <pre data-testid="contract-schema-json">{JSON.stringify(detail, null, 2)}</pre>}
      </Panel>
    </div>
  );
}

function StreamPanel() {
  const [stream, setStream] = useState(null);
  const [produced, setProduced] = useState(null);
  const [msg, setMsg] = useState('');
  const load = () => api('/stream').then(setStream).catch((e) => setMsg(e.message));
  useEffect(() => { load(); }, []);
  const run = async (action) => {
    setMsg('');
    try {
      const r = await post('/business/' + action, {});
      setProduced(r);
      if (!r.ok) setMsg(`拒绝 [${r.code}]：${r.reason}`);
      load();
    } catch (e) { setMsg(e.message); }
  };
  return (
    <div>
      <Panel title="生产者 · 触发业务动作后写入事件总线" testid="panel-stream"
        note="内存 Redis Stream 模拟：事件先按 schema 校验，通过后才 XADD；非法事件进入死信队列。">
        <div className="controls">
          {ACTIONS.map(([key, label]) => (
            <button key={key} className="primary" data-testid={'action-' + key} onClick={() => run(key)}>{label}</button>
          ))}
          <button data-testid="stream-refresh" onClick={load}>刷新总线</button>
        </div>
        {produced && <div className="result" data-testid="produced-result">
          {produced.ok
            ? <>已生产 <b>{produced.event.event_type}</b> · event_id <span className="mono">{produced.event.event_id}</span> · stream entry {produced.stream_id}</>
            : <>生产失败 [{produced.code}]：{produced.reason}</>}
        </div>}
        <ErrorLine msg={msg} />
      </Panel>
      <Panel title="事件总线 · domain.events" testid="panel-bus">
        <div className="tiny" data-testid="stream-count">当前事件 {stream ? stream.count : '—'} 条</div>
        {stream && <table data-testid="stream-table"><thead><tr><th>entry id</th><th>event_id</th><th>event_type</th><th>version</th><th>org_id</th><th>site_id</th><th>occurred_at</th></tr></thead><tbody>
          {stream.entries.map((e) => (
            <tr key={e.id} data-testid={'stream-row-' + e.data.event_id}>
              <td className="mono">{e.id}</td><td className="mono">{e.data.event_id}</td>
              <td className="mono">{e.data.event_type}</td><td>{e.data.version}</td>
              <td>{e.data.org_id || '—'}</td><td>{e.data.site_id}</td><td className="tiny">{e.data.occurred_at}</td>
            </tr>
          ))}
        </tbody></table>}
      </Panel>
    </div>
  );
}

function ConsumersPanel() {
  const [sample, setSample] = useState('contract.signed');
  const [target, setTarget] = useState('all');
  const [result, setResult] = useState(null);
  const [processed, setProcessed] = useState([]);
  const [msg, setMsg] = useState('');
  const load = () => api('/consumers').then((r) => setProcessed(r.processed)).catch((e) => setMsg(e.message));
  useEffect(() => { load(); }, []);
  const deliver = async (whichSample, whichTarget) => {
    setMsg('');
    try {
      const path = whichTarget === 'all' ? '/consume/all' : '/consume/' + whichTarget;
      const r = await post(path, { sample: whichSample });
      setResult(r);
      load();
      if (whichTarget === 'all' && r.results.some((x) => !x.ok)) setMsg('部分投递被拒绝，原因已进入死信队列。');
    } catch (e) { setMsg(`错误 [${e.code}]：${e.message}`); }
  };
  return (
    <div>
      <Panel title="消费者 · SOP派发 / 房间状态更新 / 付款计划生成" testid="panel-consumers"
        note="把样例事件投递给消费者；v1 消费者忽略未知字段；校验失败的事件进入死信队列，不阻塞后续事件。">
        <div className="controls">
          <label>样例事件<select data-testid="consume-sample" value={sample} onChange={(e) => setSample(e.target.value)}>
            {[...EVENT_TYPES, 'unknown_fields', 'missing_org_id'].map((s) => <option key={s} value={s}>{s}</option>)}
          </select></label>
          <label>消费者<select data-testid="consume-target" value={target} onChange={(e) => setTarget(e.target.value)}>
            <option value="all">全部三个</option>
            <option value="sop_dispatch">SOP派发</option>
            <option value="room_status_update">房间状态更新</option>
            <option value="payment_plan_generation">付款计划生成</option>
          </select></label>
          <button className="primary" data-testid="consume-run" onClick={() => deliver(sample, target)}>投递事件</button>
          <button data-testid="consume-contract-all" onClick={() => deliver('contract.signed', 'all')}>合同签署投递给全部消费者</button>
        </div>
        {result && <div className="result" data-testid="consume-result">
          投递 {Array.isArray(result.results) ? result.results.length : 1} 次 ·
          成功 {(result.results || [result]).filter((r) => r.ok).length} 次 ·
          <span className="mono">{JSON.stringify(result.results || result)}</span>
        </div>}
        <ErrorLine msg={msg} />
      </Panel>
      <Panel title="消费者处理记录" testid="panel-processed">
        <div className="tiny" data-testid="processed-count">已处理 {processed.length} 条</div>
        <table data-testid="processed-table"><thead><tr><th>消费者</th><th>event_id</th><th>event_type</th><th>处理结果</th></tr></thead><tbody>
          {processed.map((r) => (
            <tr key={r.id} data-testid={'processed-row-' + r.id}>
              <td>{r.consumer_name} <span className="tiny mono">({r.consumer})</span></td>
              <td className="mono">{r.event_id}</td><td className="mono">{r.event_type}</td>
              <td className="mono tiny">{JSON.stringify(r.outcome)}</td>
            </tr>
          ))}
        </tbody></table>
      </Panel>
    </div>
  );
}

function DlqPanel() {
  const [entries, setEntries] = useState([]);
  const [result, setResult] = useState(null);
  const load = () => api('/dlq').then((r) => setEntries(r.entries)).catch(() => {});
  useEffect(() => { load(); }, []);
  const inject = async () => {
    try { setResult(await post('/consume/all', { sample: 'missing_org_id' })); }
    catch (e) { setResult({ error: e.message }); }
    load();
  };
  const batch = async () => {
    try {
      const samples = await api('/samples');
      setResult(await post('/consume/batch', { events: [samples.edge_cases.missing_org_id, samples.events[0]] }));
    } catch (e) { setResult({ error: e.message }); }
    load();
  };
  return (
    <div>
      <Panel title="死信队列 · invalid events with reason" testid="panel-dlq"
        note="缺少 org_id 的事件被拒绝进入死信队列，reason 非空；后续合法事件照常处理。">
        <div className="controls">
          <button data-testid="dlq-inject" onClick={inject}>投递缺 org_id 事件</button>
          <button data-testid="dlq-batch" onClick={batch}>批量投递：坏事件 + 正常事件</button>
        </div>
        {result && <div className="result" data-testid="dlq-result"><span className="mono">{JSON.stringify(result)}</span></div>}
      </Panel>
      <Panel title="死信队列记录" testid="panel-dlq-list">
        <div className="tiny" data-testid="dlq-count">死信 {entries.length} 条</div>
        <table data-testid="dlq-table"><thead><tr><th>id</th><th>source</th><th>event_type</th><th>reason</th><th>at</th></tr></thead><tbody>
          {entries.map((d, i) => (
            <tr key={d.id} data-testid={'dlq-row-' + d.id}>
              <td className="mono">{d.id}</td><td>{d.source}</td>
              <td className="mono">{d.event?.event_type || '—'}</td>
              <td className="danger" data-testid={i === 0 ? 'first-dlq-reason' : undefined}>{d.reason}</td>
              <td className="tiny">{d.at}</td>
            </tr>
          ))}
        </tbody></table>
      </Panel>
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState('contracts');
  const [meta, setMeta] = useState(null);
  useEffect(() => { api('/meta').then(setMeta).catch(() => {}); }, []);
  const tabs = [['contracts', '事件契约'], ['stream', '生产者与总线'], ['consumers', '消费者'], ['dlq', '死信队列']];
  return (
    <div className="app">
      <header>
        <div className="brand"><span className="logo">契</span><div>
          <h1>骑手招募 · 领域事件契约验证</h1><p>跨模块事件契约 / 内存事件总线 / 生产者与消费者 / 死信队列</p>
        </div></div>
        <div className="meta">虚构数据 · 演示级交付 · 内存事件总线</div>
      </header>
      <nav>{tabs.map(([k, label]) => (
        <button key={k} data-testid={'tab-' + k} className={tab === k ? 'active' : ''} onClick={() => setTab(k)}>{label}</button>
      ))}</nav>
      <main>
        {tab === 'contracts' && <ContractsPanel />}
        {tab === 'stream' && <StreamPanel />}
        {tab === 'consumers' && <ConsumersPanel />}
        {tab === 'dlq' && <DlqPanel />}
      </main>
      <footer>{meta ? meta.demo_boundary : '演示级交付'} · 无真实个人信息 · 无境外 LLM 调用</footer>
    </div>
  );
}
