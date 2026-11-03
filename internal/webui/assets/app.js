'use strict';

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

const $ = (sel, root = document) => root.querySelector(sel);
const PALETTE = ['#2dd4bf', '#60a5fa', '#c4b5fd', '#fbbf24', '#fb7185', '#34d399', '#f472b6', '#94a3b8', '#facc15', '#38bdf8'];

const nf0 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 });
const nf2 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });

function h(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function svgEl(tag, attrs = {}) {
  const e = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
}

function fmtNum(v, precision = 0) {
  if (!isFinite(v)) return '—';
  if (precision === 0) return nf0.format(v);
  if (precision === 1) return nf1.format(v);
  return nf2.format(v);
}

function compact(v) {
  const a = Math.abs(v);
  if (a >= 1e7) return (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (a >= 1e4) return Math.round(v / 1e3) + 'k';
  if (a >= 1e3) return (v / 1e3).toFixed(1) + 'k';
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return (Math.round(v * 10) / 10).toString();
  if (a >= 1) return (Math.round(v * 100) / 100).toString();
  if (a === 0) return '0';
  return v.toFixed(2);
}

function fmtSim(ms) {
  if (ms >= 10000) return (ms / 1000).toFixed(1) + ' s';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + ' s';
  return Math.round(ms) + ' ms';
}

function niceMax(v) {
  if (!(v > 0)) return 1;
  const exp = Math.floor(Math.log10(v));
  const base = Math.pow(10, exp);
  const n = v / base;
  const step = n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10;
  return step * base;
}

async function getJSON(url) {
  const r = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!r.ok) throw new Error(await errorText(r));
  return r.json();
}

async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(await errorText(r));
  return r.json();
}

async function errorText(r) {
  try {
    const j = await r.json();
    if (j && j.error) return j.error;
  } catch (_) { /* not JSON */ }
  return `request failed (${r.status})`;
}

/* ------------------------------------------------------------------ */
/* state                                                               */
/* ------------------------------------------------------------------ */

const BASE_PLAY_SECONDS = 12;
const REDUCED_MOTION = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);

const state = {
  meta: null,
  catalog: [],
  scenario: null,
  seed: 42,
  knobs: {},
  result: null,
  lastRequest: null,
  frame: 0,
  playing: false,
  speed: 1,
  running: false,
  pin: null,
  hidden: new Set(),
  charts: [],
  diagram: null,
  lastTs: 0,
  raf: 0,
  pendingTime: null,
};

/* ------------------------------------------------------------------ */
/* boot                                                                */
/* ------------------------------------------------------------------ */

async function boot() {
  wireChrome();
  try {
    const [meta, cat] = await Promise.all([getJSON('/api/meta'), getJSON('/api/scenarios')]);
    state.meta = meta;
    state.catalog = cat.scenarios;
  } catch (err) {
    toast('Could not load the scenario catalog: ' + err.message);
    return;
  }
  renderMeta();
  renderScenarioList();
  const fromHash = parseHash();
  const fallback = state.catalog.find((s) => s.id === 'throttle') || state.catalog[0];
  const sc = state.catalog.find((s) => s.id === fromHash.scenario) || fallback;
  selectScenario(sc, fromHash);
}

function renderMeta() {
  const box = $('#header-meta');
  box.innerHTML = '';
  box.append(h('span', 'pill', 'v' + (state.meta.version || 'dev')));
  if (state.meta.dstseed_set) {
    const p = h('span', 'pill warn');
    p.append(h('span', 'dot'));
    p.append(document.createTextNode('DSTSEED=' + state.meta.dstseed + ' overrides seeds'));
    box.append(p);
  } else {
    const p = h('span', 'pill');
    p.append(h('span', 'dot'));
    p.append(document.createTextNode('deterministic'));
    box.append(p);
  }
}

/* ------------------------------------------------------------------ */
/* scenarios                                                           */
/* ------------------------------------------------------------------ */

function renderScenarioList() {
  const holder = $('#scenario-chips');
  holder.innerHTML = '';
  let group = null;
  for (const sc of state.catalog) {
    if (sc.group !== group) {
      group = sc.group;
      holder.append(h('span', 'chip-group', group));
    }
    const btn = h('button', 'chip', sc.title);
    btn.dataset.id = sc.id;
    btn.title = sc.subtitle;
    btn.setAttribute('role', 'tab');
    btn.addEventListener('click', () => {
      const active = state.scenario && state.scenario.id === sc.id;
      if (active) return;
      selectScenario(sc, { scenario: sc.id, seed: null, knobs: {} });
    });
    holder.append(btn);
  }
  markActiveScenario();
}

function markActiveScenario() {
  for (const b of document.querySelectorAll('.chip')) {
    const on = state.scenario && b.dataset.id === state.scenario.id;
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
  }
}

function selectScenario(sc, params) {
  state.scenario = sc;
  state.seed = params.seed !== null && params.seed !== undefined ? params.seed : sc.default_seed;
  state.knobs = { ...sc.preset };
  for (const k of sc.knobs) {
    if (params.knobs && Object.prototype.hasOwnProperty.call(params.knobs, k.key)) {
      const v = params.knobs[k.key];
      if (isFinite(v) && v >= k.min && v <= k.max) state.knobs[k.key] = v;
    }
  }
  $('#seed').value = String(state.seed);
  state.pin = null;
  state.pendingTime = params.time !== null && params.time !== undefined ? params.time : null;
  markActiveScenario();
  renderKnobs();
  renderScenarioHead();
  runScenario();
}

function renderScenarioHead() {
  const sc = state.scenario;
  const head = $('#scenario-head');
  head.innerHTML = '';
  head.append(h('h1', '', sc.title));
  const sub = h('div', 'sub');
  sub.append(document.createTextNode(sc.subtitle + ' '));
  const pill = h('span', 'pill warn');
  pill.id = 'modified-pill';
  pill.hidden = true;
  pill.append(h('span', 'dot'));
  pill.append(document.createTextNode('modified from preset'));
  sub.append(pill);
  head.append(sub);
  head.append(h('p', 'blurb', sc.blurb));
  const watch = h('div', 'watch');
  for (const w of sc.what_to_watch) watch.append(h('div', '', w));
  head.append(watch);
}

/* ------------------------------------------------------------------ */
/* knobs                                                               */
/* ------------------------------------------------------------------ */

function renderKnobs() {
  const sc = state.scenario;
  const holder = $('#knobs');
  holder.innerHTML = '';
  const groups = new Map();
  for (const k of sc.knobs) {
    if (!groups.has(k.group)) groups.set(k.group, []);
    groups.get(k.group).push(k);
  }
  let idx = 0;
  for (const [name, knobs] of groups) {
    const details = document.createElement('details');
    details.className = 'knob-group';
    const collapsedByDefault = name === 'Run' || name === 'History' || name === 'Broker';
    details.open = !collapsedByDefault;
    const sum = document.createElement('summary');
    sum.append(h('span', '', name));
    details.append(sum);
    const body = h('div', 'knob-body');
    for (const k of knobs) body.append(knobField(k));
    details.append(body);
    holder.append(details);
    idx += 1;
  }
}

function knobField(k) {
  const row = h('div', 'knob');
  row.dataset.key = k.key;
  const head = h('div', 'knob-head');
  const label = h('label', '', k.label);
  label.htmlFor = 'knob-' + k.key;
  const out = h('output');
  head.append(label, out);
  row.append(head);

  let input;
  if (k.kind === 'slider') {
    input = document.createElement('input');
    input.type = 'range';
    input.min = String(k.min);
    input.max = String(k.max);
    input.step = String(k.step);
    input.value = String(state.knobs[k.key]);
    input.addEventListener('input', () => onKnobChange(k, Number(input.value), out, row, input));
    row.append(input);
  } else {
    input = document.createElement('input');
    input.type = 'number';
    input.min = String(k.min);
    input.max = String(k.max);
    input.step = String(k.step);
    input.value = String(state.knobs[k.key]);
    input.addEventListener('input', () => {
      const v = Number(input.value);
      if (input.value === '' || !isFinite(v)) return;
      onKnobChange(k, v, out, row, input);
    });
    input.addEventListener('change', () => {
      if (input.value === '') input.value = String(state.knobs[k.key]);
    });
    row.append(input);
  }
  input.id = 'knob-' + k.key;
  const help = h('p', 'knob-help', k.help);
  row.append(help);
  syncKnob(k, out, row, input);
  return row;
}

function onKnobChange(k, value, out, row, input) {
  state.knobs[k.key] = value;
  syncKnob(k, out, row, input);
}

function isDirty() {
  const req = state.lastRequest;
  if (!req) return true;
  if (state.scenario.id !== req.scenario || state.seed !== req.seed) return true;
  const keys = new Set([...Object.keys(state.knobs), ...Object.keys(req.knobs)]);
  for (const k of keys) {
    if (Math.abs((state.knobs[k] ?? 0) - (req.knobs[k] ?? 0)) > 1e-9) return true;
  }
  return false;
}

function markDirty() {
  const dirty = !!state.result && isDirty();
  const btn = $('#run-btn');
  btn.classList.toggle('dirty', dirty);
  btn.title = dirty ? 'Knobs changed since this run — press Run (⌘/Ctrl+Enter)' : 'Run this scenario (⌘/Ctrl+Enter)';
  const pill = $('#modified-pill');
  if (pill) pill.hidden = !dirty;
}

function syncKnob(k, out, row, input) {
  const v = state.knobs[k.key];
  out.innerHTML = '';
  out.append(document.createTextNode(fmtKnob(v, k.step)));
  if (k.unit) out.append(h('span', 'u', k.unit));
  row.classList.toggle('modified', Math.abs(v - state.scenario.preset[k.key]) > 1e-9);
  if (input.type === 'range') {
    const pct = ((v - k.min) / (k.max - k.min)) * 100;
    input.style.setProperty('--pct', pct.toFixed(1) + '%');
    if (String(v) !== input.value) input.value = String(v);
  }
  markDirty();
}

function fmtKnob(v, step) {
  if (step >= 1) return nf0.format(v);
  if (step >= 0.1) return nf1.format(v);
  return nf2.format(v);
}

function resetKnobs() {
  state.knobs = { ...state.scenario.preset };
  renderKnobs();
}

/* ------------------------------------------------------------------ */
/* running                                                             */
/* ------------------------------------------------------------------ */

async function runScenario() {
  if (state.running) return;
  const seedInput = Number($('#seed').value);
  if (isFinite(seedInput)) state.seed = Math.trunc(seedInput);
  state.running = true;
  setBusy(true, `<b>Simulating</b> ${state.scenario.title} — deterministic event loop, no wall clock`);
  const req = currentRequest();
  try {
    const res = await postJSON('/api/runs', req);
    state.result = res;
    state.lastRequest = req;
    state.frame = 0;
    state.playing = true;
    state.lastTs = 0;
    state.hidden.clear();
    renderResult();
    updateHash();
    markDirty();
    if (state.pendingTime !== null) {
      seekToTime(state.pendingTime);
      state.pendingTime = null;
    }
  } catch (err) {
    toast('Run failed: ' + err.message);
  } finally {
    state.running = false;
    setBusy(false);
  }
}

function currentRequest() {
  return { scenario: state.scenario.id, seed: state.seed, knobs: { ...state.knobs } };
}

function setBusy(on, message) {
  $('#busy').classList.toggle('on', on);
  if (message) $('#busy-msg').innerHTML = message;
  $('#run-btn').disabled = on;
}

/* ------------------------------------------------------------------ */
/* result rendering                                                    */
/* ------------------------------------------------------------------ */

function renderResult() {
  const r = state.result;
  renderKpis();
  renderInsights();
  renderPercentiles();
  initDiagram();
  initCharts();
  renderDeterminism();
  renderPinButton();
  const bar = $('#playbar');
  bar.classList.add('visible');
  const scrub = $('#scrub');
  scrub.max = String(Math.max(0, r.t.length - 1));
  scrub.value = '0';
  $('#kpi-hint').textContent =
    `${r.model} · seed ${r.seed} · ${r.t.length} samples · simulation wall time ${r.wall_ms} ms`;
  setPlaying(!REDUCED_MOTION);
  renderFrame();
}

function renderKpis() {
  const r = state.result;
  const holder = $('#kpis');
  holder.innerHTML = '';
  for (const m of r.metrics) {
    const card = h('div', 'kpi ' + (m.tone || ''));
    card.append(h('div', 'k', m.label));
    const v = h('div', 'v');
    v.append(document.createTextNode(fmtNum(m.value, m.precision || 0)));
    if (m.unit) v.append(h('span', 'u', m.unit));
    card.append(v);
    const delta = h('div', 'delta');
    if (state.pin) {
      const pm = state.pin.metrics.find((x) => x.key === m.key);
      if (pm) {
        const d = m.value - pm.value;
        if (Math.abs(d) > 1e-9) {
          delta.textContent = `pinned ${fmtNum(pm.value, m.precision || 0)} (${d > 0 ? '+' : ''}${fmtNum(d, m.precision || 1)})`;
        } else {
          delta.textContent = 'same as pinned';
        }
      }
    }
    card.append(delta);
    if (m.help) card.title = m.help;
    holder.append(card);
  }
  const fanout = state.knobs.fanout_k || 0;
  if (r.model === 'temporal' && fanout > 0) {
    holder.title = `Fan-out ${fanout}: each workflow schedules ${fanout + 1} activity tasks per success.`;
  }
}

function renderInsights() {
  const holder = $('#insights');
  holder.innerHTML = '';
  const list = state.result.insights || [];
  if (list.length === 0) {
    holder.append(h('div', 'insight', 'Nothing surprising: the run stayed inside its normal regime.'));
    return;
  }
  for (const ins of list) {
    const box = h('div', 'insight ' + (ins.tone || ''));
    box.append(h('div', 't', ins.title));
    box.append(h('div', 'd', ins.detail));
    holder.append(box);
  }
}

function renderPercentiles() {
  const holder = $('#percentiles');
  holder.innerHTML = '';
  if (!state.result.percentiles || state.result.percentiles.length === 0) return;
  const table = h('table', 'mini');
  const thead = document.createElement('thead');
  const tr = document.createElement('tr');
  for (const t of ['Latency distribution', 'n', 'p50', 'p90', 'p99', 'max']) tr.append(h('th', '', t));
  thead.append(tr);
  table.append(thead);
  const tbody = document.createElement('tbody');
  for (const p of state.result.percentiles) {
    const row = document.createElement('tr');
    row.append(h('td', '', p.label));
    row.append(h('td', '', nf0.format(p.count)));
    row.append(h('td', '', fmtSim(p.p50_ms)));
    row.append(h('td', '', fmtSim(p.p90_ms)));
    row.append(h('td', '', fmtSim(p.p99_ms)));
    row.append(h('td', '', fmtSim(p.max_ms)));
    tbody.append(row);
  }
  table.append(tbody);
  holder.append(table);
}

function renderRunStatus() {
  const r = state.result;
  const holder = $('#run-status');
  holder.innerHTML = '';
  const line = h('div', 'status-line');
  if (r.model === 'temporal' && r.status) {
    const s = r.status;
    if (s.metastable) {
      let text = `Monitor declared metastable at ${fmtSim(s.metastable_at_ms)}`;
      if (s.recovered && s.recovery_time_ms > 0) text += `, recovered after ${fmtSim(s.recovery_time_ms)}`;
      else if (s.recovered) text += ', recovered';
      line.append(h('span', 'pill warn', text));
    } else {
      line.append(h('span', 'pill', 'monitor: no metastable regime detected'));
    }
  }
  if (r.notes && r.notes.length) {
    for (const n of r.notes) line.append(h('span', 'pill', n));
  }
  holder.append(line);
}

/* ------------------------------------------------------------------ */
/* diagram                                                             */
/* ------------------------------------------------------------------ */

const DIAGRAMS = {
  temporal: {
    w: 920,
    h: 330,
    nodes: [
      { id: 'load', x: 30, y: 36, w: 104, h: 58, kind: 'source', label: 'load', bind: ['started_rate'], fmt: 'rate' },
      { id: 'wfq', x: 186, y: 36, w: 138, h: 58, kind: 'queue', label: 'matching-wf', bind: ['wf_depth'], fmt: 'int', unit: 'tasks' },
      { id: 'wfpool', x: 376, y: 36, w: 140, h: 58, kind: 'pool', label: 'workflow workers', bind: ['wf_inflight'], fmt: 'int', extra: 'wf_pollers', extraLabel: 'parked' },
      { id: 'actq', x: 376, y: 150, w: 140, h: 58, kind: 'queue', label: 'matching-act', bind: ['act_depth'], fmt: 'int', extra: 'child_depth', extraLabel: 'child' },
      { id: 'actpool', x: 566, y: 150, w: 150, h: 58, kind: 'pool', label: 'activity workers', bind: ['act_inflight'], fmt: 'int', extra: 'act_pollers', extraLabel: 'parked' },
      { id: 'done', x: 770, y: 150, w: 118, h: 58, kind: 'sink', label: 'completed', bind: ['completed'], fmt: 'int', unit: 'wf' },
      { id: 'shards', x: 186, y: 250, w: 158, h: 58, kind: 'store', label: 'history shards', bind: ['shard_transfer', 'shard_timer', 'shard_resched'], fmt: 'int', unit: 'tasks' },
      { id: 'timer', x: 566, y: 262, w: 150, h: 52, kind: 'fault', label: 'retry timers', bind: ['shard_timer'], fmt: 'int', unit: 'tasks' },
    ],
    edges: [
      { from: 'load', to: 'wfq', label: 'start', flow: 'started_rate' },
      { from: 'wfq', to: 'wfpool', label: 'dispatch', flow: 'started_rate' },
      { from: 'wfpool', to: 'actq', label: 'schedule', flow: 'activity_adds_rate' },
      { from: 'actq', to: 'actpool', label: 'take', flow: 'attempts_rate' },
      { from: 'actpool', to: 'done', label: 'done', flow: 'completed_rate' },
      { from: 'wfpool', to: 'shards', label: 'persist', flow: 'activity_adds_rate', via: [300, 175] },
      { from: 'shards', to: 'actq', label: 'transfer', flow: 'activity_adds_rate', via: [300, 240] },
      { from: 'actpool', to: 'timer', label: 'fail', flow: 'retries_rate', via: [640, 235] },
      { from: 'timer', to: 'actq', label: 'retry', flow: 'retries_rate', via: [480, 240] },
    ],
  },
  tasqueue: {
    w: 920,
    h: 260,
    nodes: [
      { id: 'load', x: 30, y: 52, w: 110, h: 58, kind: 'source', label: 'producers', bind: ['started_rate'], fmt: 'rate' },
      { id: 'broker', x: 200, y: 52, w: 158, h: 58, kind: 'queue', label: 'broker queue', bind: ['queue_depth', 'dispatch_depth'], fmt: 'int', unit: 'jobs' },
      { id: 'workers', x: 426, y: 52, w: 152, h: 58, kind: 'pool', label: 'workers', bind: ['poller_inflight'], fmt: 'int', unit: 'jobs' },
      { id: 'done', x: 700, y: 52, w: 116, h: 58, kind: 'sink', label: 'completed', bind: ['completed'], fmt: 'int', unit: 'jobs' },
      { id: 'timer', x: 200, y: 168, w: 158, h: 56, kind: 'store', label: 'scheduled', bind: ['timer_depth'], fmt: 'int', unit: 'jobs' },
      { id: 'failed', x: 700, y: 168, w: 116, h: 56, kind: 'fault', label: 'failed', bind: ['failed'], fmt: 'int', unit: 'jobs' },
    ],
    edges: [
      { from: 'load', to: 'broker', label: 'enqueue', flow: 'started_rate' },
      { from: 'timer', to: 'broker', label: 'due' },
      { from: 'broker', to: 'workers', label: 'poll', flow: 'attempts_rate' },
      { from: 'workers', to: 'done', label: 'complete', flow: 'completed_rate' },
      { from: 'workers', to: 'failed', label: 'terminal', flow: 'failed_rate', via: [640, 165] },
      { from: 'workers', to: 'broker', label: 'retry', flow: 'retried_rate', via: [390, 22] },
    ],
  },
};

function initDiagram() {
  const spec = DIAGRAMS[state.result.model];
  const holder = $('#diagram');
  holder.innerHTML = '';
  if (!spec) return;

  const svg = svgEl('svg', { viewBox: `0 0 ${spec.w} ${spec.h}`, role: 'img', 'aria-label': 'Live mechanism map' });
  const defs = svgEl('defs');
  const marker = svgEl('marker', {
    id: 'arrow', viewBox: '0 0 8 8', refX: '7', refY: '4',
    markerWidth: '6', markerHeight: '6', orient: 'auto-start-reverse',
  });
  const arrowPath = svgEl('path', { d: 'M0 0 L8 4 L0 8 z', fill: '#3f5a80' });
  marker.append(arrowPath);
  defs.append(marker);
  svg.append(defs);

  const edgeLayer = svgEl('g');
  svg.append(edgeLayer);
  const flowEdges = [];
  const nodeLayer = svgEl('g');
  svg.append(nodeLayer);

  const nodeById = new Map();
  for (const n of spec.nodes) nodeById.set(n.id, n);

  for (const e of spec.edges) {
    const from = nodeById.get(e.from);
    const to = nodeById.get(e.to);
    if (!from || !to) continue;
    const a = borderPoint(from, e.via ? { x: e.via[0], y: e.via[1], w: 0, h: 0 } : to);
    const b = e.via ? borderPoint(to, { x: e.via[0], y: e.via[1], w: 0, h: 0 }) : borderPoint(to, from);
    const d = e.via
      ? `M ${a.x} ${a.y} Q ${e.via[0]} ${e.via[1]} ${b.x} ${b.y}`
      : `M ${a.x} ${a.y} L ${b.x} ${b.y}`;
    const path = svgEl('path', { d, 'marker-end': 'url(#arrow)' });
    const g = svgEl('g', { class: 'edge' });
    g.append(path);
    if (e.flow) g.append(svgEl('path', { d, class: 'flow' }));
    if (e.label) {
      const mid = e.via ? { x: e.via[0], y: e.via[1] } : { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
      const text = svgEl('text', { x: mid.x, y: mid.y - 4, 'text-anchor': 'middle' });
      text.textContent = e.label;
      g.append(text);
    }
    edgeLayer.append(g);
    if (e.flow) flowEdges.push({ g, key: e.flow });
  }

  const nodes = [];
  for (const n of spec.nodes) {
    const g = svgEl('g', { class: `node kind-${n.kind}` });
    const box = svgEl('rect', { class: 'box', x: n.x, y: n.y, width: n.w, height: n.h, rx: 9 });
    const fill = svgEl('rect', { class: 'fill', x: n.x + 1, y: n.y + n.h - 2, width: n.w - 2, height: 0, rx: 8 });
    const label = svgEl('text', { class: 'label', x: n.x + n.w / 2, y: n.y + 17, 'text-anchor': 'middle' });
    label.textContent = n.label;
    const value = svgEl('text', { class: 'value', x: n.x + n.w / 2, y: n.y + 40, 'text-anchor': 'middle' });
    value.textContent = '0';
    const unit = svgEl('text', { class: 'unit', x: n.x + n.w / 2, y: n.y + 52, 'text-anchor': 'middle' });
    unit.textContent = n.unit || '';
    g.append(box, fill, label, value, unit);
    let extra = null;
    if (n.extra) {
      extra = svgEl('text', { class: 'unit', x: n.x + n.w - 7, y: n.y + 13, 'text-anchor': 'end' });
      g.append(extra);
    }
    nodeLayer.append(g);
    const max = seriesScale(n.bind);
    nodes.push({ spec: n, g, fill, value, unit, extra, max });
  }

  state.diagram = { spec, nodes, flowEdges, svg };
  holder.append(svg);
}

// seriesScale normalizes a node bar to the 99th percentile (×1.5), so a
// one-tick burst does not leave the live bar visually empty.
function seriesScale(keys) {
  const vals = [];
  for (const k of keys) {
    const a = state.result.v[k];
    if (a) for (const v of a) if (v > 0) vals.push(v);
  }
  if (vals.length === 0) return 1;
  vals.sort((a, b) => a - b);
  const raw = vals[vals.length - 1];
  const p99 = vals[Math.min(vals.length - 1, Math.floor(vals.length * 0.99))];
  return Math.max(1, Math.min(raw, p99 * 1.5));
}

function borderPoint(from, to) {
  const cx = from.x + from.w / 2;
  const cy = from.y + from.h / 2;
  const tx = to.x + (to.w || 0) / 2;
  const ty = to.y + (to.h || 0) / 2;
  const dx = tx - cx;
  const dy = ty - cy;
  if (dx === 0 && dy === 0) return { x: cx, y: cy };
  const sx = dx === 0 ? Infinity : from.w / 2 / Math.abs(dx);
  const sy = dy === 0 ? Infinity : from.h / 2 / Math.abs(dy);
  const s = Math.min(sx, sy);
  return { x: cx + dx * s, y: cy + dy * s };
}

function frameIndex() {
  const r = state.result;
  if (!r) return 0;
  return Math.min(r.t.length - 1, Math.max(0, Math.round(state.frame)));
}

function frameValue(keys) {
  const r = state.result;
  const i = frameIndex();
  let sum = 0;
  for (const k of keys) {
    const a = r.v[k];
    if (a) sum += a[i] || 0;
  }
  return sum;
}

function updateDiagram() {
  const d = state.diagram;
  if (!d) return;
  for (const n of d.nodes) {
    const v = frameValue(n.spec.bind);
    let text;
    if (n.spec.fmt === 'rate') text = compact(v);
    else text = compact(v);
    n.value.textContent = text;
    const frac = Math.max(0, Math.min(1, v / n.max));
    const fh = Math.max(0, (n.spec.h - 2) * frac);
    n.fill.setAttribute('y', String(n.spec.y + n.spec.h - 1 - fh));
    n.fill.setAttribute('height', String(fh));
    if (n.extra) {
      const ev = frameValue([n.spec.extra]);
      n.extra.textContent = ev > 0 ? `${n.spec.extraLabel} ${compact(ev)}` : '';
    }
    n.g.classList.toggle('hot', n.spec.kind === 'queue' && frac > 0.5);
  }
  for (const e of d.flowEdges || []) {
    const v = frameValue([e.key]);
    e.g.classList.toggle('active', v > 0.05);
  }
}

/* ------------------------------------------------------------------ */
/* charts                                                              */
/* ------------------------------------------------------------------ */

function colorFor(key) {
  const idx = state.result.series.findIndex((s) => s.key === key);
  return PALETTE[(idx < 0 ? 0 : idx) % PALETTE.length];
}

function seriesMeta(key) {
  return state.result.series.find((s) => s.key === key) || { key, label: key, unit: '' };
}

function initCharts() {
  const holder = $('#charts');
  holder.innerHTML = '';
  state.charts = [];
  for (const def of state.result.charts) {
    const wrap = h('div', 'chart');
    const head = h('div', 'chart-head');
    head.append(h('div', 'chart-title', def.title));
    head.append(h('div', 'chart-unit', def.unit || ''));
    wrap.append(head);
    if (def.note) wrap.append(h('p', 'chart-note', def.note));
    const legend = h('div', 'legend');
    for (const key of def.series) {
      const meta = seriesMeta(key);
      const btn = h('button');
      btn.type = 'button';
      btn.dataset.key = key;
      const sw = h('span', 'swatch');
      sw.style.background = colorFor(key);
      btn.append(sw, document.createTextNode(meta.label));
      btn.classList.toggle('off', state.hidden.has(key));
      btn.addEventListener('click', () => {
        if (state.hidden.has(key)) state.hidden.delete(key);
        else state.hidden.add(key);
        btn.classList.toggle('off', state.hidden.has(key));
        drawAllCharts();
      });
      legend.append(btn);
    }
    wrap.append(legend);
    const canvas = document.createElement('canvas');
    canvas.setAttribute('role', 'img');
    canvas.setAttribute('aria-label', def.title);
    const tip = h('div', 'chart-tip');
    tip.style.display = 'none';
    wrap.append(canvas, tip);
    holder.append(wrap);
    const c = { def, canvas, ctx: canvas.getContext('2d'), tip, hover: null, hoverX: 0 };
    canvas.addEventListener('mousemove', (e) => onChartHover(c, e));
    canvas.addEventListener('mouseleave', () => clearChartHover(c));
    canvas.addEventListener('click', (e) => onChartClick(c, e));
    state.charts.push(c);
  }
  resizeCharts();
}

function resizeCharts() {
  const dpr = window.devicePixelRatio || 1;
  for (const c of state.charts) {
    if (c.tip) c.tip.style.display = 'none';
    c.hover = null;
  }
  for (const c of state.charts) {
    const w = c.canvas.clientWidth || c.canvas.parentElement.clientWidth || 600;
    const hgt = 132;
    c.canvas.width = Math.round(w * dpr);
    c.canvas.height = Math.round(hgt * dpr);
    c.w = w;
    c.h = hgt;
    c.dpr = dpr;
  }
  drawAllCharts();
}

function drawAllCharts() {
  if (!state.result) return;
  for (const c of state.charts) drawChart(c);
}

function drawChart(c) {
  const ctx = c.ctx;
  const r = state.result;
  const n = r.t.length;
  if (!n || !c.w) return;
  const dpr = c.dpr || 1;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, c.w, c.h);

  const pad = { l: 44, r: 10, t: 8, b: 17 };
  const iw = c.w - pad.l - pad.r;
  const ih = c.h - pad.t - pad.b;
  const t0 = r.t[0];
  const t1 = r.t[n - 1] || 1;

  const visible = c.def.series.filter((k) => !state.hidden.has(k));
  const arrays = [];
  for (const k of visible) {
    if (r.v[k]) arrays.push(r.v[k]);
    if (state.pin && state.pin.v[k]) arrays.push(state.pin.v[k]);
  }
  const dataMax = robustMax(arrays);
  const yMax = niceMax(dataMax);
  const xAt = (t) => pad.l + ((t - t0) / (t1 - t0 || 1)) * iw;
  const yAt = (v) => pad.t + ih - (Math.max(0, v) / yMax) * ih;

  // annotation bands
  for (const a of r.annotations || []) {
    const start = a.start_ms;
    const end = a.end_ms > 0 ? a.end_ms : t1;
    if (end < t0 || start > t1) continue;
    const x0 = xAt(Math.max(t0, start));
    const x1 = xAt(Math.min(t1, end));
    ctx.fillStyle = a.kind === 'fault' ? 'rgba(251, 113, 133, 0.08)' : 'rgba(251, 191, 36, 0.06)';
    ctx.fillRect(x0, pad.t, Math.max(1, x1 - x0), ih);
    ctx.strokeStyle = a.kind === 'fault' ? 'rgba(251, 113, 133, 0.35)' : 'rgba(251, 191, 36, 0.3)';
    ctx.setLineDash([3, 3]);
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x0, pad.t);
    ctx.lineTo(x0, pad.t + ih);
    if (x1 > x0) {
      ctx.moveTo(x1, pad.t);
      ctx.lineTo(x1, pad.t + ih);
    }
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // grid
  ctx.font = '9.5px ui-monospace, monospace';
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= 3; i++) {
    const v = (yMax / 3) * i;
    const y = yAt(v);
    ctx.strokeStyle = 'rgba(35, 50, 78, 0.5)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(pad.l + iw, y);
    ctx.stroke();
    ctx.fillStyle = '#607394';
    ctx.fillText(compact(v), pad.l - 6, y);
  }

  // x labels
  ctx.textAlign = 'center';
  ctx.textBaseline = 'top';
  for (let i = 0; i <= 4; i++) {
    const t = t0 + ((t1 - t0) / 4) * i;
    const x = xAt(t);
    ctx.fillStyle = '#607394';
    ctx.fillText(fmtSim(t), x, pad.t + ih + 5);
    ctx.strokeStyle = 'rgba(35, 50, 78, 0.35)';
    ctx.beginPath();
    ctx.moveTo(x, pad.t);
    ctx.lineTo(x, pad.t + ih);
    ctx.stroke();
  }

  ctx.save();
  ctx.beginPath();
  ctx.rect(pad.l, pad.t, iw, ih);
  ctx.clip();

  // pinned ghost
  if (state.pin) {
    for (const k of visible) {
      const a = state.pin.v[k];
      if (!a || a.length === 0) continue;
      ctx.strokeStyle = hexA(colorFor(k), 0.3);
      ctx.lineWidth = 1.2;
      ctx.setLineDash([3, 4]);
      ctx.beginPath();
      for (let i = 0; i < a.length; i++) {
        const x = pad.l + ((state.pin.t[i] - t0) / (t1 - t0 || 1)) * iw;
        const y = yAt(a[i]);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  // series
  ctx.lineJoin = 'round';
  for (const k of visible) {
    const a = r.v[k];
    if (!a || a.length === 0) continue;
    ctx.strokeStyle = colorFor(k);
    ctx.lineWidth = 1.7;
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
      const x = xAt(r.t[i]);
      const y = yAt(a[i]);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  ctx.restore();

  if (dataMax === 0) {
    ctx.fillStyle = '#4c5f7d';
    ctx.font = '11px ui-sans-serif, system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('no events in this run', pad.l + iw / 2, pad.t + ih / 2);
  }

  // hover line (the playhead is drawn separately)
  if (c.hover != null && c.hover !== frameIndex()) {
    const hx = xAt(r.t[c.hover]);
    ctx.strokeStyle = 'rgba(45, 212, 191, 0.45)';
    ctx.setLineDash([2, 3]);
    ctx.beginPath();
    ctx.moveTo(hx, pad.t);
    ctx.lineTo(hx, pad.t + ih);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // cursor
  const i = frameIndex();
  const cx = xAt(r.t[i]);
  ctx.strokeStyle = 'rgba(232, 238, 251, 0.28)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(cx, pad.t);
  ctx.lineTo(cx, pad.t + ih);
  ctx.stroke();
  for (const k of visible) {
    const a = r.v[k];
    if (!a) continue;
    const y = yAt(a[i]);
    ctx.fillStyle = colorFor(k);
    ctx.beginPath();
    ctx.arc(cx, y, 2.4, 0, Math.PI * 2);
    ctx.fill();
  }
}

// chartIndexAt maps a client x coordinate to a trace index.
function chartIndexAt(c, clientX) {
  const r = state.result;
  const rect = c.canvas.getBoundingClientRect();
  const x = clientX - rect.left;
  const padL = 44;
  const padR = 10;
  const iw = c.w - padL - padR;
  if (x < padL - 2 || x > padL + iw + 2 || iw <= 0) return null;
  const t0 = r.t[0];
  const t1 = r.t[r.t.length - 1];
  const target = t0 + ((x - padL) / iw) * (t1 - t0);
  let lo = 0;
  let hi = r.t.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (r.t[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(r.t[lo - 1] - target) < Math.abs(r.t[lo] - target)) lo -= 1;
  return { idx: lo, x: Math.max(padL, Math.min(padL + iw, x)) };
}

function onChartHover(c, e) {
  const hit = chartIndexAt(c, e.clientX);
  if (!hit) return clearChartHover(c);
  c.hover = hit.idx;
  c.hoverX = hit.x;
  drawChart(c);
  showChartTip(c);
}

function clearChartHover(c) {
  if (c.hover === null && c.tip.style.display === 'none') return;
  c.hover = null;
  c.tip.style.display = 'none';
  drawChart(c);
}

function onChartClick(c, e) {
  if (!state.result) return;
  const hit = chartIndexAt(c, e.clientX);
  if (!hit) return;
  state.frame = hit.idx;
  setPlaying(false);
  renderFrame();
}

function seriesValueText(meta, v) {
  if (meta.unit === 'ms') return fmtSim(v);
  if (meta.unit === 'tasks' || meta.unit === 'jobs' || meta.unit === 'wf') return compact(v) + ' ' + meta.unit;
  return compact(v) + (meta.unit ? ' ' + meta.unit : '');
}

function showChartTip(c) {
  const r = state.result;
  const idx = c.hover;
  if (idx == null || !r) return;
  c.tip.innerHTML = '';
  c.tip.append(h('div', 'tt', fmtSim(r.t[idx])));
  for (const key of c.def.series) {
    if (state.hidden.has(key)) continue;
    const meta = seriesMeta(key);
    const row = h('div', 'row');
    const sw = h('span', 'sw');
    sw.style.background = colorFor(key);
    row.append(sw, h('span', 'nm', meta.label));
    const v = (r.v[key] || [])[idx] || 0;
    row.append(h('span', 'v', seriesValueText(meta, v)));
    c.tip.append(row);
  }
  c.tip.style.display = 'block';
  const maxLeft = Math.max(4, c.w - c.tip.offsetWidth - 6);
  c.tip.style.left = Math.min(c.hoverX + 14, maxLeft) + 'px';
  c.tip.style.top = '8px';
}

// robustMax returns the 99th percentile of the visible values, so a
// one-sample burst does not flatten the whole chart.
function robustMax(arrays) {
  const vals = [];
  for (const a of arrays) for (const v of a) if (v > 0) vals.push(v);
  if (vals.length === 0) return 0;
  vals.sort((a, b) => a - b);
  const idx = Math.min(vals.length - 1, Math.floor(vals.length * 0.99));
  return vals[idx];
}

function hexA(hex, alpha) {
  const h6 = hex.replace('#', '');
  const r = parseInt(h6.slice(0, 2), 16);
  const g = parseInt(h6.slice(2, 4), 16);
  const b = parseInt(h6.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

/* ------------------------------------------------------------------ */
/* playback                                                            */
/* ------------------------------------------------------------------ */

function renderFrame() {
  const r = state.result;
  if (!r) return;
  const i = frameIndex();
  updateDiagram();
  drawAllCharts();
  const scrub = $('#scrub');
  if (document.activeElement !== scrub) scrub.value = String(i);
  $('#time-label').textContent = `${fmtSim(r.t[i])} / ${fmtSim(r.t[r.t.length - 1])}`;
}

function setPlaying(on) {
  state.playing = on && !!state.result;
  if (state.playing && frameIndex() >= state.result.t.length - 1) state.frame = 0;
  state.lastTs = 0;
  const icon = $('#play-icon');
  icon.innerHTML = '';
  if (state.playing) {
    icon.append(svgEl('path', { d: 'M3 2h4v12H3zM9 2h4v12H9z' }));
  } else {
    icon.append(svgEl('path', { d: 'M4 2.5v11l9-5.5z' }));
  }
  $('#play-btn').setAttribute('aria-label', state.playing ? 'Pause' : 'Play');
}

function loop(ts) {
  const r = state.result;
  if (r && state.playing) {
    if (!state.lastTs) state.lastTs = ts;
    const dt = Math.min(0.1, (ts - state.lastTs) / 1000);
    state.lastTs = ts;
    const fps = (r.t.length / BASE_PLAY_SECONDS) * state.speed;
    state.frame += dt * fps;
    if (state.frame >= r.t.length - 1) {
      state.frame = r.t.length - 1;
      setPlaying(false);
    }
    renderFrame();
  }
  state.raf = requestAnimationFrame(loop);
}

/* ------------------------------------------------------------------ */
/* determinism                                                         */
/* ------------------------------------------------------------------ */

function renderDeterminism() {
  const r = state.result;
  $('#digest').textContent = r.digest || '(no digest)';
  const status = $('#det-status');
  status.className = 'status-line wait';
  status.textContent = 'Replay reruns the exact request and compares the digest.';
  renderSweepPlaceholder();
  renderRunStatus();
}

function renderSweepPlaceholder() {
  const tbody = $('#sweep-table').querySelector('tbody');
  tbody.innerHTML = '';
  const tr = document.createElement('tr');
  const td = h('td', '', 'Sweep runs the current config across eight seeds — one config, eight deterministic worlds.');
  td.colSpan = 4;
  td.style.color = 'var(--dim)';
  td.style.fontFamily = 'var(--sans)';
  tr.append(td);
  tbody.append(tr);
}

async function verifyReplay() {
  if (!state.lastRequest || state.running) return;
  const btn = $('#replay-btn');
  const status = $('#det-status');
  btn.disabled = true;
  status.className = 'status-line wait';
  status.textContent = 'Replaying the exact request…';
  try {
    const again = await postJSON('/api/runs', state.lastRequest);
    if (again.digest === state.result.digest) {
      status.className = 'status-line ok';
      status.textContent = `✓ identical digest across two runs (${again.wall_ms} ms) — ${again.digest.slice(0, 12)}…`;
    } else {
      status.className = 'status-line err';
      status.textContent = '✗ digests differ! The model is not deterministic for this config.';
    }
  } catch (err) {
    status.className = 'status-line err';
    status.textContent = 'Replay failed: ' + err.message;
  } finally {
    btn.disabled = false;
  }
}

async function sweepSeeds() {
  if (!state.lastRequest || state.running) return;
  const btn = $('#sweep-btn');
  btn.disabled = true;
  setBusy(true, '<b>Sweeping seeds</b> — eight deterministic worlds, one config');
  const status = $('#det-status');
  status.className = 'status-line wait';
  status.textContent = 'Running across eight seeds…';
  try {
    const res = await postJSON('/api/runs/sweep', {
      scenario: state.lastRequest.scenario,
      knobs: state.lastRequest.knobs,
      count: 8,
      start_seed: state.lastRequest.seed !== undefined ? state.lastRequest.seed : state.seed,
    });
    renderSweep(res.points);
  } catch (err) {
    status.className = 'status-line err';
    status.textContent = 'Sweep failed: ' + err.message;
  } finally {
    btn.disabled = false;
    setBusy(false);
  }
}

function renderSweep(points) {
  const tbody = $('#sweep-table').querySelector('tbody');
  tbody.innerHTML = '';
  const digests = new Set();
  let min = Infinity;
  let max = -Infinity;
  for (const p of points) {
    digests.add(p.digest);
    min = Math.min(min, p.value);
    max = Math.max(max, p.value);
    const tr = document.createElement('tr');
    tr.append(h('td', '', String(p.seed)));
    tr.append(h('td', '', p.value.toFixed(2)));
    tr.append(h('td', '', compact(p.max_depth)));
    tr.append(h('td', '', p.digest ? p.digest.slice(0, 10) + '…' : '—'));
    tbody.append(tr);
  }
  const status = $('#det-status');
  status.className = 'status-line';
  const spread = max - min < 1e-9 ? `identical attempts/start (${min.toFixed(2)})` : `attempts/start spans ${min.toFixed(2)}–${max.toFixed(2)}`;
  status.textContent = `${points.length} runs · ${digests.size} distinct digest${digests.size === 1 ? '' : 's'} · ${spread}`;
  status.className = 'status-line ' + (digests.size === 1 ? 'ok' : '');
}

function renderPinButton() {
  const btn = $('#pin-btn');
  if (state.pin) {
    btn.textContent = 'Unpin';
  } else {
    btn.textContent = 'Pin for compare';
  }
}

function togglePin() {
  if (state.pin) {
    state.pin = null;
  } else {
    state.pin = state.result;
  }
  renderPinButton();
  renderKpis();
  drawAllCharts();
}

/* ------------------------------------------------------------------ */
/* permalink + download                                                */
/* ------------------------------------------------------------------ */

function parseHash() {
  const raw = location.hash.replace(/^#/, '');
  const p = new URLSearchParams(raw);
  const knobs = {};
  for (const [k, v] of p.entries()) {
    if (k === 's' || k === 'seed' || k === 't') continue;
    const num = Number(v);
    if (isFinite(num)) knobs[k] = num;
  }
  const seedRaw = p.get('seed');
  const timeRaw = p.get('t');
  return {
    scenario: p.get('s'),
    seed: seedRaw === null || seedRaw === '' ? null : Number(seedRaw),
    time: timeRaw === null || timeRaw === '' ? null : Number(timeRaw),
    knobs,
  };
}

function buildHash(includeTime) {
  const p = new URLSearchParams();
  p.set('s', state.scenario.id);
  p.set('seed', String(state.seed));
  for (const [k, v] of Object.entries(state.knobs)) p.set(k, String(v));
  if (includeTime && state.result) p.set('t', String(state.result.t[frameIndex()]));
  return '#' + p.toString();
}

function updateHash() {
  history.replaceState(null, '', buildHash(false));
}

function seekToTime(ms) {
  const r = state.result;
  if (!r) return;
  let lo = 0;
  let hi = r.t.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (r.t[mid] < ms) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(r.t[lo - 1] - ms) < Math.abs(r.t[lo] - ms)) lo -= 1;
  state.frame = lo;
  setPlaying(false);
  renderFrame();
}

// copyText prefers the async clipboard API and falls back to a hidden
// textarea so the dashboard also works over plain HTTP on a LAN address.
async function copyText(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch (_) { /* fall through */ }
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.setAttribute('readonly', '');
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.append(ta);
  ta.select();
  let ok = false;
  try { ok = document.execCommand('copy'); } catch (_) { /* no copy */ }
  ta.remove();
  return ok;
}

async function copyPermalink() {
  history.replaceState(null, '', buildHash(true));
  if (await copyText(location.href)) {
    toast('Permalink copied — it encodes the scenario, seed and every knob.', 'ok');
  } else {
    toast('Copy failed; the URL bar already has the permalink.');
  }
}

function cliCommand() {
  const req = state.lastRequest;
  if (!req) return '';
  const changed = Object.entries(req.knobs)
    .filter(([k, v]) => Math.abs(v - (state.scenario.preset[k] ?? v)) > 1e-9)
    .map(([k, v]) => `${k}=${v}`);
  let cmd = `go run ./cmd/queuesim run -scenario ${req.scenario} -seed ${req.seed}`;
  if (changed.length) cmd += ` -set ${changed.join(',')}`;
  return cmd;
}

async function copyCLI() {
  const cmd = cliCommand();
  if (!cmd) return;
  if (await copyText(cmd)) {
    toast('CLI command copied: ' + cmd, 'ok');
  } else {
    toast('Copy failed. Command: ' + cmd);
  }
}

async function copyDigest() {
  if (!state.result) return;
  if (await copyText(state.result.digest)) {
    toast('Digest copied.', 'ok');
  } else {
    toast('Copy failed; select the digest text instead.');
  }
}

function downloadTrace() {
  if (!state.result) return;
  const blob = new Blob([JSON.stringify(state.lastRequest ? { request: state.lastRequest, result: state.result } : state.result, null, 2)],
    { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `queuesim-${state.result.scenario}-seed${state.result.seed}.json`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

/* ------------------------------------------------------------------ */
/* chrome                                                              */
/* ------------------------------------------------------------------ */

function wireChrome() {
  $('#run-btn').addEventListener('click', runScenario);
  $('#reset-knobs').addEventListener('click', resetKnobs);
  $('#seed').addEventListener('change', () => {
    const v = Number($('#seed').value);
    if (isFinite(v)) state.seed = Math.trunc(v);
    markDirty();
  });
  $('#replay-btn').addEventListener('click', verifyReplay);
  $('#sweep-btn').addEventListener('click', sweepSeeds);
  $('#link-btn').addEventListener('click', copyPermalink);
  $('#download-btn').addEventListener('click', downloadTrace);
  $('#copy-digest').addEventListener('click', copyDigest);
  $('#copy-cli').addEventListener('click', copyCLI);
  $('#pin-btn').addEventListener('click', togglePin);
  $('#play-btn').addEventListener('click', () => {
    if (!state.result) return;
    setPlaying(!state.playing);
    renderFrame();
  });
  $('#scrub').addEventListener('input', (e) => {
    state.frame = Number(e.target.value);
    setPlaying(false);
    renderFrame();
  });
  $('#speed').addEventListener('change', (e) => { state.speed = Number(e.target.value); });

  const help = $('#help');
  $('#help-btn').addEventListener('click', () => help.showModal());
  $('#help-close').addEventListener('click', () => help.close());
  help.addEventListener('click', (e) => { if (e.target === help) help.close(); });

  window.addEventListener('resize', () => { resizeCharts(); });
  window.addEventListener('hashchange', () => {
    const p = parseHash();
    const sc = state.catalog.find((s) => s.id === p.scenario);
    if (!sc) return;
    const sameKnobs = Object.keys(p.knobs).every((k) => state.knobs[k] === p.knobs[k]);
    const sameRun = state.scenario && state.scenario.id === sc.id && p.seed === state.seed && sameKnobs;
    if (sameRun) {
      if (p.time !== null) seekToTime(p.time);
      return;
    }
    selectScenario(sc, p);
  });
  window.addEventListener('keydown', (e) => {
    const tag = (document.activeElement && document.activeElement.tagName) || '';
    const typing = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      runScenario();
    } else if (e.key === ' ' && !typing && !$('#help').open) {
      e.preventDefault();
      if (state.result) { setPlaying(!state.playing); renderFrame(); }
    } else if (e.key === 'ArrowRight' && !typing && state.result) {
      state.frame = Math.min(state.result.t.length - 1, frameIndex() + 10);
      setPlaying(false);
      renderFrame();
    } else if (e.key === 'ArrowLeft' && !typing && state.result) {
      state.frame = Math.max(0, frameIndex() - 10);
      setPlaying(false);
      renderFrame();
    }
  });

  const charts = $('#charts');
  if ('ResizeObserver' in window) {
    new ResizeObserver(() => { if (state.result) resizeCharts(); }).observe(charts);
  }
}

let toastTimer = 0;
function toast(msg, kind) {
  const el = $('#toast');
  el.textContent = msg;
  el.style.borderColor = kind === 'ok' ? 'var(--good)' : 'var(--bad)';
  el.style.background = kind === 'ok' ? '#0f2a22' : '#2a1420';
  el.style.color = kind === 'ok' ? '#c9ffe9' : '#ffd7de';
  el.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove('show'), 4200);
}

/* ------------------------------------------------------------------ */

boot();
requestAnimationFrame(loop);
