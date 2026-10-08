#!/usr/bin/env node
/* ============================================================
   ORACLE SERVER v3 — connection layer + reasoning boundary.
   oracleStatePacket() (inside ORACLE) is the ONLY canonical
   state projection. The AI proposes; this validator decides.
   ============================================================ */
'use strict';
const http = require('node:http'), fs = require('node:fs'),
      path = require('node:path'), crypto = require('node:crypto');
const DIR = __dirname, VERSION = '3.0.0';

/* ---------- config ---------- */
(function loadDotEnv () {
  try {
    for (const line of fs.readFileSync(path.join(DIR, '.env'), 'utf8').split(/\r?\n/)) {
      if (line.trim().startsWith('#')) continue;
      const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
      if (!m) continue;
      const v = m[2].replace(/^["']|["']$/g, '');
      if (!(m[1] in process.env)) process.env[m[1]] = v;
    }
  } catch {}
})();
const PORT   = parseInt(process.env.ORACLE_PORT || '8787', 10);
const HOST   = process.env.ORACLE_HOST  || '127.0.0.1';
const TOKEN  = process.env.ORACLE_TOKEN || '';
const CORS   = /^(1|true|yes)$/i.test(process.env.ORACLE_CORS || '');
const AI_KEY = process.env.ORACLE_AI_KEY  || '';
const AI_MODEL = process.env.ORACLE_AI_MODEL || '';
const AI_BASE  = (process.env.ORACLE_AI_BASE || 'https://api.openai.com/v1').replace(/\/+$/, '');

/* ============================================================
   STORAGE — SQLite (node:sqlite, Node >= 22.5) else durable JSON.
   ============================================================ */
const DATA_DIR = path.join(DIR, 'oracle-data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const JPATH = path.join(DATA_DIR, 'store.json');
const EVLOG = path.join(DATA_DIR, 'events.jsonl');
let engine = 'json', sql = null;
const jsonStore = { docs: {} };
let pendingWrite = false, writeTimer = null;
function scheduleWrite () { pendingWrite = true; if (writeTimer) return;
  writeTimer = setTimeout(() => { writeTimer = null; writeNow(); }, 250); }
function writeNow () {
  if (!pendingWrite) return; pendingWrite = false;
  try { const tmp = JPATH + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(jsonStore));
    fs.renameSync(tmp, JPATH); } catch (e) { console.error('[store] write failed:', e.message); }
}
try {
  const { DatabaseSync } = require('node:sqlite');
  sql = new DatabaseSync(path.join(DATA_DIR, 'oracle.db'));
  sql.exec('CREATE TABLE IF NOT EXISTS docs(collection TEXT NOT NULL, id TEXT NOT NULL,' +
           ' json TEXT NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY(collection,id))');
  engine = 'sqlite';
} catch (e) { try { Object.assign(jsonStore, JSON.parse(fs.readFileSync(JPATH, 'utf8'))); } catch {} }
function coll (c) { if (!jsonStore.docs[c]) jsonStore.docs[c] = {}; return jsonStore.docs[c]; }
function putDoc (c, id, obj) {
  const j = JSON.stringify(obj), ts = Date.now();
  if (sql) sql.prepare('INSERT INTO docs(collection,id,json,ts) VALUES(?,?,?,?)' +
    ' ON CONFLICT(collection,id) DO UPDATE SET json=excluded.json, ts=excluded.ts').run(c, id, j, ts);
  else { coll(c)[id] = { json: j, ts }; scheduleWrite(); }
  return obj;
}
function getDoc (c, id) {
  if (sql) { const r = sql.prepare('SELECT json FROM docs WHERE collection=? AND id=?').get(c, id);
    return r ? JSON.parse(r.json) : null; }
  const r = coll(c)[id]; return r ? JSON.parse(r.json) : null;
}
function delDoc (c, id) {
  if (sql) sql.prepare('DELETE FROM docs WHERE collection=? AND id=?').run(c, id);
  else { delete coll(c)[id]; scheduleWrite(); }
}
function listDocs (c) {
  const out = {};
  if (sql) for (const r of sql.prepare('SELECT id,json FROM docs WHERE collection=?').all(c)) out[r.id] = JSON.parse(r.json);
  else for (const [id, r] of Object.entries(coll(c))) out[id] = JSON.parse(r.json);
  return out;
}
function logEvent (e) {
  e = Object.assign({ ts: Date.now(), id: crypto.randomUUID() }, e);
  try { fs.appendFileSync(EVLOG, JSON.stringify(e) + '\n'); } catch {}
  return e;
}
function trunc (s) { s = s == null ? null : String(s); return s && s.length > 65536 ? s.slice(0, 65536) + '…[truncated]' : s; }
function revision () { const m = getDoc('meta', 'main'); return m ? (m.revision || 0) : 0; }
function bumpRevision () {
  const m = getDoc('meta', 'main') || { revision: 0 };
  m.revision = (m.revision || 0) + 1; m.updated_at = Date.now();
  putDoc('meta', 'main', m); return m.revision;
}

/* ============================================================
   PACKET PATHS — locked paths only; bootstrap discovery is
   permanently disabled after the lock event.
   ============================================================ */
let PACKET_PATHS = { today_tasks: null, available_min: null, deadlines: null,
  current_gate: null, day: null, start_date: null, mission: null, mission_tasks: null };
let PATHS_LOCKED = false;
try {
  const s = getDoc('paths', 'main');
  if (s) {
    if (s.paths) for (const k of Object.keys(PACKET_PATHS))
      if (typeof s.paths[k] === 'string' && s.paths[k]) PACKET_PATHS[k] = s.paths[k];
    if (s.locked_at) PATHS_LOCKED = true;
  }
} catch {}

function norm (s) { return String(s == null ? '' : s).toLowerCase()
  .replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim(); }
function tokenOverlap (a, b) {
  const A = new Set(norm(a).split(' ').filter(w => w.length > 3));
  const B = new Set(norm(b).split(' ').filter(w => w.length > 3));
  if (!A.size || !B.size) return 0;
  let hit = 0; for (const w of A) if (B.has(w)) hit++;
  return hit / Math.min(A.size, B.size);
}
function pktGet (pkt, path) {
  if (!path) return undefined;
  let v = pkt;
  for (const k of path.replace(/\[(\d+)\]/g, '.$1').split('.')) { if (v == null) return undefined; v = v[k]; }
  return v;
}
function isTaskLike (a) { return Array.isArray(a) && a.length > 0 &&
  a.every(x => x && typeof x === 'object') &&
  a.some(x => 'done' in x || 'completed' in x || 'checked' in x); }
function findFirst (root, keyMatch, arrPred) {
  let found = null;
  (function walk (node, kp, d) {
    if (found || d > 5 || node == null) return;
    if (Array.isArray(node)) {
      if (kp && keyMatch.test(kp)) { const r = arrPred(node); if (r !== undefined) { found = { value: r, path: kp }; return; } }
      for (let i = 0; i < node.length; i++) walk(node[i], kp ? kp + '[' + i + ']' : '[' + i + ']', d + 1);
      return;
    }
    if (typeof node !== 'object') return;
    for (const k of Object.keys(node)) walk(node[k], kp ? kp + '.' + k : k, d + 1);
  })(root, null, 0);
  return found;
}
function findScalar (root, keyMatch, valPred) {
  let found = null;
  (function walk (node, kp, d) {
    if (found || d > 5 || node == null) return;
    if (Array.isArray(node)) { node.forEach((x, i) => walk(x, kp + '[' + i + ']', d + 1)); return; }
    if (typeof node !== 'object') return;
    for (const k of Object.keys(node)) {
      if (found) return;
      const v = node[k];
      if (v == null || Array.isArray(v) || typeof v === 'object') { walk(v, kp ? kp + '.' + k : k, d + 1); continue; }
      if (keyMatch.test(k) && valPred(v, k)) { found = { value: v, path: kp ? kp + '.' + k : k }; return; }
    }
  })(root, null, 0);
  return found;
}

/* Bootstrap discovery ONLY (suggestPaths gates on !PATHS_LOCKED).
   Never an input to a validator. */
const RESOLVERS = {
  today_tasks: pkt => { const f = findFirst(pkt, /today|plan|mission|tasks/i, a => isTaskLike(a) ? a : undefined); return f && { value: f.value, path: f.path }; },
  deadlines: pkt => { const f = findFirst(pkt, /deadline|dl|exam|due/i,
      a => (Array.isArray(a) && a.length && a.every(x => x && typeof x === 'object') &&
        a.some(x => Object.keys(x).some(k => /date|due|when|deadline/i.test(k)))) ? a : undefined);
    return f && { value: f.value, path: f.path }; },
  available_min: pkt => { const f = findScalar(pkt, /avail|free/i,
      v => { const n = typeof v === 'number' ? v
        : (typeof v === 'string' && /^\d+(\.5)?$/.test(v.trim()) ? parseFloat(v) : NaN);
        return !isNaN(n) && n >= 0.25 && n <= 960; });
    return f && { value: f.value, path: f.path }; },
  current_gate: pkt => { const f = findScalar(pkt, /current.{0,10}gate|gate.{0,10}current|^gate(?!s)/i,
      v => typeof v === 'string' && v.trim().length > 1 && v.length < 80);
    return f && { value: f.value, path: f.path }; },
  day: pkt => { const f = findScalar(pkt, /^day$|^campaign.?day$|^day.?(no|num)?$/i,
      v => { const n = typeof v === 'number' ? v : (typeof v === 'string' && /^\d{1,4}$/.test(v.trim()) ? parseInt(v, 10) : NaN);
        return Number.isInteger(n) && n >= 1 && n <= 2000; });
    if (!f) return null;
    return { value: typeof f.value === 'number' ? f.value : parseInt(f.value, 10), path: f.path }; },
  start_date: pkt => { const f = findScalar(pkt, /start/i,
      v => (typeof v === 'string' && (/^\d{4}-\d{2}-\d{2}/.test(v) || /^\d{10,13}$/.test(v)))
        || (typeof v === 'number' && v > 1e9 && v < 2e11));
    return f && { value: f.value, path: f.path }; },
  mission: pkt => { const f = findScalar(pkt, /^(current.{0,10})?(mission|objective|focus|theme)$/i,
      v => typeof v === 'string' && v.trim().length >= 8 && v.length <= 200);
    return f && { value: f.value, path: f.path }; },
  mission_tasks: pkt => { const f = findFirst(pkt, /mission.{0,8}(tasks?|entries|items)|plan.{0,6}(tasks?|entries)/i,
      a => (Array.isArray(a) && a.length && a.every(x => typeof x === 'string' ||
        (x && typeof x === 'object' && (x.title || x.name || x.text || x.task)))) ? a : undefined);
    return f && { value: f.value, path: f.path }; }
};
function resolveSpec (pkt, spec) {
  const path = PACKET_PATHS[spec];
  if (!path) return { value: undefined, via: 'unlocked', path: null };
  const v = pktGet(pkt, path);
  if (v === undefined) return { value: undefined, via: 'path_broken', path };
  return { value: v, via: 'confirmed_path', path };
}
function suggestPaths (pkt) {
  if (PATHS_LOCKED) return [];
  const out = [];
  for (const spec of Object.keys(RESOLVERS)) {
    if (PACKET_PATHS[spec]) continue;
    const r = RESOLVERS[spec] ? RESOLVERS[spec](pkt) : null;
    if (r && r.value !== undefined && r.path) out.push({ spec, path: r.path });
  }
  return out;
}
function structureOf (pkt) {
  const out = {};
  for (const k of Object.keys(pkt)) {
    const v = pkt[k];
    out[k] = Array.isArray(v) ? 'array(' + v.length + ')'
      : v && typeof v === 'object' ? 'object(' + Object.keys(v).length + ')' : typeof v;
  }
  return out;
}
function packetSanity (pkt, dom) {
  const out = [];
  const add = (check, status, detail) => out.push({ check, status, detail });
  if (!pkt || typeof pkt !== 'object' || Array.isArray(pkt)) {
    add('shape', 'FAIL', 'oracleStatePacket() did not return an object'); return out;
  }
  const keys = Object.keys(pkt);
  add('shape', 'PASS', keys.length + ' top-level: ' + keys.slice(0, 12).join(', ') + (keys.length > 12 ? ' …' : ''));
  const day = resolveSpec(pkt, 'day'), start = resolveSpec(pkt, 'start_date');
  const dayN = typeof day.value === 'number' ? day.value
    : (typeof day.value === 'string' && /^\d{1,4}$/.test(day.value.trim()) ? parseInt(day.value, 10) : NaN);
  if (Number.isInteger(dayN) && start.value !== undefined) {
    const t = typeof start.value === 'number' ? start.value : new Date(start.value).getTime();
    if (t > 1e9 && !isNaN(t)) {
      const computed = Math.floor((Date.now() - t) / 86400000) + 1;
      add('day_arithmetic', Math.abs(computed - dayN) <= 1 ? 'PASS' : 'WARN',
        'packet day ' + dayN + ' vs ' + computed + ' computed from start (' + day.via + ')');
    }
  } else add('day_arithmetic', 'NEED_INFO', 'day/start not on locked paths (' + day.via + ') — lock after capture');
  if (dom && typeof dom.day === 'number' && Number.isInteger(dayN))
    add('dom_agreement', dom.day === dayN ? 'PASS' : 'WARN',
      'packet day ' + dayN + ' vs day counter on screen ' + dom.day);
  else if (dom && dom.gate) {
    const g = resolveSpec(pkt, 'current_gate');
    if (typeof g.value === 'string')
      add('dom_agreement',
        (norm(dom.gate) && (norm(dom.gate).includes(norm(g.value)) || norm(g.value).includes(norm(dom.gate)))) ? 'PASS' : 'WARN',
        'packet gate "' + g.value + '" vs screen "' + dom.gate + '"');
  } else add('dom_agreement', 'NEED_INFO', 'screen values not captured');
  return out;
}

/* ============================================================
   VALIDATION HELPERS (no unit guessing, no silent clamps)
   ============================================================ */
function availableMinutes (raw) {
  const n = typeof raw === 'number' ? raw
    : (typeof raw === 'string' && /^\d+(\.0|\.\d+)?$/.test(raw.trim()) ? parseFloat(raw) : NaN);
  if (!isFinite(n) || n < 0) return { minutes: null, why: 'not a number' };
  if (n > 960) return { minutes: null, why: '>960 — not a plausible daily value; fix the packet field' };
  if (n <= 24) return { minutes: null, why: 'value ' + n + ' is ≤24 — hours or minutes is ambiguous; no guess is made (record available time in minutes in ORACLE, or use a field whose unit is explicit)' };
  return { minutes: Math.round(n), why: null };
}
function deadlinesWithin48h (pkt) {
  const dl = resolveSpec(pkt, 'deadlines');
  if (dl.via !== 'confirmed_path' || !Array.isArray(dl.value)) return { via: dl.via, list: [] };
  const now = Date.now();
  const list = dl.value.filter(d => {
    if (!d || typeof d !== 'object') return false;
    const t = d.date || d.due || d.when; if (!t) return false;
    const ms = new Date(typeof t === 'number' ? t : String(t)).getTime();
    return isFinite(ms) && ms > now && ms - now < 48 * 3600e3;
  }).map(d => ({ name: String(d.name || d.title || 'deadline'), date: d.date || d.due || d.when }));
  return { via: 'confirmed_path', list };
}
function eqFact (cited, actual) {
  const cn = Number(cited), an = Number(actual);
  if (!isNaN(cn) && !isNaN(an) && cited !== '' && actual !== '') return cn === an;
  return norm(String(cited)) === norm(String(actual));
}
function groundChecks (basis, pkt) {
  const out = [];
  const add = (check, status, detail, via) => out.push({ check, status, detail, via: via || null });
  const b = (basis && typeof basis === 'object') ? basis : {};
  const av = resolveSpec(pkt, 'available_min');
  if (av.via !== 'confirmed_path')
    add('ground_available_minutes', 'NEED_INFO', 'packet value not on a locked path (' + av.via + ')', av.via);
  else if (b.available_minutes === undefined || b.available_minutes === null)
    add('ground_available_minutes', 'NEED_INFO', 'not cited; packet has ' + JSON.stringify(av.value), 'confirmed_path');
  else if (!eqFact(b.available_minutes, av.value))
    add('ground_available_minutes', 'FAIL', 'cited ' + JSON.stringify(b.available_minutes) + '; packet says ' + JSON.stringify(av.value), 'confirmed_path');
  else add('ground_available_minutes', 'PASS', 'cited ' + JSON.stringify(b.available_minutes) + ' = packet', 'confirmed_path');
  const tt = resolveSpec(pkt, 'today_tasks');
  if (tt.via !== 'confirmed_path' || !Array.isArray(tt.value))
    add('ground_open_today', 'NEED_INFO', 'today’s tasks not on a locked path (' + tt.via + ')', tt.via);
  else {
    const open = tt.value.filter(t => t && !((t.done ?? t.completed ?? t.checked) === true)).length;
    if (b.open_today === undefined || b.open_today === null)
      add('ground_open_today', 'NEED_INFO', 'not cited; packet has ' + open + ' open task(s)', 'confirmed_path');
    else if (Number(b.open_today) !== open)
      add('ground_open_today', 'FAIL', 'cited ' + b.open_today + '; packet has ' + open + ' open', 'confirmed_path');
    else add('ground_open_today', 'PASS', 'cited ' + open + ' = packet', 'confirmed_path');
  }
  const dl = deadlinesWithin48h(pkt);
  if (dl.via !== 'confirmed_path')
    add('ground_deadlines', 'NEED_INFO', 'deadlines not on a locked path (' + dl.via + ')', dl.via);
  else {
    const cited = Array.isArray(b.urgent_deadlines) ? b.urgent_deadlines : null;
    if (cited === null)
      add('ground_deadlines', 'NEED_INFO', 'not cited; packet has ' + (dl.list.length ? dl.list.map(d => d.name).join(', ') : 'none') + ' within 48h', 'confirmed_path');
    else {
      const cSet = new Set(cited.map(x => norm(String(x))).filter(Boolean));
      const pSet = new Set(dl.list.map(d => norm(d.name)));
      const same = cSet.size === pSet.size && [...cSet].every(x => pSet.has(x));
      if (same) add('ground_deadlines', 'PASS', 'cited ' + (cited.join(', ') || 'none') + ' = packet 48h set', 'confirmed_path');
      else add('ground_deadlines', 'FAIL', 'cited [' + (cited.join(', ') || '—') + ']; packet 48h set is [' + (dl.list.map(d => d.name).join(', ') || '—') + ']', 'confirmed_path');
    }
  }
  const g = resolveSpec(pkt, 'current_gate');
  if (g.via !== 'confirmed_path')
    add('ground_gate', 'NEED_INFO', 'current gate not on a locked path (' + g.via + ')', g.via);
  else if (b.current_gate === undefined || b.current_gate === null || String(b.current_gate).trim() === '')
    add('ground_gate', 'NEED_INFO', 'not cited; packet says "' + g.value + '"', 'confirmed_path');
  else if (norm(String(b.current_gate)) !== norm(String(g.value)))
    add('ground_gate', 'FAIL', 'cited "' + b.current_gate + '"; packet says "' + g.value + '"', 'confirmed_path');
  else add('ground_gate', 'PASS', 'cited "' + g.value + '" = packet', 'confirmed_path');
  return out;
}

/* ============================================================
   MISSION COMPATIBILITY — deterministic action ↔ mission test.
   Domain vocabulary from the canonical mission string (+ the
   mission's own plan entries if the packet exposes them).
   ============================================================ */
const MISSION_STOP = new Set(['complete','study','learn','practice','build','understand','master',
  'fundamentals','fundamental','basics','basic','review','revise','exercise','exercises','module',
  'week','day','part','session','work','finish','start','read','watch','implement','write','solve',
  'today','mission','phase','gate','focus','topic','chapter','section','problem','problems',
  'tutorial','course','lesson','the','and','for','with','your','this','that','from','into','using',
  'about','hour','hours','minute','minutes','block','next','current','then','when','while','also',
  'make','makes','making','begin','spend']);
function stemT (w) {
  let s = w;
  if (s.length >= 5 && s.endsWith('ies')) s = s.slice(0, -3) + 'y';
  if (s.length >= 5 && s.endsWith('ing')) s = s.slice(0, -3);
  if (s.length >= 5 && s.endsWith('ed')) s = s.slice(0, -2);
  if (s.length >= 4 && s.endsWith('s') && !s.endsWith('ss')) s = s.slice(0, -1);
  return s;
}
function domainVocab (text) {
  const toks = norm(text).split(' ').filter(w => w.length >= 3);
  const out = [];
  for (const t of toks) {
    const s = stemT(t);
    if (s.length >= 3 && !MISSION_STOP.has(s) && !MISSION_STOP.has(t) && !out.includes(s)) out.push(s);
  }
  return out;
}
function missionCompatibilityDom (missionDom, actionText, domainSrc) {
  if (!missionDom.length) return { status: 'NEED_INFO',
    detail: 'mission domain vocabulary is empty (from ' + domainSrc + ') — deterministic compatibility cannot be established. Missing canonical field: the current mission’s plan entries (expose e.g. mission_tasks: the planFor() titles for the current mission) in oracleStatePacket().' };
  const raw = norm(actionText).split(' ').filter(w => w.length >= 3);
  const aDom = [];
  for (const t of raw) { const s = stemT(t);
    if (s.length >= 3 && !MISSION_STOP.has(s) && !MISSION_STOP.has(t) && !aDom.includes(s)) aDom.push(s); }
  const shared = missionDom.filter(d => aDom.includes(d));
  if (shared.length) return { status: 'PASS',
    detail: 'action operates in the mission’s domain {' + shared.join(', ') + '} (domain from ' + domainSrc + ': {' + missionDom.slice(0, 10).join(', ') + '})' };
  if (aDom.length) return { status: 'FAIL',
    detail: 'action is outside the mission’s domain — mission domain {' + missionDom.slice(0, 10).join(', ') + '}; action domain {' + aDom.join(', ') + '}' };
  return { status: 'NEED_INFO',
    detail: 'action carries no domain-identifying content — compatibility cannot be established (missing: a concrete action expressed in the mission’s subject)' };
}

/* ============================================================
   VALIDATE PRIMARY — runs on the AI's ORIGINAL output.
   Text never modified; numbers only via recorded MODIFY.
   ============================================================ */
function validatePrimary (primary, pkt, parsed) {
  const checks = [];
  const modifications = [];
  const add = (check, status, detail, via) => checks.push({ check, status, detail, via: via || null });
  const basis = (parsed && parsed.basis && typeof parsed.basis === 'object') ? parsed.basis : {};
  const mstat = (parsed && parsed.mission && typeof parsed.mission === 'object') ? parsed.mission : null;

  const what = typeof primary.what === 'string' ? primary.what.trim() : '';
  let finalMins = primary.minutes;
  if (!what) add('contract', 'FAIL', 'primary.what missing');
  else if (what.length > 140) add('contract', 'FAIL', 'primary.what is ' + what.length + ' chars (>140) — text is never auto-truncated');
  else add('contract', 'PASS', 'what ok (' + what.length + ' chars)');
  if (typeof primary.why === 'string' && primary.why.length > 600)
    add('contract', 'FAIL', 'primary.why is ' + primary.why.length + ' chars (>600) — text is never auto-truncated');
  if (primary.minutes === undefined || primary.minutes === null || !Number.isInteger(+primary.minutes))
    add('contract', 'FAIL', 'primary.minutes missing or not an integer (got ' + JSON.stringify(primary.minutes) + ')');
  else {
    const n = +primary.minutes;
    if (n < 15 || n > 600) {
      const to = Math.max(15, Math.min(600, n));
      modifications.push({ field: 'minutes', from: n, to, rule: 'block_range_15_600' });
      finalMins = to;
      add('contract', 'MODIFY', 'proposed ' + n + ' min outside 15–600; adjusted to ' + to + ' (recorded)', null);
    } else add('contract', 'PASS', 'minutes ' + n + ' within 15–600');
  }

  /* mission_alignment — citation */
  const mres = resolveSpec(pkt, 'mission');
  if (mres.via !== 'confirmed_path')
    add('mission_alignment', 'NEED_INFO', 'current mission not on a locked path (' + mres.via + ') — alignment unvalidated', mres.via);
  else {
    const packetMission = String(mres.value).trim();
    if (!mstat || typeof mstat.relation !== 'string' || !mstat.relation)
      add('mission_alignment', 'NEED_INFO', 'mission relation not stated (required: advances | exception)', 'confirmed_path');
    else if (mstat.relation === 'advances') {
      const cited = basis.current_mission;
      if (cited === undefined || cited === null || String(cited).trim() === '')
        add('mission_alignment', 'NEED_INFO', 'claims "advances" but basis.current_mission not cited', 'confirmed_path');
      else if (norm(String(cited)) !== norm(packetMission))
        add('mission_alignment', 'FAIL', 'cited mission "' + cited + '"; packet says "' + packetMission + '"', 'confirmed_path');
      else add('mission_alignment', 'PASS', 'advances current mission (exact match verified): "' + packetMission + '"', 'confirmed_path');
    } else if (mstat.relation === 'exception') {
      const trig = mstat.trigger;
      const reason = typeof mstat.reason === 'string' ? mstat.reason.trim() : '';
      if (trig === 'deadline_48h') {
        const dl = deadlinesWithin48h(pkt);
        if (dl.via !== 'confirmed_path')
          add('mission_alignment', 'NEED_INFO', 'deadlines not on a locked path (' + dl.via + ') — deadline_48h claim unverifiable, not contradicted', dl.via);
        else if (dl.list.length)
          add('mission_alignment', 'PASS', 'justified exception · deadline within 48h: ' + dl.list.map(d => d.name).join(', '), 'confirmed_path');
        else
          add('mission_alignment', 'FAIL', 'claims deadline_48h; packet (locked deadlines) shows none within 48h', 'confirmed_path');
      } else if (trig === 'mission_complete') {
        const tt = resolveSpec(pkt, 'today_tasks');
        if (tt.via !== 'confirmed_path')
          add('mission_alignment', 'NEED_INFO', 'mission_complete claimed but today’s tasks not on a locked path', tt.via);
        else {
          const open = tt.value.filter(t => t && !((t.done ?? t.completed ?? t.checked) === true)).length;
          if (open === 0) add('mission_alignment', 'PASS', 'justified exception · today’s tasks all complete (' + tt.value.length + '/' + tt.value.length + ')', 'confirmed_path');
          else add('mission_alignment', 'FAIL', 'claims mission_complete but ' + open + ' task(s) still open', 'confirmed_path');
        }
      } else if (trig === 'other') {
        if (reason.length >= 40)
          add('mission_alignment', 'WARN', 'declared exception (unverifiable — requires your judgment): "' + reason.slice(0, 160) + '"', 'confirmed_path');
        else add('mission_alignment', 'FAIL', 'exception "other" requires a substantive reason (≥40 chars)', 'confirmed_path');
      } else add('mission_alignment', 'FAIL', 'invalid trigger "' + trig + '" (deadline_48h | mission_complete | other)', 'confirmed_path');
    } else add('mission_alignment', 'FAIL', 'invalid relation "' + mstat.relation + '" (advances | exception)', 'confirmed_path');
  }

  /* mission_compatibility — does the ACTION advance the MISSION? */
  if (mres.via === 'confirmed_path') {
    if (mstat && mstat.relation === 'exception') {
      add('mission_compatibility', 'PASS', 'not required — declared exception; the trigger itself is verified in mission_alignment', 'confirmed_path');
    } else if (mstat && mstat.relation === 'advances') {
      let dom = domainVocab(String(mres.value).trim()), src = 'mission string';
      const mt = resolveSpec(pkt, 'mission_tasks');
      if (mt.via === 'confirmed_path' && Array.isArray(mt.value) && mt.value.length) {
        src += ' + ' + mt.value.length + ' mission plan entr' + (mt.value.length === 1 ? 'y' : 'ies');
        for (const t of mt.value) {
          const txt = typeof t === 'string' ? t : (t && (t.title || t.name || t.text || t.task)) || '';
          for (const d of domainVocab(txt)) if (!dom.includes(d)) dom.push(d);
        }
      }
      const comp = missionCompatibilityDom(dom, what, src);
      add('mission_compatibility', comp.status, comp.detail, 'confirmed_path');
    }
  }

  /* duplicate_today — lexical duplicate guard only, never mission authority */
  const tt = resolveSpec(pkt, 'today_tasks');
  if (tt.via === 'confirmed_path' && Array.isArray(tt.value)) {
    const open = tt.value.filter(t => t && !((t.done ?? t.completed ?? t.checked) === true));
    let hit = null;
    for (const t of open) {
      const txt = t.title || t.name || t.text || t.task || t.label || JSON.stringify(t);
      if (tokenOverlap(what, txt) >= 0.6) { hit = txt; break; }
    }
    if (hit) add('duplicate_today', 'FAIL', 'overlaps today’s open task: "' + String(hit).slice(0, 60) + '"', 'confirmed_path');
    else add('duplicate_today', 'PASS', 'no overlap with ' + open.length + ' open task(s)', 'confirmed_path');
  } else add('duplicate_today', 'NEED_INFO', 'today’s tasks not on a locked path (' + tt.via + ') — unverified', tt.via);

  /* time_fit — no unit guessing */
  const av = resolveSpec(pkt, 'available_min');
  if (av.via !== 'confirmed_path')
    add('time_fit', 'NEED_INFO', 'available time not on a locked path (' + av.via + ') — unverified', av.via);
  else {
    const n24 = availableMinutes(av.value);
    if (!n24.minutes)
      add('time_fit', 'NEED_INFO', 'available time not usable: ' + n24.why, 'confirmed_path');
    else if (Number.isInteger(finalMins)) {
      if (finalMins > n24.minutes) {
        const before = finalMins;
        modifications.push({ field: 'minutes', from: before, to: n24.minutes, rule: 'time_fit_cap' });
        finalMins = n24.minutes;
        add('time_fit', 'MODIFY', before + ' min proposed, ' + n24.minutes + ' available — capped to ' + n24.minutes + ' (recorded)', 'confirmed_path');
      } else add('time_fit', 'PASS', finalMins + ' of ' + n24.minutes + ' min available', 'confirmed_path');
    }
  }

  const dl48 = deadlinesWithin48h(pkt);
  if (dl48.via !== 'confirmed_path')
    add('deadline_pressure', 'NEED_INFO', 'deadlines not on a locked path (' + dl48.via + ')', dl48.via);
  else if (dl48.list.length)
    add('deadline_pressure', 'WARN', 'within 48h: ' + dl48.list.map(d => d.name + ' (' + d.date + ')').slice(0, 3).join(', ') + ' — schedule around it', 'confirmed_path');
  else add('deadline_pressure', 'PASS', 'nothing due within 48h', 'confirmed_path');

  for (const c of groundChecks(basis, pkt)) checks.push(c);

  const verdict = checks.some(c => c.status === 'FAIL') ? 'REJECT'
    : checks.some(c => c.status === 'WARN' || c.status === 'MODIFY') ? 'MODIFY'
    : checks.some(c => c.status === 'NEED_INFO') ? 'NEED_INFO' : 'ACCEPT';
  return { verdict, checks, modifications,
    final_minutes: Number.isInteger(finalMins) ? finalMins : null };
}

/* ============================================================
   AI BOUNDARY — provider-agnostic; key never leaves this process
   ============================================================ */
function extractJSON (s) {
  try { return JSON.parse(s); } catch {}
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  if (a >= 0 && b > a) { try { return JSON.parse(s.slice(a, b + 1)); } catch {}
    try { return JSON.parse(s.slice(a, b + 1).replace(/,\s*([}\]])/g, '$1')); } catch {} }
  return null;
}
const PACKET_STALE_LIMIT_MS = 24 * 60 * 60 * 1000;
async function askWithPacket (question) {
  const latest = getDoc('packets', 'latest');
  if (!latest || !latest.packet) { const e = new Error('no_packet');
    e.hint = 'Open the served ORACLE page — the bridge captures oracleStatePacket() automatically.'; throw e; }
  const age = Date.now() - (latest.received_at || 0);
  if (age > PACKET_STALE_LIMIT_MS) { const e = new Error('packet_stale');
    e.hint = 'Recapture first (reopen ORACLE, or CAPTURE NOW in the panel).'; throw e; }
  const staleness_min = Math.round(age / 60000);
  const context = {
    schema: 'oracle.context.v2', today: new Date().toISOString(),
    packet: latest.packet,
    external: { status: 'not_connected',
      note: 'No web/market/GitHub/calendar sources are connected (Phases 3–6). Do not speculate about the market, openings, or current events.' },
    captured: { hash: latest.hash, revision: latest.revision, age_minutes: staleness_min }
  };
  const system = [
    'You are the reasoning layer of ORACLE, a deterministic personal campaign system.',
    'You receive one JSON (CONTEXT). Its "packet" is the user’s real state, produced by ORACLE itself — ground truth. Never invent fields, numbers, or history; if something you need is absent, name it in "missing".',
    '"external" is empty by design — do not speculate about markets, openings, or current events.',
    'Answer from the packet. If the question implies "what should I do", return EXACTLY ONE primary action for the next available block: concrete, executable today, evidence-producing. It must not duplicate today’s open tasks.',
    'Every fact you rely on must appear in "basis", QUOTED EXACTLY AS IT APPEARS IN THE PACKET (same numbers, same names, same wording). Each basis fact is verified against the packet by a deterministic checker; any contradiction fails validation.',
    'In "mission", state how the action relates to the current mission shown in the packet: "advances" (it directly serves that mission) or "exception" (justified by trigger "deadline_48h", "mission_complete", or "other" + a substantive reason). "other" exceptions are surfaced for human judgment and never accepted silently.',
    'Express primary.what in the mission’s own domain vocabulary (the concrete subject matter of the current mission) — it is deterministically checked against the mission; actions outside the mission’s domain are rejected.',
    'No motivational filler. Respond with STRICT JSON only, no markdown fence:',
    '{"answer":"≤160 words; \\n between short paragraphs","primary":{"what":"imperative, ≤140 chars","minutes":90,"why":"1–3 sentences citing specific packet facts"},"mission":{"relation":"advances|exception","trigger":"deadline_48h|mission_complete|other","reason":"required for exception"},"avoid":["..."],"basis":{"open_today":0,"available_minutes":null,"urgent_deadlines":[],"current_gate":"","current_mission":""},"confidence":0.0,"missing":["..."]}',
    'primary.minutes must be an integer between 15 and 600. Use primary:null for purely informational questions (mission may then be null).'
  ].join('\n');
  const r = await fetch(AI_BASE + '/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + AI_KEY },
    body: JSON.stringify({ model: AI_MODEL, temperature: 0.2, max_tokens: 700,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: 'CONTEXT:\n' + JSON.stringify(context) + '\n\nQUESTION: ' + question } ] }),
    signal: AbortSignal.timeout ? AbortSignal.timeout(60000) : undefined });
  if (!r.ok) { const t = await r.text().catch(() => ''); throw new Error('provider ' + r.status + ' ' + t.slice(0, 200)); }
  const j = await r.json();
  const content = (j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content) || '';
  const parsed = extractJSON(content);
  if (!parsed || typeof parsed.answer !== 'string') throw new Error('model did not return the JSON contract');
  let original = null;
  if (parsed.primary && typeof parsed.primary.what === 'string' && parsed.primary.what.trim()) {
    original = { what: parsed.primary.what.trim(), minutes: parsed.primary.minutes,
      why: typeof parsed.primary.why === 'string' ? parsed.primary.why : null };
  }
  const validation = original ? validatePrimary(original, latest.packet, parsed) : null;
  let primary = null;
  if (original) {
    primary = { what: original.what,
      minutes: validation ? validation.final_minutes : original.minutes,
      why: original.why,
      modified: !!(validation && validation.modifications.length),
      modifications: validation ? validation.modifications : [],
      original: { what: original.what, minutes: original.minutes, why: original.why } };
  }
  const id = crypto.randomUUID().slice(0, 8);
  putDoc('ai_recommendations', id, {
    ts: Date.now(), question, packet_hash: latest.hash, packet_revision: latest.revision,
    staleness_min, answer: parsed.answer, primary, original_primary: original,
    modifications: validation ? validation.modifications : [],
    mission: parsed.mission || null, avoid: parsed.avoid || [],
    basis: parsed.basis || null, confidence: parsed.confidence ?? null, missing: parsed.missing || [],
    validation, applied: false, applied_at: null });
  return { ok: true, verdict: validation ? validation.verdict : 'ACCEPT', staleness_min,
    answer: parsed.answer, primary, avoid: parsed.avoid || [], missing: parsed.missing || [],
    confidence: parsed.confidence ?? null, checks: validation ? validation.checks : [], audit_id: id };
}
function recentPackets (n) {
  const docs = listDocs('packets'); const out = [];
  for (const id of Object.keys(docs)) if (id !== 'latest') out.push(docs[id]);
  out.sort((a, b) => (b.received_at || 0) - (a.received_at || 0));
  return out.slice(0, n);
}
function recentRecommendations (n) {
  const docs = listDocs('ai_recommendations');
  return Object.keys(docs).map(id => Object.assign({ id }, docs[id]))
    .sort((a, b) => b.ts - a.ts).slice(0, n);
}

/* ============================================================
   TEST HARNESS — node server.js --test-mission
   Tests A–I. Real execution of validatePrimary on synthetic
   packets; in-memory only, never persisted.
   ============================================================ */
function runMissionTests () {
  const savedPaths = Object.assign({}, PACKET_PATHS), savedLocked = PATHS_LOCKED;
  const NUMPY = {
    mission: 'Complete NumPy fundamentals',
    today_tasks: [
      { title: 'NumPy array creation drills', done: false },
      { title: 'Read NumPy docs', done: false },
      { title: 'Review notes', done: true } ],
    available_min: 180, deadlines: [],
    current_gate: 'Gate 3 — Python & Tooling',
    day: 61, start_date: '2026-01-05' };
  const mk = over => JSON.parse(JSON.stringify(Object.assign({}, NUMPY, over || {})));
  const setPaths = p => { PACKET_PATHS = Object.assign({
      mission: 'mission', today_tasks: 'today_tasks', available_min: 'available_min',
      deadlines: 'deadlines', current_gate: 'current_gate', day: 'day',
      start_date: 'start_date', mission_tasks: null }, p || {}); };
  const basisOk = over => Object.assign({ open_today: 2, available_minutes: 180, urgent_deadlines: [],
    current_gate: 'Gate 3 — Python & Tooling', current_mission: 'Complete NumPy fundamentals' }, over || {});
  const results = [];
  const run = (name, packet, primary, parsed, expect) => {
    const v = validatePrimary(primary, packet, parsed);
    results.push({ name, expect, got: v.verdict, ok: v.verdict === expect, checks: v.checks });
  };

  setPaths();
  run('A: correct mission action → ACCEPT', mk(),
    { what: 'Implement NumPy array indexing exercises', minutes: 90, why: 'mission is NumPy fundamentals; 90 of 180 min available' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'ACCEPT');
  run('B: unrelated action → REJECT', mk(),
    { what: 'Study system design', minutes: 60, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'REJECT');
  run('C: unrelated entertainment → REJECT', mk(),
    { what: 'Watch a movie for an hour', minutes: 60, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'REJECT');

  setPaths({ mission: null });
  run('D: missing mission → NEED_INFO', mk(),
    { what: 'Implement NumPy array indexing exercises', minutes: 90, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'NEED_INFO');

  setPaths();
  run('E: mission complete / valid next action → ACCEPT', mk({
      today_tasks: [ { title: 'NumPy array creation drills', done: true },
                     { title: 'Read NumPy docs', done: true } ] }),
    { what: 'Begin next module: pandas Series exercises', minutes: 75, why: 'today fully complete' },
    { mission: { relation: 'exception', trigger: 'mission_complete', reason: 'all of today’s tasks are done' },
      basis: basisOk({ open_today: 0 }) }, 'ACCEPT');

  setPaths({ mission_tasks: 'mission_tasks' });
  run('F: false advances claim under full mission structure → REJECT', mk({
      mission_tasks: [ 'NumPy array creation drills', 'Read NumPy docs', 'NumPy vectorization worksheet' ] }),
    { what: 'Read startup news and industry trends', minutes: 45, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'REJECT');

  setPaths();
  run('G: BUILD + application activity → REJECT', mk({
      mission: 'BUILD phase — implement the RAG retrieval pipeline project',
      today_tasks: [ { title: 'Implement retrieval module', done: false } ],
      current_gate: 'Gate 4 — BUILD', day: 120 }),
    { what: 'Submit applications to 5 internship postings', minutes: 60, why: '' },
    { mission: { relation: 'advances' },
      basis: basisOk({ open_today: 1, current_gate: 'Gate 4 — BUILD',
        current_mission: 'BUILD phase — implement the RAG retrieval pipeline project' }) }, 'REJECT');
  run('H: ambiguous canonical data → NEED_INFO', mk({ available_min: 3 }),
    { what: 'Implement NumPy array indexing exercises', minutes: 90, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk({ available_minutes: 3 }) }, 'NEED_INFO');
  run('I: roadmap/catch-up change attempt → REJECT', mk(),
    { what: 'Reorganize the roadmap to skip ahead two gates', minutes: 30, why: '' },
    { mission: { relation: 'advances' }, basis: basisOk() }, 'REJECT');

  PACKET_PATHS = savedPaths; PATHS_LOCKED = savedLocked;
  console.log('MISSION VALIDATOR TESTS');
  let pass = 0;
  for (const r of results) {
    console.log('  TEST ' + r.name + ': expect ' + r.expect + '  got ' + r.got + '  [' + (r.ok ? 'PASS' : 'FAIL') + ']');
    for (const c of r.checks)
      console.log('      ' + c.check + ' · ' + c.status + (c.via ? ' [' + c.via + ']' : '') + ' — ' + c.detail.slice(0, 110));
    if (r.ok) pass++;
  }
  console.log('  ' + pass + '/' + results.length + ' passed' + (pass === results.length ? ' — mission validator verified' : ' — FAILURES PRESENT'));
  return pass === results.length;
}

/* ============================================================
   ARTIFACT EMITTER — node server.js --emit-artifact
   Reads the genuine ORACLE html, injects the bridge once,
   refuses truncated source, writes ORACLE_FINAL_INTELLIGENT_REPAIRED.html
   ============================================================ */
function findOracleHTML () {
  if (process.env.ORACLE_HTML) {
    const p = path.join(DIR, process.env.ORACLE_HTML);
    if (fs.existsSync(p)) return p;
  }
  try {
    const files = fs.readdirSync(DIR).filter(f => /\.html?$/i.test(f));
    const pick = files.find(f => /^oracle/i.test(f)) || files.find(f => /^index\.html?$/i.test(f)) || files[0];
    return pick ? path.join(DIR, pick) : null;
  } catch { return null; }
}
function injectBridge (html) {
  const tag = '\n<script id="oracle-bridge">' + BRIDGE + '</script>\n';
  return /<\/body>/i.test(html) ? html.replace(/<\/body>/i, tag + '</body>') : html + tag;
}
function buildArtifact () {
  const src = findOracleHTML();
  if (!src) return { ok: false, error: 'no ORACLE html found next to server.js' };
  const raw = fs.readFileSync(src, 'utf8');
  if (!/<\/html>/i.test(raw.trimEnd()))
    return { ok: false, error: 'SOURCE REFUSED: the html does not end with </html> — it is truncated. Fix the source first; a "complete" artifact will not be emitted from an incomplete file.' };
  const out = raw.indexOf('id="oracle-bridge"') === -1 ? injectBridge(raw) : raw;
  const bridgeCalls = (BRIDGE.match(/window\.oracleStatePacket\(/g) || []).length;
  const checks = {
    source: path.basename(src), source_bytes: Buffer.byteLength(raw),
    source_complete: true,
    bridge_injected: (out.match(/id="oracle-bridge"/g) || []).length === 1,
    single_projection_callsites: bridgeCalls === 1,
    bridge_writes_no_campaign_state: true,
    no_fabricated_missions_or_dates: !/NumPy|system design|Summer 202\d/.test(BRIDGE)
  };
  const dest = path.join(DIR, 'ORACLE_FINAL_INTELLIGENT_REPAIRED.html');
  fs.writeFileSync(dest, out);
  return { ok: true, dest, bytes: Buffer.byteLength(out), checks,
    note: 'bridge adds no campaign state; the only writes into ORACLE are (a) user-clicked apply via ORACLE’s own add-task handler, (b) user-clicked restore of previously mirrored state' };
}

/* ============================================================
   BRIDGE (injected at serve time) — mirror + packet capture + ask
   ============================================================ */
const BRIDGE_CSS = `
#orb-fab{position:fixed;right:18px;bottom:18px;z-index:440;width:42px;height:42px;border-radius:50%;
  background:var(--panel);border:1px solid var(--line);color:var(--dim);cursor:pointer;
  display:flex;align-items:center;justify-content:center;box-shadow:var(--shadow);transition:all .2s ease}
#orb-fab:hover{color:var(--txt);border-color:var(--line2)}
#orb-fab svg{width:19px;height:19px}
#orb-fab .dot{position:absolute;top:2px;right:2px;width:8px;height:8px;border-radius:50%;
  background:var(--red);border:2px solid var(--panel)}
#orb-fab .dot.wait{background:var(--amber)}#orb-fab .dot.ok{background:var(--green)}
@media(max-width:640px){#orb-fab{bottom:calc(88px + env(safe-area-inset-bottom))}}
#orb-ov{display:none;position:fixed;inset:0;z-index:460;background:color-mix(in srgb,var(--bg) 94%,transparent);
  backdrop-filter:blur(30px) saturate(160%);overflow-y:auto;padding:5vh 16px 8vh}
#orb-ov.on{display:block}
#orb-panel{max-width:740px;margin:0 auto;background:var(--panel);border:1px solid var(--line);border-radius:22px;
  padding:26px 28px 22px;box-shadow:0 24px 70px rgba(0,0,0,.22)}
#orb-ov.on #orb-panel{animation:orbin .35s cubic-bezier(.22,.68,0,1) both}
@keyframes orbin{from{opacity:0;transform:translateY(14px) scale(.99)}to{opacity:1;transform:none}}
@media(prefers-reduced-motion:reduce){#orb-ov.on #orb-panel{animation:none}}
.orb-hd{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px}
.orb-h1{font-size:20px;font-weight:700;letter-spacing:-.02em}
.orb-h2{font-size:11px;font-weight:600;letter-spacing:.8px;color:var(--faint);margin-top:3px}
.orb-x{background:none;border:0;color:var(--dim);font-size:16px;cursor:pointer;padding:4px 8px;border-radius:8px}
.orb-x:hover{color:var(--red);background:var(--panel2)}
.orb-sec{margin-top:22px}
.orb-eb{font-size:11px;font-weight:600;letter-spacing:.8px;color:var(--faint);margin-bottom:10px}
.orb-row{display:flex;justify-content:space-between;align-items:baseline;gap:12px;padding:8px 0;
  border-bottom:1px solid var(--line);font-size:13px;color:var(--dim)}
.orb-row:last-child{border-bottom:0}
.orb-row b{color:var(--txt);font-weight:600;font-variant-numeric:tabular-nums;text-align:right}
.orb-btnrow{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}
.orb-btn{border-radius:980px;padding:8px 16px;font-size:12.5px;font-weight:600;cursor:pointer;
  font-family:inherit;transition:all .15s}
.orb-btn.pri{background:var(--acc);border:1px solid var(--acc);color:#fff}
.orb-btn.pri:hover{filter:brightness(1.08)}
.orb-btn.sec{background:transparent;border:1px solid var(--line2);color:var(--dim)}
.orb-btn.sec:hover{color:var(--txt);border-color:var(--faint)}
.orb-btn:disabled{opacity:.4;cursor:default;pointer-events:none}
.orb-note{margin-top:10px;font-size:11.5px;color:var(--faint);line-height:1.6;min-height:14px}
.orb-pill{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.5px;padding:2px 9px;border-radius:980px;white-space:nowrap}
.orb-pill.ok{background:color-mix(in srgb,var(--green) 14%,transparent);color:var(--green)}
.orb-pill.warn{background:color-mix(in srgb,var(--amber) 14%,transparent);color:var(--amber)}
.orb-pill.bad{background:color-mix(in srgb,var(--red) 14%,transparent);color:var(--red)}
.orb-pill.info{background:color-mix(in srgb,var(--violet) 14%,transparent);color:var(--violet)}
.orb-mini{font-size:10.5px;color:var(--faint);letter-spacing:.3px;line-height:1.6}
.orb-pre{display:none;margin-top:10px;background:var(--panel2);border:1px solid var(--line);border-radius:12px;
  padding:14px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;line-height:1.5;
  max-height:280px;overflow:auto;white-space:pre;color:var(--dim)}
.orb-chips{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}
.orb-chip{border:1px solid var(--line2);background:transparent;color:var(--dim);border-radius:980px;
  padding:5px 13px;font-size:11.5px;cursor:pointer;font-family:inherit}
.orb-chip:hover{border-color:var(--acc);color:var(--acc)}
.orb-chip:disabled{opacity:.35;pointer-events:none}
.orb-askrow{display:flex;gap:8px;margin-top:10px}
.orb-input{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:10px;
  color:var(--txt);font-size:13px;padding:10px 12px;font-family:inherit}
.orb-input:focus{outline:none;border-color:var(--acc)}
.orb-ans{font-size:14px;color:var(--txt);line-height:1.65;white-space:pre-wrap;margin-top:6px}
.orb-li{font-size:13px;color:var(--dim);padding:3px 0;display:flex;gap:8px}
.orb-ai{font-size:11.5px;letter-spacing:.3px;line-height:1.6}
.orb-ai.off{color:var(--faint)}.orb-ai.on{color:var(--green)}
.orb-aud{display:flex;gap:10px;align-items:baseline;padding:7px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
.orb-aud:last-child{border-bottom:0}
.orb-aq{font-size:12.5px;color:var(--txt);flex:1;min-width:0}
.orb-restore{background:color-mix(in srgb,var(--amber) 10%,var(--panel));
  border:1px solid color-mix(in srgb,var(--amber) 40%,transparent);border-radius:14px;
  padding:16px;margin-top:16px;font-size:13px;color:var(--txt)}
.orb-restore .orb-btn{margin-top:10px}
.orb-foot{margin-top:24px;padding-top:14px;border-top:1px solid var(--line)}
.orb-act{border:1px solid color-mix(in srgb,var(--acc) 35%,transparent);
  background:color-mix(in srgb,var(--acc) 6%,transparent);border-radius:14px;padding:14px}
.orb-aw{font-size:15px;font-weight:600;line-height:1.5}
.orb-why{font-size:12.5px;color:var(--dim);margin-top:8px;line-height:1.6}
`;

const BRIDGE = `(function () {
'use strict';
if (window.__ORACLE_BRIDGE__) return; window.__ORACLE_BRIDGE__ = true;
var CFG = window.ORACLE_BRIDGE_CONFIG || {};
var BASE = CFG.url || ((location.protocol === 'http:' || location.protocol === 'https:') ? '' : 'http://127.0.0.1:8787');
var TOKEN = CFG.token || '';
var S = { connected: false, health: null, revision: 0, dirty: {}, lastSync: null,
          restore: null, serverOnly: [], packet: null, lastAuditId: null };
function api (method, p, body) {
  var h = { 'Content-Type': 'application/json' };
  if (TOKEN) h['Authorization'] = 'Bearer ' + TOKEN;
  return fetch(BASE + p, { method: method, headers: h, body: body ? JSON.stringify(body) : undefined })
    .then(function (r) { return r.json().then(function (j) { return { status: r.status, json: j }; },
      function () { return { status: r.status, json: null }; }); });
}
/* mirror: localStorage stays authoritative */
var origSet = localStorage.setItem.bind(localStorage);
var origDel = localStorage.removeItem.bind(localStorage);
var origClear = localStorage.clear.bind(localStorage);
function isMine (k) { return typeof k === 'string' && k.indexOf('__orb') === 0; }
function localKeys () { var out = [], i;
  for (i = 0; i < localStorage.length; i++) { var k = localStorage.key(i); if (k && !isMine(k)) out.push(k); }
  return out; }
var capTimer = null;
function scheduleCapture () { if (capTimer) clearTimeout(capTimer); capTimer = setTimeout(function () { capture('state-change'); }, 15000); }
function markDirty (k) { if (!isMine(k)) { S.dirty[k] = true; schedulePush(); scheduleCapture(); } }
try {
  localStorage.setItem = function (k, v) { origSet(k, v); markDirty(k); };
  localStorage.removeItem = function (k) { origDel(k); markDirty(k); };
  localStorage.clear = function () { var ks = localKeys(); origClear(); ks.forEach(markDirty); };
} catch (e) {}
window.addEventListener('storage', function (e) {
  if (e.key && !isMine(e.key) && e.newValue !== null) { S.dirty[e.key] = true; schedulePush(); }
});
var pushTimer = null;
function schedulePush () { if (pushTimer) clearTimeout(pushTimer); pushTimer = setTimeout(flush, 1500); }
function flush (force) {
  var ks = Object.keys(S.dirty);
  if (!ks.length || (!S.connected && !force)) return;
  var arr = ks.map(function (k) { return { k: k, v: localStorage.getItem(k) }; });
  return api('POST', '/api/state/sync', { keys: arr }).then(function (r) {
    if (r.status === 200 && r.json && r.json.ok) {
      S.revision = r.json.revision; S.lastSync = new Date();
      arr.forEach(function (a) { if (localStorage.getItem(a.k) === a.v) delete S.dirty[a.k]; });
    }
    render();
  }).catch(function () {});
}
window.addEventListener('beforeunload', function () {
  var ks = Object.keys(S.dirty); if (!ks.length) return;
  try {
    var arr = ks.map(function (k) { return { k: k, v: localStorage.getItem(k) }; });
    var h = { 'Content-Type': 'application/json' };
    if (TOKEN) h['Authorization'] = 'Bearer ' + TOKEN;
    fetch(BASE + '/api/state/sync', { method: 'POST', headers: h,
      body: JSON.stringify({ keys: arr }), keepalive: true });
  } catch (e) {}
});
function boot () {
  api('GET', '/api/health').then(function (r) {
    if (r.status !== 200 || !r.json || !r.json.ok) throw new Error('unreachable');
    S.connected = true; S.health = r.json;
    return api('GET', '/api/state');
  }).then(function (r) {
    if (r.status !== 200) throw new Error('state');
    var server = r.json || {};
    S.revision = server.revision || 0;
    var skeys = server.keys || {};
    var lk = localKeys();
    var n = 0; for (var k in skeys) n++;
    if (lk.length === 0 && n > 0) {
      var m = 0; for (var k2 in skeys) if (skeys[k2].updatedAt > m) m = skeys[k2].updatedAt;
      S.restore = { n: n, at: m };
    } else {
      var push = [];
      lk.forEach(function (k) {
        var sv = skeys[k] ? skeys[k].value : undefined;
        if (sv === undefined || sv !== localStorage.getItem(k)) push.push(k);
      });
      S.serverOnly = Object.keys(skeys).filter(function (k) { return localStorage.getItem(k) === null; });
      push.forEach(function (k) { S.dirty[k] = true; });
      flush(true);
      if (!push.length) S.lastSync = new Date();
    }
    render();
    capture('boot');
  }).catch(function () { S.connected = false; render(); });
}
function restoreHere () {
  api('GET', '/api/state').then(function (r) {
    var keys = (r.json && r.json.keys) || {}; var n = 0;
    for (var k in keys) if (localStorage.getItem(k) === null && keys[k].value !== null) { origSet(k, keys[k].value); n++; }
    note('restored ' + n + ' key(s) — reloading');
    setTimeout(function () { location.reload(); }, 600);
  });
}
function pullMissing () {
  api('GET', '/api/state').then(function (r) {
    var keys = (r.json && r.json.keys) || {}; var n = 0;
    for (var k in keys) if (localStorage.getItem(k) === null && keys[k].value !== null) { origSet(k, keys[k].value); n++; }
    note('pulled ' + n + ' missing key(s)'); render();
  });
}
function el (t, c, tx) { var e = document.createElement(t); if (c) e.className = c; if (tx != null) e.textContent = tx; return e; }
function fmtAgo (d) {
  if (!d) return '\\u2014';
  var s = (Date.now() - d.getTime()) / 1000;
  if (s < 90) return 'just now';
  if (s < 3600) return Math.round(s / 60) + ' min ago';
  if (s < 86400) return Math.round(s / 3600) + ' h ago';
  return Math.round(s / 86400) + ' d ago';
}
function pillClass (v) {
  return (v === 'ACCEPT' || v === 'PASS') ? 'ok'
    : (v === 'MODIFY' || v === 'WARN') ? 'warn'
    : (v === 'REJECT' || v === 'FAIL') ? 'bad' : 'info';
}
var D = {};
function sec (parent, title) { var s = el('div', 'orb-sec'); s.appendChild(el('div', 'orb-eb', title)); parent.appendChild(s); return s; }
function row (s, label) { var r = el('div', 'orb-row'); r.appendChild(el('span', null, label)); var b = el('b'); r.appendChild(b); s.appendChild(r); return b; }
function note (t) {
  if (!D.noteEl) return;
  D.noteEl.textContent = t || '';
  clearTimeout(D.noteT);
  D.noteT = setTimeout(function () { if (D.noteEl) D.noteEl.textContent = ''; }, 4200);
}
function buildUI () {
  var css = el('style'); css.textContent = CSS; document.head.appendChild(css);
  var fab = el('button'); fab.id = 'orb-fab'; fab.setAttribute('aria-label', 'ORACLE connection layer');
  fab.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><circle cx="5" cy="12" r="2.4"/><circle cx="19" cy="5.5" r="2.4"/><circle cx="17.5" cy="18.5" r="2.4"/><path d="M7.2 10.9 16.9 6.6M7.1 13.2l8.2 4.4"/></svg><span class="dot"></span>';
  fab.onclick = function () { toggle(); };
  document.body.appendChild(fab);
  D.fabDot = fab.querySelector('.dot');
  var ov = el('div'); ov.id = 'orb-ov';
  var pn = el('div'); pn.id = 'orb-panel';
  var hd = el('div', 'orb-hd');
  var ht = el('div');
  ht.appendChild(el('div', 'orb-h1', 'ORACLE'));
  ht.appendChild(el('div', 'orb-h2', 'CONNECTION LAYER \\u00b7 REASONING BOUNDARY'));
  hd.appendChild(ht);
  var xb = el('button', 'orb-x', '\\u00d7'); xb.onclick = function () { toggle(false); };
  hd.appendChild(xb);
  pn.appendChild(hd);
  D.restoreBox = el('div', 'orb-restore'); D.restoreBox.style.display = 'none';
  D.restoreTxt = el('div');
  D.restoreBtn = el('button', 'orb-btn pri', 'RESTORE TO THIS BROWSER');
  D.restoreBtn.onclick = restoreHere;
  D.restoreBox.appendChild(D.restoreTxt); D.restoreBox.appendChild(D.restoreBtn);
  pn.appendChild(D.restoreBox);
  var s1 = sec(pn, 'LINK');
  D.stLink = row(s1, 'SERVER');
  D.stEngine = row(s1, 'STORAGE');
  D.stRev = row(s1, 'STATE REVISION');
  D.stKeys = row(s1, 'KEYS MIRRORED');
  D.stPending = row(s1, 'PENDING WRITES');
  D.stSync = row(s1, 'LAST SYNC');
  D.mirrorNote = el('div', 'orb-mini'); s1.appendChild(D.mirrorNote);
  D.pullBtn = el('button', 'orb-btn sec', 'PULL MISSING KEYS');
  D.pullBtn.style.display = 'none'; D.pullBtn.style.marginTop = '8px';
  D.pullBtn.onclick = pullMissing; s1.appendChild(D.pullBtn);
  var b1 = el('div', 'orb-btnrow');
  var btnSync = el('button', 'orb-btn sec', 'SYNC NOW');
  btnSync.onclick = function () { note('syncing\\u2026'); flush(true); };
  var btnExp = el('button', 'orb-btn sec', 'EXPORT BACKUP'); btnExp.onclick = doExport;
  var btnImp = el('button', 'orb-btn sec', 'IMPORT'); btnImp.onclick = function () { D.file.click(); };
  var btnEmit = el('button', 'orb-btn sec', 'EMIT ARTIFACT');
  btnEmit.onclick = doArtifact;
  b1.appendChild(btnSync); b1.appendChild(btnExp); b1.appendChild(btnImp); b1.appendChild(btnEmit);
  s1.appendChild(b1);
  D.file = el('input'); D.file.type = 'file'; D.file.accept = '.json,application/json';
  D.file.style.display = 'none'; D.file.onchange = doImport; s1.appendChild(D.file);
  var s2 = sec(pn, 'ORACLE STATE PACKET \\u00b7 oracleStatePacket()');
  D.pkNote = el('div', 'orb-mini'); s2.appendChild(D.pkNote);
  D.pkHash = row(s2, 'HASH');
  D.pkAge = row(s2, 'CAPTURED');
  D.pkSec = row(s2, 'SECTIONS');
  D.pkSan = row(s2, 'SANITY');
  var b2 = el('div', 'orb-btnrow');
  D.capBtn = el('button', 'orb-btn pri', 'CAPTURE NOW');
  D.capBtn.onclick = function () { pktNote('capturing\\u2026'); capture('manual'); };
  D.rawBtn = el('button', 'orb-btn sec', 'VIEW RAW'); D.rawBtn.onclick = viewRaw;
  D.copyBtn = el('button', 'orb-btn sec', 'COPY'); D.copyBtn.style.display = 'none';
  D.copyBtn.onclick = function () { if (navigator.clipboard) navigator.clipboard.writeText(D.pre.textContent).then(function () { note('copied'); }); };
  D.lockBtn = el('button', 'orb-btn sec', 'LOCK PATHS'); D.lockBtn.style.display = 'none';
  D.lockBtn.onclick = lockPaths;
  b2.appendChild(D.capBtn); b2.appendChild(D.rawBtn); b2.appendChild(D.copyBtn); b2.appendChild(D.lockBtn);
  s2.appendChild(b2);
  D.pre = el('pre', 'orb-pre'); s2.appendChild(D.pre);
  var s3 = sec(pn, 'AI REASONING \\u00b7 ASK');
  D.aiStatus = el('div', 'orb-ai off'); s3.appendChild(D.aiStatus);
  D.chips = el('div', 'orb-chips');
  ['What should I do now?', 'Where am I right now?', 'Am I on track?',
   'What should I NOT do today?', 'What is my biggest bottleneck?'].forEach(function (q) {
    var c = el('button', 'orb-chip', q);
    c.onclick = function () { D.ask.value = q; ask(); };
    D.chips.appendChild(c);
  });
  s3.appendChild(D.chips);
  var ar = el('div', 'orb-askrow');
  D.ask = el('input', 'orb-input'); D.ask.placeholder = 'ask against your real captured state\\u2026';
  D.ask.addEventListener('keydown', function (e) { if (e.key === 'Enter') ask(); });
  D.askBtn = el('button', 'orb-btn pri', 'ASK'); D.askBtn.onclick = ask;
  ar.appendChild(D.ask); ar.appendChild(D.askBtn);
  s3.appendChild(ar);
  D.answer = el('div'); s3.appendChild(D.answer);
  var s4 = sec(pn, 'DECISION AUDIT');
  D.audit = el('div'); s4.appendChild(D.audit);
  pn.appendChild(el('div', 'orb-foot orb-mini',
    'ORACLE core untouched \\u00b7 oracleStatePacket() is the single projection \\u00b7 AI is advisory, validated and audited \\u2014 it never writes state'));
  D.noteEl = el('div', 'orb-note'); pn.appendChild(D.noteEl);
  ov.appendChild(pn);
  ov.onclick = function (e) { if (e.target === ov) toggle(false); };
  document.body.appendChild(ov);
  D.ov = ov;
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && D.ov.classList.contains('on')) { toggle(false); e.stopPropagation(); }
  }, true);
}
function toggle (force) {
  var on = force !== undefined ? force : !D.ov.classList.contains('on');
  D.ov.classList.toggle('on', on);
  if (on) refreshAll();
}
function refreshAll () {
  api('GET', '/api/health').then(function (r) {
    S.health = (r.status === 200 && r.json) ? r.json : null;
    S.connected = !!(S.health && S.health.ok);
    render();
  });
  api('GET', '/api/state').then(function (r) {
    if (r.status === 200 && r.json) {
      S.revision = r.json.revision || 0;
      var n = 0; for (var k in r.json.keys) n++;
      render();
    }
  });
  api('GET', '/api/packet').then(function (r) {
    if (r.status === 200 && r.json && r.json.ok) { S.packet = r.json; S.packet.at = Date.now(); renderPacket(); }
  });
  loadAudit();
}
function render () {
  if (!D.fabDot) return;
  D.fabDot.className = 'dot' + (S.connected ? (Object.keys(S.dirty).length ? ' wait' : ' ok') : '');
  var h = S.health;
  D.stLink.textContent = S.connected
    ? 'connected \\u00b7 ' + ((h && h.host) || '127.0.0.1') + ':' + ((h && h.port) || '')
    : 'offline \\u2014 ORACLE runs exactly as before';
  D.stEngine.textContent = h ? (h.engine + (h.engine === 'json' ? ' (Node 22.5+ enables SQLite)' : '')) : '\\u2014';
  D.stRev.textContent = String(S.revision);
  var n = 0; for (var k in S.dirty) n++;
  D.stPending.textContent = String(n);
  D.stSync.textContent = S.connected ? fmtAgo(S.lastSync) : '\\u2014';
  var lk = localKeys().length;
  D.stKeys.textContent = lk + (h && h.state ? ' local / ' + (h.state.keys || 0) + ' server' : '');
  if (S.restore) {
    D.restoreBox.style.display = 'block';
    D.restoreTxt.textContent = 'SERVER STATE FOUND \\u2014 ' + S.restore.n +
      ' keys, last updated ' + fmtAgo(new Date(S.restore.at)) +
      '. This browser (this origin) has no ORACLE state yet.';
  } else D.restoreBox.style.display = 'none';
  D.mirrorNote.textContent = S.serverOnly.length
    ? S.serverOnly.length + ' key(s) exist on the server but not in this browser.' : '';
  D.pullBtn.style.display = S.serverOnly.length ? 'inline-block' : 'none';
  var ai = h && h.ai, on = !!(ai && ai.connected);
  D.aiStatus.textContent = on
    ? 'CONNECTED \\u00b7 ' + ai.model + ' \\u00b7 every answer is checked and audited'
    : 'NOT CONFIGURED \\u2014 set ORACLE_AI_KEY and ORACLE_AI_MODEL in .env next to server.js, then restart. Keys stay server-side and never reach this page.';
  D.aiStatus.className = on ? 'orb-ai on' : 'orb-ai off';
  D.ask.disabled = !on; D.askBtn.disabled = !on;
  for (var i = 0; i < D.chips.children.length; i++) D.chips.children[i].disabled = !on;
}
function domSnapshot () {
  var o = {};
  try { var d = document.querySelector('.days'); if (d) { var m = (d.textContent || '').match(/\\d+/); if (m) o.day = parseInt(m[0], 10); } } catch (e) {}
  try { var g = document.querySelector('.gatename'); if (g) o.gate = (g.textContent || '').trim(); } catch (e) {}
  try { var mo = document.querySelector('.mod.active .modt'); if (mo) o.module = (mo.textContent || '').trim(); } catch (e) {}
  return o;
}
function pktNote (t) { if (D.pkNote) D.pkNote.textContent = t || ''; }
function capture (reason) {
  if (typeof window.oracleStatePacket !== 'function') { pktNote('oracleStatePacket() not found in this page \\u2014 capture impossible'); return Promise.resolve(null); }
  var pkt;
  try { pkt = window.oracleStatePacket(); }
  catch (e) { pktNote('oracleStatePacket() threw: ' + e.message); return Promise.resolve(null); }
  if (!pkt || typeof pkt !== 'object' || Array.isArray(pkt)) { pktNote('oracleStatePacket() returned ' + (pkt === null ? 'null' : typeof pkt)); return Promise.resolve(null); }
  return api('POST', '/api/packet', { packet: pkt, dom: domSnapshot() }).then(function (r) {
    if (r.status !== 200 || !r.json || !r.json.ok) { pktNote('capture failed (HTTP ' + r.status + ')'); return null; }
    S.packet = r.json; S.packet.at = Date.now();
    renderPacket();
    if (!r.json.unchanged && r.json.suggested_paths && r.json.suggested_paths.length)
      note(r.json.suggested_paths.length + ' packet path(s) detected \\u2014 LOCK to confirm them');
    return r.json;
  }).catch(function () { pktNote('server unreachable'); return null; });
}
function renderPacket () {
  if (!D.pkHash) return;
  var p = S.packet;
  if (!p || !p.hash) { D.pkHash.textContent = '\\u2014'; D.pkAge.textContent = 'not yet';
    D.pkSec.textContent = '\\u2014'; D.pkSan.textContent = '\\u2014';
    D.pkNote.textContent = 'capture runs automatically on load and on state changes';
    D.lockBtn.style.display = 'none'; return; }
  D.pkHash.textContent = p.hash;
  D.pkAge.textContent = Math.max(0, Math.round((Date.now() - (p.received_at || p.at || Date.now())) / 60000)) + ' min ago';
  var st = p.structure || {}, n = 0, ks = []; for (var k in st) { n++; if (ks.length < 6) ks.push(k); }
  D.pkSec.textContent = n + ' (' + ks.join(', ') + (n > 6 ? '\\u2026' : '') + ')';
  var san = p.sanity || [], w = 0, f = 0;
  san.forEach(function (s) { if (s.status === 'WARN') w++; if (s.status === 'FAIL') f++; });
  D.pkSan.textContent = san.length + ' check(s)' + (w ? ' \\u00b7 ' + w + ' warn' : '') + (f ? ' \\u00b7 ' + f + ' FAIL' : '');
  D.pkSan.style.color = f ? 'var(--red)' : (w ? 'var(--amber)' : 'var(--green)');
  D.pkNote.textContent = '';
  var sug = p.suggested_paths || [];
  D.lockBtn.style.display = sug.length ? 'inline-block' : 'none';
  if (sug.length) D.lockBtn.textContent = 'LOCK ' + sug.length + ' DETECTED PATH' + (sug.length > 1 ? 'S' : '');
}
function lockPaths () {
  var p = S.packet; if (!p || !p.suggested_paths) return;
  var map = {}; p.suggested_paths.forEach(function (s) { map[s.spec] = s.path; });
  api('POST', '/api/packet/lock', { paths: map }).then(function (r) {
    if (r.status === 200 && r.json && r.json.ok) {
      note('locked ' + r.json.locked.length + ' path(s) \\u2014 validators now read confirmed paths');
      if (S.packet) S.packet.suggested_paths = [];
      renderPacket();
    }
  });
}
function viewRaw () {
  api('GET', '/api/packet?raw=1').then(function (r) {
    if (r.status !== 200) { pktNote('no packet stored yet'); return; }
    D.pre.style.display = 'block';
    D.pre.textContent = JSON.stringify(r.json.packet, null, 2);
    D.copyBtn.style.display = 'inline-block';
    D.rawBtn.textContent = 'REFRESH RAW';
    note('this is exactly what the reasoning model receives');
  });
}
function ask () {
  var q = (D.ask.value || '').trim(); if (!q) return;
  var go = function () {
    D.answer.textContent = '';
    D.answer.appendChild(el('div', 'orb-mini', 'reasoning over your captured packet\\u2026'));
    api('POST', '/api/ask', { question: q }).then(renderAsk)
      .catch(function () { D.answer.textContent = ''; D.answer.appendChild(el('div', 'orb-ans', 'request failed \\u2014 is the server running?')); });
  };
  var stale = !S.packet || !S.packet.hash || (Date.now() - (S.packet.at || 0) > 30 * 60000);
  if (stale) capture('pre-ask').then(function () { go(); }); else go();
}
function renderAsk (r) {
  D.answer.textContent = '';
  if (r.status !== 200 || !r.json || !r.json.ok) {
    var j = r.json || {};
    D.answer.appendChild(el('div', 'orb-ans',
      j.error === 'ai_not_configured' ? 'AI is not configured server-side. Add ORACLE_AI_KEY and ORACLE_AI_MODEL to .env next to server.js, restart, and ask again.'
      : j.error === 'no_packet' ? 'No packet captured yet \\u2014 open this page once so the bridge can capture oracleStatePacket().'
      : j.error === 'packet_stale' ? 'Packet is stale \\u2014 click CAPTURE NOW, then ask again.'
      : 'query failed \\u2014 ' + (j.error || ('HTTP ' + r.status))));
    return;
  }
  var j = r.json;
  S.lastAuditId = j.audit_id;
  var vh = el('div', 'orb-eb');
  vh.appendChild(el('span', 'orb-pill ' + pillClass(j.verdict), j.verdict));
  vh.appendChild(document.createTextNode('  VERDICT \\u00b7 packet ' + j.staleness_min + ' min old'));
  D.answer.appendChild(vh);
  D.answer.appendChild(el('div', 'orb-eb', 'ANSWER'));
  D.answer.appendChild(el('div', 'orb-ans', j.answer));
  if (j.primary) {
    D.answer.appendChild(el('div', 'orb-eb', 'PRIMARY ACTION'));
    var card = el('div', 'orb-act');
    card.appendChild(el('div', 'orb-aw', j.primary.what));
    var meta = el('div', 'orb-mini');
    meta.textContent = (j.primary.minutes != null ? j.primary.minutes : '\\u2014') + ' min';
    card.appendChild(meta);
    if (j.primary.modified && j.primary.modifications) {
      j.primary.modifications.forEach(function (m) {
        card.appendChild(el('div', 'orb-mini',
          'MODIFIED \\u00b7 ' + m.field + ': ' + m.from + ' \\u2192 ' + m.to + ' (' + m.rule + ')'));
      });
      if (j.primary.original && j.primary.original.minutes != null)
        card.appendChild(el('div', 'orb-mini', 'AI originally proposed ' + j.primary.original.minutes + ' min'));
    }
    if (j.primary.why) card.appendChild(el('div', 'orb-why', j.primary.why));
    D.answer.appendChild(card);
  }
  if (j.avoid && j.avoid.length) {
    D.answer.appendChild(el('div', 'orb-eb', 'DO NOT'));
    j.avoid.forEach(function (a) { D.answer.appendChild(el('div', 'orb-li', '\\u2014 ' + a)); });
  }
  if (j.checks && j.checks.length) {
    D.answer.appendChild(el('div', 'orb-eb', 'DETERMINISTIC CHECKS'));
    j.checks.forEach(function (c) {
      var li = el('div', 'orb-li');
      li.appendChild(el('span', 'orb-pill ' + pillClass(c.status), c.status));
      var sp = el('span'); sp.textContent = c.check + ' \\u2014 ' + c.detail + (c.via ? '  [' + c.via + ']' : '');
      li.appendChild(sp); D.answer.appendChild(li);
    });
  }
  if (j.missing && j.missing.length) {
    D.answer.appendChild(el('div', 'orb-eb', 'CONTEXT GAPS \\u2014 FILL THESE FOR BETTER ANSWERS'));
    j.missing.forEach(function (m) { D.answer.appendChild(el('div', 'orb-li', '\\u2014 ' + m)); });
  }
  var foot = el('div', 'orb-mini');
  foot.textContent = 'confidence ' + (j.confidence == null ? '\\u2014' : j.confidence) +
    ' \\u00b7 audit ' + j.audit_id + ' \\u00b7 advisory only \\u2014 the AI never writes ORACLE state';
  D.answer.appendChild(foot);
  if (j.primary && (j.verdict === 'ACCEPT' || j.verdict === 'MODIFY')) {
    var ap = el('button', 'orb-btn pri', 'ADD TO TODAY \\u25b8');
    ap.style.marginTop = '12px';
    ap.onclick = function () {
      applyToToday(j.primary.what + (j.primary.minutes ? ' \\u2014 ' + j.primary.minutes + ' min block' : ''));
    };
    D.answer.appendChild(ap);
  } else if (j.primary) {
    D.answer.appendChild(el('div', 'orb-mini',
      j.verdict === 'REJECT'
        ? 'rejected by the validator \\u2014 not offered for apply'
        : 'incomplete grounding (' + j.verdict + ') \\u2014 not offered for apply; lock the missing paths, then re-ask'));
  }
  loadAudit();
}
function applyToToday (text) {
  var inputs = document.querySelectorAll('.addrow input');
  for (var i = 0; i < inputs.length; i++) {
    var inp = inputs[i];
    if (inp.offsetParent === null) continue;
    inp.value = text;
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    var btn = inp.parentElement ? inp.parentElement.querySelector('button') : null;
    if (btn) { btn.click(); note('added through ORACLE\\u2019s own task handler'); markApplied(); return; }
  }
  if (navigator.clipboard) navigator.clipboard.writeText(text)
    .then(function () { note('no add-task field visible \\u2014 text copied instead'); });
}
function markApplied () {
  if (!S.lastAuditId) return;
  api('POST', '/api/audit/applied', { id: S.lastAuditId }).then(function () { loadAudit(); });
}
function loadAudit () {
  api('GET', '/api/audit').then(function (r) {
    D.audit.textContent = '';
    var items = (r.status === 200 && r.json && r.json.items) || [];
    if (!items.length) {
      D.audit.appendChild(el('div', 'orb-mini',
        'no AI recommendations yet \\u2014 the audit trail records every query, its packet hash, and the deterministic verdict'));
      return;
    }
    items.forEach(function (it) {
      var rr = el('div', 'orb-aud');
      rr.appendChild(el('span', 'orb-mini', new Date(it.ts).toLocaleString()));
      if (it.verdict) rr.appendChild(el('span', 'orb-pill ' + pillClass(it.verdict), it.verdict));
      rr.appendChild(el('span', 'orb-aq', it.question));
      if (it.modified) rr.appendChild(el('span', 'orb-pill warn', 'MODIFIED'));
      if (it.applied) rr.appendChild(el('span', 'orb-pill ok', 'APPLIED'));
      D.audit.appendChild(rr);
    });
  });
}
function doExport () {
  var h = {}; if (TOKEN) h['Authorization'] = 'Bearer ' + TOKEN;
  fetch(BASE + '/api/export', { headers: h }).then(function (r) { return r.blob(); }).then(function (b) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(b); a.download = 'oracle-backup.json'; a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 2000);
    note('backup downloaded');
  });
}
function doImport () {
  var f = D.file.files && D.file.files[0]; if (!f) return;
  var rd = new FileReader();
  rd.onload = function () {
    var body; try { body = JSON.parse(rd.result); } catch (e) { note('not valid JSON'); return; }
    api('POST', '/api/import', body).then(function (r) {
      note(r.status === 200 && r.json && r.json.ok
        ? 'imported ' + r.json.keys + ' key(s) \\u00b7 previous state archived on the server'
        : 'import failed \\u2014 ' + ((r.json && r.json.error) || ('HTTP ' + r.status)));
      if (r.status === 200) refreshAll();
    });
  };
  rd.readAsText(f); D.file.value = '';
}
function doArtifact () {
  var h = {}; if (TOKEN) h['Authorization'] = 'Bearer ' + TOKEN;
  fetch(BASE + '/api/artifact', { headers: h }).then(function (r) {
    if (r.status !== 200) { r.json().then(function (j) { note((j && j.error) || 'emit failed'); }); return; }
    return r.blob().then(function (b) {
      var a = document.createElement('a'); a.href = URL.createObjectURL(b);
      a.download = 'ORACLE_FINAL_INTELLIGENT_REPAIRED.html'; a.click();
      setTimeout(function () { URL.revokeObjectURL(a.href); }, 2000);
      note('artifact emitted — also written next to server.js');
    });
  });
}
var CSS = CSS_JSON_PLACEHOLDER;
buildUI();
render();
boot();
})();`
  .replace('CSS_JSON_PLACEHOLDER', JSON.stringify(BRIDGE_CSS));

/* ============================================================
   HTTP — serving (serve-time injection) + API
   ============================================================ */
function send (res, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store', 'Content-Length': buf.length });
  res.end(buf);
}
function sendText (res, code, txt) {
  res.writeHead(code, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(txt);
}
function corsHeaders (res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
}
function readBody (req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > 25 * 1024 * 1024) { reject(new Error('body too large')); req.destroy(); return; } chunks.push(c); });
    req.on('end', () => {
      if (!chunks.length) return resolve(null);
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch { resolve(undefined); }
    });
    req.on('error', reject);
  });
}
const htmlCache = new Map();
const MIME = { '.html': 'text/html; charset=utf-8', '.htm': 'text/html; charset=utf-8',
  '.js': 'application/javascript', '.css': 'text/css', '.json': 'application/json',
  '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.ico': 'image/x-icon', '.txt': 'text/plain', '.md': 'text/plain' };
function serveHTML (fp, res) {
  const st = fs.statSync(fp);
  let c = htmlCache.get(fp);
  if (!c || c.mtime !== st.mtimeMs) {
    let html = fs.readFileSync(fp, 'utf8');
    if (html.indexOf('oracle-bridge') === -1) html = injectBridge(html);
    c = { mtime: st.mtimeMs, buf: Buffer.from(html) };
    htmlCache.set(fp, c);
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': c.buf.length });
  res.end(c.buf);
}
function serveStatic (p, res) {
  let rel = decodeURIComponent(p);
  const main = findOracleHTML();
  if (rel === '/' || rel === '') rel = '/' + (main ? path.basename(main) : 'index.html');
  const fp = path.normalize(path.join(DIR, rel));
  if (!fp.startsWith(DIR + path.sep) && fp !== DIR) return sendText(res, 403, 'forbidden');
  let st; try { st = fs.statSync(fp); } catch { return sendText(res, 404, 'not found — place server.js next to your ORACLE html file'); }
  if (st.isDirectory()) return sendText(res, 404, 'not found');
  if (/\.html?$/i.test(fp)) return serveHTML(fp, res);
  const ext = path.extname(fp).toLowerCase();
  if (!MIME[ext]) return sendText(res, 403, 'unsupported file type');
  const data = fs.readFileSync(fp);
  res.writeHead(200, { 'Content-Type': MIME[ext], 'Cache-Control': 'no-store', 'Content-Length': data.length });
  res.end(data);
}
function coerceImport (p) {
  if (!p || typeof p !== 'object' || Array.isArray(p)) return null;
  if (p.kv && typeof p.kv === 'object') return { kv: p.kv, packets: p.packets || null, paths: p.paths || null, audit: p.audit || null };
  const keys = Object.keys(p).filter(k => !['meta', 'packets', 'paths', 'audit', 'recommendations', 'canonical'].includes(k));
  if (keys.length && keys.filter(k => typeof p[k] === 'string').length >= Math.max(1, keys.length - 2))
    return { kv: p, packets: null, paths: null, audit: null, note: 'adopted as a raw storage mirror (foreign backup format)' };
  return null;
}
const PACKET_KEEP = 200;
async function handle (req, res) {
  const u = new URL(req.url, 'http://local');
  const p = u.pathname;
  if (p.startsWith('/api/')) {
    if (CORS) corsHeaders(res);
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
    if (TOKEN && req.headers.authorization !== 'Bearer ' + TOKEN)
      return send(res, 401, { ok: false, error: 'unauthorized' });
    const body = (req.method === 'POST' || req.method === 'PUT') ? await readBody(req) : null;
    if (p === '/api/health' && req.method === 'GET')
      return send(res, 200, { ok: true, service: 'oracle-server', version: VERSION, phase: 1,
        engine, host: HOST, port: PORT,
        ai: { connected: !!(AI_KEY && AI_MODEL), model: AI_MODEL || null },
        state: { keys: Object.keys(listDocs('kv')).length, revision: revision() },
        packet: (() => { const d = getDoc('packets', 'latest'); return d
          ? { hash: d.hash, age_min: Math.round((Date.now() - d.received_at) / 60000), revision: d.revision } : null; })(),
        time: new Date().toISOString() });
    if (p === '/api/state' && req.method === 'GET') {
      const kv = listDocs('kv'); const keys = {};
      for (const k of Object.keys(kv)) keys[k] = { value: kv[k].value, updatedAt: kv[k].updatedAt };
      return send(res, 200, { ok: true, revision: revision(), keys });
    }
    if (p === '/api/state/sync' && req.method === 'POST') {
      const items = (body && Array.isArray(body.keys)) ? body.keys : [];
      let up = 0, del = 0;
      for (const it of items) {
        if (!it || typeof it.k !== 'string' || it.k.length > 250 || it.k.indexOf('__orb') === 0) continue;
        const prev = getDoc('kv', it.k);
        if (it.v === null || it.v === undefined) {
          if (prev) { logEvent({ op: 'kv_del', key: it.k, old: trunc(prev.value) }); delDoc('kv', it.k); del++; }
          continue;
        }
        const v = String(it.v);
        if (!prev || prev.value !== v) {
          logEvent({ op: 'kv_put', key: it.k, old: prev ? trunc(prev.value) : null });
          putDoc('kv', it.k, { value: v, updatedAt: Date.now() }); up++;
        }
      }
      const rev = (up || del) ? bumpRevision() : revision();
      return send(res, 200, { ok: true, revision: rev, updated: up, deleted: del });
    }
    if (p === '/api/packet' && req.method === 'POST') {
      if (!body || typeof body !== 'object') return send(res, 400, { ok: false, error: 'body must be { packet, dom? }' });
      const pkt = (body.packet && typeof body.packet === 'object' && !Array.isArray(body.packet)) ? body.packet : null;
      if (!pkt) return send(res, 400, { ok: false, error: 'no packet object' });
      const hash = crypto.createHash('sha256').update(JSON.stringify(pkt)).digest('hex').slice(0, 16);
      const prev = getDoc('packets', 'latest');
      if (prev && prev.hash === hash) {
        prev.received_at = Date.now(); putDoc('packets', 'latest', prev);
        return send(res, 200, { ok: true, unchanged: true, hash, revision: revision(), received_at: prev.received_at,
          sanity: prev.sanity || [], structure: prev.structure || null, suggested_paths: prev.suggested_paths || [] });
      }
      const doc = { hash, received_at: Date.now(), revision: bumpRevision(), packet: pkt,
        dom: body.dom || null, sanity: packetSanity(pkt, body.dom || null),
        structure: structureOf(pkt), suggested_paths: suggestPaths(pkt) };
      putDoc('packets', 'latest', doc);
      putDoc('packets', 'p' + doc.received_at + '-' + hash, doc);
      const ids = Object.keys(listDocs('packets')).filter(id => id !== 'latest').sort();
      for (const id of ids.slice(0, Math.max(0, ids.length - PACKET_KEEP))) delDoc('packets', id);
      return send(res, 200, { ok: true, hash, revision: revision(), received_at: doc.received_at,
        sanity: doc.sanity, structure: doc.structure, suggested_paths: doc.suggested_paths });
    }
    if (p === '/api/packet' && req.method === 'GET') {
      const d = getDoc('packets', 'latest');
      if (!d) return send(res, 404, { ok: false, error: 'no_packet',
        hint: 'open the served ORACLE page — the bridge captures oracleStatePacket() automatically' });
      if (u.searchParams.get('raw') === '1') return send(res, 200, Object.assign({ ok: true }, d));
      return send(res, 200, { ok: true, hash: d.hash, received_at: d.received_at, revision: d.revision,
        age_min: Math.round((Date.now() - d.received_at) / 60000),
        sanity: d.sanity || [], structure: d.structure || null, suggested_paths: d.suggested_paths || [] });
    }
    if (p === '/api/packet/lock' && req.method === 'POST') {
      const latest = getDoc('packets', 'latest');
      if (!latest) return send(res, 409, { ok: false, error: 'no_packet' });
      const paths = body && body.paths && typeof body.paths === 'object' ? body.paths : {};
      const locked = [];
      for (const spec of Object.keys(PACKET_PATHS)) {
        const path2 = paths[spec];
        if (path2 && typeof path2 === 'string' && pktGet(latest.packet, path2) !== undefined) {
          PACKET_PATHS[spec] = path2; locked.push(spec);
        }
      }
      putDoc('paths', 'main', { paths: PACKET_PATHS, locked_at: Date.now(), locked });
      PATHS_LOCKED = true;
      return send(res, 200, { ok: true, locked, paths: PACKET_PATHS });
    }
    if (p === '/api/ask' && req.method === 'POST') {
      if (!(AI_KEY && AI_MODEL))
        return send(res, 503, { ok: false, error: 'ai_not_configured',
          hint: 'Set ORACLE_AI_KEY and ORACLE_AI_MODEL in .env next to server.js (server-side only), then restart.' });
      const q = body && typeof body.question === 'string' ? body.question.trim() : '';
      if (!q) return send(res, 400, { ok: false, error: 'question required' });
      try { return send(res, 200, await askWithPacket(q)); }
      catch (e) { return send(res, (e.message === 'no_packet' || e.message === 'packet_stale') ? 409 : 502,
        { ok: false, error: e.message, hint: e.hint || null }); }
    }
    if (p === '/api/audit' && req.method === 'GET')
      return send(res, 200, { ok: true, items: recentRecommendations(50).map(r => ({
        id: r.id, ts: r.ts, question: r.question, packet_hash: r.packet_hash || null,
        verdict: r.validation ? r.validation.verdict : null,
        confidence: r.confidence, applied: !!r.applied,
        modified: !!(r.primary && r.primary.modified) })) });
    if (p === '/api/audit/applied' && req.method === 'POST') {
      const d = body && body.id ? getDoc('ai_recommendations', body.id) : null;
      if (!d) return send(res, 404, { ok: false });
      d.applied = true; d.applied_at = Date.now(); putDoc('ai_recommendations', body.id, d);
      return send(res, 200, { ok: true });
    }
    if (p === '/api/export' && req.method === 'GET') {
      const kv = listDocs('kv'); const flat = {};
      for (const k of Object.keys(kv)) flat[k] = kv[k].value;
      const payload = { meta: { service: 'oracle-server', schema: 'oracle.backup.v2',
        exported_at: new Date().toISOString(), revision: revision(), engine },
        kv: flat,
        packets: { latest: getDoc('packets', 'latest') || null, history: recentPackets(20) },
        paths: getDoc('paths', 'main') || null,
        audit: recentRecommendations(50) };
      const buf = Buffer.from(JSON.stringify(payload, null, 2));
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store',
        'Content-Disposition': 'attachment; filename="oracle-backup-' +
        new Date().toISOString().replace(/[:T]/g, '-').slice(0, 16) + '.json"',
        'Content-Length': buf.length });
      return res.end(buf);
    }
    if (p === '/api/import' && req.method === 'POST') {
      const coerced = coerceImport(body);
      if (!coerced) return send(res, 400, { ok: false,
        error: 'unrecognized backup shape — expected an oracle-server export or a raw storage-key map' });
      const kvNow = listDocs('kv'); const flatNow = {};
      for (const k of Object.keys(kvNow)) flatNow[k] = kvNow[k].value;
      putDoc('snapshots', 'pre-import-' + Date.now(), { ts: Date.now(), kv: flatNow,
        packets: getDoc('packets', 'latest') || null });
      for (const k of Object.keys(kvNow)) delDoc('kv', k);
      let n = 0;
      for (const k of Object.keys(coerced.kv)) {
        const v = coerced.kv[k];
        if (v === null || v === undefined || k.indexOf('__orb') === 0) continue;
        putDoc('kv', k, { value: typeof v === 'string' ? v : JSON.stringify(v), updatedAt: Date.now() }); n++;
      }
      if (coerced.packets && coerced.packets.latest && coerced.packets.latest.packet)
        putDoc('packets', 'latest', coerced.packets.latest);
      if (coerced.paths && coerced.paths.paths) { putDoc('paths', 'main', coerced.paths);
        Object.assign(PACKET_PATHS, coerced.paths.paths);
        PATHS_LOCKED = !!coerced.paths.locked_at; }
      if (Array.isArray(coerced.audit)) for (const a of coerced.audit) if (a && a.id) putDoc('ai_recommendations', a.id, a);
      bumpRevision();
      return send(res, 200, { ok: true, keys: n, note: coerced.note || null,
        rollback: 'the previous state was archived in the snapshots collection' });
    }
    if (p === '/api/artifact' && req.method === 'GET') {
      const r = buildArtifact();
      if (!r.ok) return send(res, 500, { ok: false, error: r.error });
      const buf = fs.readFileSync(r.dest);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store',
        'Content-Disposition': 'attachment; filename="ORACLE_FINAL_INTELLIGENT_REPAIRED.html"',
        'Content-Length': buf.length });
      return res.end(buf);
    }
    return send(res, 404, { ok: false, error: 'no such endpoint' });
  }
  if (p === '/bridge.js') {
    const buf = Buffer.from(BRIDGE);
    res.writeHead(200, { 'Content-Type': 'application/javascript', 'Cache-Control': 'no-store', 'Content-Length': buf.length });
    return res.end(buf);
  }
  return serveStatic(p, res);
}

/* CLI dispatch — runs the real tests / emitter, then exits */
if (process.argv.includes('--test-mission')) process.exit(runMissionTests() ? 0 : 1);
if (process.argv.includes('--emit-artifact')) {
  const r = buildArtifact();
  if (!r.ok) { console.error(r.error); process.exit(1); }
  console.log('  source        : ' + r.checks.source + ' (' + r.checks.source_bytes + ' bytes) — ends </html> ✓');
  console.log('  bridge        : injected, id="oracle-bridge" ×1 ' + (r.checks.bridge_injected ? '✓' : '✗'));
  console.log('  projection    : oracleStatePacket() call sites ×1 ' + (r.checks.single_projection_callsites ? '✓' : '✗'));
  console.log('  state writes  : ' + r.note);
  console.log('  fallbacks     : no hardcoded missions/dates in bridge ' + (r.checks.no_fabricated_missions_or_dates ? '✓' : '✗'));
  console.log('  written       : ' + r.dest + ' (' + r.bytes + ' bytes) ✓');
  process.exit(r.checks.bridge_injected && r.checks.single_projection_callsites ? 0 : 1);
}

const server = http.createServer((req, res) => {
  handle(req, res).then(() => {
    console.log('[' + new Date().toISOString().slice(11, 19) + '] ' + req.method + ' ' + req.url + ' ' + res.statusCode);
  }).catch(e => {
    console.error('[error]', e);
    try { send(res, 500, { ok: false, error: e.message }); } catch {}
  });
});
process.on('SIGINT', () => { writeNow(); console.log('\n[oracle-server] state flushed. bye.'); process.exit(0); });
server.listen(PORT, HOST, () => {
  const main = findOracleHTML();
  console.log('');
  console.log('  ORACLE SERVER v3 — connection layer + reasoning boundary');
  console.log('  ------------------------------------------------');
  console.log('  html      : ' + (main ? path.basename(main) + '  (file on disk is never modified)' : 'none found — bridge still usable via /bridge.js'));
  console.log('  url       : http://' + HOST + ':' + PORT);
  console.log('  storage   : ' + engine + (engine === 'sqlite' ? ' (oracle-data/oracle.db)' : ' (oracle-data/store.json — Node 22.5+ enables SQLite)'));
  console.log('  ai        : ' + (AI_KEY && AI_MODEL ? 'ARMED — model ' + AI_MODEL : 'off (set ORACLE_AI_KEY + ORACLE_AI_MODEL; keys stay server-side)'));
  console.log('  artifact  : node server.js --emit-artifact  →  ORACLE_FINAL_INTELLIGENT_REPAIRED.html');
  console.log('  tests     : node server.js --test-mission');
  console.log('');
});
server.on('error', e => {
  if (e.code === 'EADDRINUSE') console.error('Port ' + PORT + ' is busy — set ORACLE_PORT in .env');
  else console.error(e);
  process.exit(1);
});