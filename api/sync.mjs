// 乙級送電計畫：帳號（只用名稱、不用密碼）與進度同步
// POST /api/sync  {op:'register'|'pull'|'push'|'delete'|'board', name, data?, force?}
//                 {op:'fb', k, t, who}  題目回饋：Redis hash「fb」，欄位「題號|來源」，t 空字串＝刪除
//                 {op:'fblist'}          列出全部回饋 [{k, t, at}]（不含來源）
// 風雲榜：Redis hash「lb」，每個帳號一欄 {m:遮罩名稱, xp:總積分, dx:{日期:當日積分}, at:最後上傳}。
// 榜上只放遮罩後的名稱（第一個字＋○○），完整帳號名稱不會傳給別人。
// 每個帳號是一筆 Redis 資料，存活 30 天；每次讀寫都重設 30 天，所以 30 天沒用就自動刪除。
// 環境變數：Vercel 的 Upstash for Redis 整合會自動加上 KV_REST_API_URL／KV_REST_API_TOKEN
// （或 UPSTASH_REDIS_REST_URL／UPSTASH_REDIS_REST_TOKEN）。本機測試可設 SYNC_DEV_MEMORY=1 改用記憶體。

const TTL = 30 * 24 * 60 * 60;            // 30 天（秒）
const MAX_BYTES = 512 * 1024;             // 單一帳號進度上限
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const MEM = process.env.SYNC_DEV_MEMORY === '1' ? (globalThis.__syncMem ||= new Map()) : null;

async function redis(cmd) {
  if (MEM) return memRedis(cmd);
  const r = await fetch(URL_, { method: 'POST', headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error('redis: ' + (j.error || r.status));
  return j.result;
}

// 只實作用到的指令，行為比照 Redis（含到期）
function memRedis([op, key, ...a]) {
  const now = Date.now(), e = MEM.get(key);
  const live = e && e.exp > now ? e : (MEM.delete(key), null);
  const exp = () => now + TTL * 1000;
  if (op === 'GETEX') { if (live) live.exp = exp(); return live ? live.v : null; }
  if (op === 'GET') return live ? live.v : null;
  if (op === 'SET') {
    if (a.includes('NX') && live) return null;
    if (a.includes('XX') && !live) return null;
    MEM.set(key, { v: a[0], exp: exp() }); return 'OK';
  }
  if (op === 'DEL') { MEM.delete(key); return live ? 1 : 0; }
  if (op === 'HSET') { const h = (live && live.h) || {}; h[a[0]] = a[1]; MEM.set(key, { h, exp: Infinity }); return 1; }
  if (op === 'HGETALL') return live && live.h ? Object.entries(live.h).flat() : [];
  if (op === 'HLEN') return live && live.h ? Object.keys(live.h).length : 0;
  if (op === 'HDEL') { if (live && live.h) a.forEach(f => delete live.h[f]); return 1; }
  throw new Error('unsupported ' + op);
}

const json = (status, body) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' } });

// 名稱：去頭尾空白、全形半形統一；2–20 字；不分大小寫
function cleanName(n) {
  if (typeof n !== 'string') return null;
  const s = n.normalize('NFKC').trim().replace(/\s+/g, ' ');
  const len = [...s].length;
  if (len < 2 || len > 20 || /[\u0000-\u001f\u007f]/.test(s)) return null;
  return s;
}
const keyOf = name => 'acct:' + name.toLowerCase();
const LB = 'lb', FB = 'fb', DAY = 864e5, FB_MAX = 1000, FB_ROWS = 20000;
const mask = name => [...name][0] + '○○';
// 台北時間的日期字串，格式與前端 dstr() 相同（YYYY-M-D，不補零）
const tpeDay = ms => { const d = new Date(ms + 8 * 3600e3); return d.getUTCFullYear() + '-' + (d.getUTCMonth() + 1) + '-' + d.getUTCDate(); };
const lastDays = (n, now) => Array.from({ length: n }, (_, i) => tpeDay(now - i * DAY));
const num = v => (typeof v === 'number' && isFinite(v) && v > 0 ? Math.round(v) : 0);
// 只留最近 8 天的每日積分
function lbEntry(name, data, now) {
  const keep = new Set(lastDays(8, now)), dx = {};
  if (data.dx && typeof data.dx === 'object') for (const [k, v] of Object.entries(data.dx)) if (keep.has(k) && num(v)) dx[k] = num(v);
  return JSON.stringify({ m: mask(name), xp: num(data.xp), dx, at: now });
}
async function board(meKey, now) {
  const flat = await redis(['HGETALL', LB]) || [], week = new Set(lastDays(7, now)), rows = [], stale = [];
  for (let i = 0; i < flat.length; i += 2) {
    let e; try { e = JSON.parse(flat[i + 1]); } catch { continue; }
    if (now - (e.at || 0) > TTL * 1000) { stale.push(flat[i]); continue; }   // 30 天沒上傳＝帳號已到期
    rows.push({ k: flat[i], m: e.m, xp: num(e.xp), wk: Object.entries(e.dx || {}).reduce((s, [d, v]) => s + (week.has(d) ? num(v) : 0), 0) });
  }
  if (stale.length) await redis(['HDEL', LB, ...stale]);
  const rank = f => {
    const list = rows.filter(r => r[f] > 0).sort((a, b) => b[f] - a[f]);
    const top = list.slice(0, 20).map((r, i) => ({ r: i + 1, m: r.m, s: r[f], me: r.k === meKey || undefined }));
    const mi = list.findIndex(r => r.k === meKey);
    return { top, me: mi >= 20 ? { r: mi + 1, m: list[mi].m, s: list[mi][f], me: true } : null, n: list.length };
  };
  return { week: rank('wk'), total: rank('xp') };
}

export async function POST(request) {
  if (!MEM && (!URL_ || !TOKEN)) return json(503, { error: '雲端資料庫尚未設定' });
  const raw = await request.text();
  if (raw.length > MAX_BYTES) return json(413, { error: '進度資料太大' });
  let b; try { b = JSON.parse(raw); } catch { return json(400, { error: '格式錯誤' }); }
  const now = Date.now();
  if (b && b.op === 'board') {
    const n = cleanName(b.name);
    try { return json(200, await board(n ? keyOf(n) : null, now)); } catch { return json(502, { error: '雲端資料庫暫時無法連線' }); }
  }
  if (b && b.op === 'fb') {
    const k = typeof b.k === 'string' && /^\d{1,2}-\d{1,3}$/.test(b.k) ? b.k : null;
    const who = typeof b.who === 'string' ? b.who.normalize('NFKC').trim().toLowerCase().slice(0, 40) : '';
    const t = typeof b.t === 'string' ? b.t.trim().slice(0, FB_MAX) : null;
    if (!k || !who || t === null) return json(400, { error: '格式錯誤' });
    try {
      if (!t) { await redis(['HDEL', FB, k + '|' + who]); return json(200, { ok: true }); }
      if (await redis(['HLEN', FB]) >= FB_ROWS) return json(507, { error: '回饋已滿' });
      await redis(['HSET', FB, k + '|' + who, JSON.stringify({ t, at: now })]);
      return json(200, { ok: true });
    } catch { return json(502, { error: '雲端資料庫暫時無法連線' }); }
  }
  if (b && b.op === 'fblist') {
    try {
      const flat = await redis(['HGETALL', FB]) || [], rows = [];
      for (let i = 0; i < flat.length; i += 2) { let e; try { e = JSON.parse(flat[i + 1]); } catch { continue; } rows.push({ k: flat[i].split('|')[0], t: e.t, at: e.at }); }
      rows.sort((a, b2) => a.k.localeCompare(b2.k, 'en', { numeric: true }) || a.at - b2.at);
      return json(200, { n: rows.length, rows });
    } catch { return json(502, { error: '雲端資料庫暫時無法連線' }); }
  }
  const name = cleanName(b && b.name);
  if (!name) return json(400, { error: '帳號名稱需為 2–20 個字' });
  const key = keyOf(name);
  const okData = d => d && typeof d === 'object' && !Array.isArray(d);

  try {
    if (b.op === 'register') {
      const data = okData(b.data) ? b.data : null;
      const rec = { name, c: now, t: (data && data.t) || 0, data };
      const r = await redis(['SET', key, JSON.stringify(rec), 'NX', 'EX', String(TTL)]);
      if (r === null) return json(409, { error: '這個名稱已經有人使用' });
      if (data && num(data.xp)) await redis(['HSET', LB, key, lbEntry(name, data, now)]);
      return json(200, { name, t: rec.t });
    }
    if (b.op === 'pull') {
      const v = await redis(['GETEX', key, 'EX', String(TTL)]);
      if (v === null) return json(404, { error: '找不到這個帳號' });
      const rec = JSON.parse(v);
      return json(200, { name: rec.name, t: rec.t, data: rec.data });
    }
    if (b.op === 'push') {
      if (!okData(b.data)) return json(400, { error: '沒有進度資料' });
      const v = await redis(['GET', key]);
      if (v === null) return json(404, { error: '找不到這個帳號' });
      const rec = JSON.parse(v);
      // 雲端比較新（別的瀏覽器剛存過）就退回雲端版本，除非指定覆蓋
      if (!b.force && (rec.t || 0) > (b.data.t || 0)) return json(409, { name: rec.name, t: rec.t, data: rec.data });
      const xpChanged = num(b.data.xp) !== num(rec.data && rec.data.xp);
      rec.data = b.data; rec.t = b.data.t || now;
      await redis(['SET', key, JSON.stringify(rec), 'XX', 'EX', String(TTL)]);
      if (xpChanged) await redis(['HSET', LB, key, lbEntry(rec.name, b.data, now)]);   // 積分沒變就不必更新榜
      return json(200, { name: rec.name, t: rec.t });
    }
    if (b.op === 'delete') {
      await redis(['DEL', key]);
      await redis(['HDEL', LB, key]);
      return json(200, { ok: true });
    }
    return json(400, { error: '未知的操作' });
  } catch (e) {
    return json(502, { error: '雲端資料庫暫時無法連線' });
  }
}
