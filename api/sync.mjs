// 乙級送電計畫：帳號（只用名稱、不用密碼）與進度同步
// POST /api/sync  {op:'register'|'pull'|'push'|'delete', name, data?, force?}
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

export async function POST(request) {
  if (!MEM && (!URL_ || !TOKEN)) return json(503, { error: '雲端資料庫尚未設定' });
  const raw = await request.text();
  if (raw.length > MAX_BYTES) return json(413, { error: '進度資料太大' });
  let b; try { b = JSON.parse(raw); } catch { return json(400, { error: '格式錯誤' }); }
  const name = cleanName(b && b.name);
  if (!name) return json(400, { error: '帳號名稱需為 2–20 個字' });
  const key = keyOf(name), now = Date.now();
  const okData = d => d && typeof d === 'object' && !Array.isArray(d);

  try {
    if (b.op === 'register') {
      const data = okData(b.data) ? b.data : null;
      const rec = { name, c: now, t: (data && data.t) || 0, data };
      const r = await redis(['SET', key, JSON.stringify(rec), 'NX', 'EX', String(TTL)]);
      if (r === null) return json(409, { error: '這個名稱已經有人使用' });
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
      rec.data = b.data; rec.t = b.data.t || now;
      await redis(['SET', key, JSON.stringify(rec), 'XX', 'EX', String(TTL)]);
      return json(200, { name: rec.name, t: rec.t });
    }
    if (b.op === 'delete') {
      await redis(['DEL', key]);
      return json(200, { ok: true });
    }
    return json(400, { error: '未知的操作' });
  } catch (e) {
    return json(502, { error: '雲端資料庫暫時無法連線' });
  }
}
