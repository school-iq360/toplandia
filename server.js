'use strict';
// Рейтинг России: топ-100, валюта, платные топы, реальное время (SSE).
// Запуск: node server.js  (Node 18+, без зависимостей)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'data.json');
const UNLOCK_MS = 24 * 3600e3; // открытый топ доступен сутки

// Цены топов: day бесплатный, остальные за валюту
const PRICES = {
  day:   { cur: 'coins',    amount: 0 },
  week:  { cur: 'coins',    amount: 50 },
  month: { cur: 'coins',    amount: 150 },
  year:  { cur: 'coins',    amount: 400 },
  olymp: { cur: 'diamonds', amount: 5 },
};
const REWARD = {
  easy:  { points: 10, olymp: 0,  coins: 1 },
  olymp: { points: 0,  olymp: 30, coins: 3 },
};
const STREAK_FOR_DIAMOND = 10;

// ---------- хранилище (JSON-файл; для продакшена замени на PostgreSQL) ----------
let db = { users: {}, events: [] };
let byToken = new Map();

// Хранилище: PostgreSQL (если задан DATABASE_URL), иначе обычный файл.
// На бесплатном Render файлы стираются при перезапуске, поэтому для настоящих игроков нужна база.
function fileStore() {
  return {
    name: 'файл ' + DB_FILE,
    async read() {
      try { return fs.readFileSync(DB_FILE, 'utf8'); }
      catch (e) { if (e.code === 'ENOENT') return null; throw e; }
    },
    async write(text) {
      await fs.promises.writeFile(DB_FILE + '.tmp', text);
      await fs.promises.rename(DB_FILE + '.tmp', DB_FILE);
    },
  };
}
function pgStore() {
  const { Pool } = require('pg');
  const url = process.env.DATABASE_URL;
  const pool = new Pool({
    connectionString: url, max: 3, connectionTimeoutMillis: 20000,
    ssl: /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: false },
  });
  pool.on('error', e => console.error('Ошибка соединения с базой:', e.message));
  return {
    name: 'PostgreSQL',
    async read() {
      await pool.query('CREATE TABLE IF NOT EXISTS kv (k text PRIMARY KEY, v text NOT NULL, updated timestamptz NOT NULL DEFAULT now())');
      const r = await pool.query("SELECT v FROM kv WHERE k = 'main'");
      return r.rows.length ? r.rows[0].v : null;
    },
    async write(text) {
      await pool.query("INSERT INTO kv (k, v) VALUES ('main', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated = now()", [text]);
    },
  };
}
const store = process.env.DATABASE_URL ? pgStore() : fileStore();

async function loadDb() {
  const raw = await store.read(); // при ошибке сервер не запустится, чтобы не затереть данные пустой базой
  if (raw) db = JSON.parse(raw);
  byToken = new Map(Object.values(db.users).map(u => [u.token, u]));
  // миграция: добавляем новые поля старым игрокам и считаем их общие очки по истории
  if (Object.values(db.users).some(u => u.pts === undefined)) {
    for (const u of Object.values(db.users)) Object.assign(u, { pts: 0, olymp: 0, seconds: 0, av: 0, photo: null });
    for (const e of db.events) { const u = db.users[e.u]; if (u) { u.pts += e.p; u.olymp += e.o; } }
  }
  console.log(`Данные загружены (${store.name}): игроков ${Object.keys(db.users).length}, событий ${db.events.length}`);
}

// Автосохранение: раз в минуту, и только если что-то изменилось (так база может "спать"), плюс при остановке сервера
let dirty = false, flushing = false;
function save() { dirty = true; }
async function flush() {
  if (!dirty || flushing) return;
  flushing = true; dirty = false;
  try { await store.write(JSON.stringify(db)); }
  catch (e) { dirty = true; console.error('Ошибка сохранения:', e.message); }
  flushing = false;
}
setInterval(flush, 60e3);
async function shutdown() {
  const t0 = Date.now();
  while (flushing && Date.now() - t0 < 8000) await new Promise(r => setTimeout(r, 100));
  await flush();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// ---------- периоды (по московскому времени, UTC+3) ----------
const MSK = 3 * 3600e3;
function periodStart(p) {
  const d = new Date(Date.now() + MSK);
  const y = d.getUTCFullYear(), m = d.getUTCMonth(), day = d.getUTCDate();
  switch (p) {
    case 'day':   return Date.UTC(y, m, day) - MSK;
    case 'week':  return Date.UTC(y, m, day - ((d.getUTCDay() + 6) % 7)) - MSK;
    case 'month': return Date.UTC(y, m, 1) - MSK;
    case 'year':  return Date.UTC(y, 0, 1) - MSK;
    case 'olymp': return 0;
  }
}

// ---------- расчёт топа (кэш на 1 секунду) ----------
const topCache = {};
function computeTop(p) {
  const c = topCache[p];
  if (c && Date.now() - c.at < 1000) return c.list;
  const from = periodStart(p), sums = new Map();
  for (let i = db.events.length - 1; i >= 0; i--) {
    const e = db.events[i];
    if (e.t < from) break; // события хранятся по времени
    const v = p === 'olymp' ? e.o : e.p;
    if (v) sums.set(e.u, (sums.get(e.u) || 0) + v);
  }
  const list = [...sums].sort((a, b) => b[1] - a[1]).map(([id, score], i) => ({
    rank: i + 1, id, nick: db.users[id].nick, region: db.users[id].region, av: db.users[id].av || 0, score,
  }));
  topCache[p] = { at: Date.now(), list };
  return list;
}
function topView(p, user) {
  const all = computeTop(p);
  return { period: p, top: all.slice(0, 100), me: all.find(r => r.id === user.id) || null, total: all.length };
}
const hasAccess = (u, p) => PRICES[p] && (PRICES[p].amount === 0 || (u.unlocks[p] || 0) > Date.now());

// ---------- задания (ответ проверяет сервер) ----------
const tasks = new Map(); // taskId -> {answer, kind, user, exp}
function rnd(a, b) { return a + Math.floor(Math.random() * (b - a + 1)); }
function newTask(kind, user) {
  let q, answer;
  if (kind === 'olymp') { const a = rnd(12, 40), b = rnd(12, 40), c = rnd(10, 99); q = `${a} × ${b} + ${c}`; answer = a * b + c; }
  else { const a = rnd(10, 99), b = rnd(10, 99); q = `${a} + ${b}`; answer = a + b; }
  const id = crypto.randomBytes(8).toString('hex');
  tasks.set(id, { answer, kind, user: user.id, exp: Date.now() + 60e3 });
  return { id, question: q, kind };
}
setInterval(() => { const n = Date.now(); for (const [k, t] of tasks) if (t.exp < n) tasks.delete(k); }, 30e3);

// ---------- утилиты HTTP ----------
function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(obj));
}
function readBody(req, max = 10e3) {
  return new Promise((resolve, reject) => {
    let s = '';
    req.on('data', c => { s += c; if (s.length > max) { reject(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { resolve(s ? JSON.parse(s) : {}); } catch (e) { reject(e); } });
  });
}
const lastHit = new Map();
function limited(u, ms) { const n = Date.now(), l = lastHit.get(u.id) || 0; lastHit.set(u.id, n); return n - l < ms; }
const me = u => ({ id: u.id, nick: u.nick, region: u.region, coins: u.coins, diamonds: u.diamonds, streak: u.streak, unlocks: u.unlocks, av: u.av || 0 });

// ---------- SSE ----------
const clients = new Set(); // {res, user, period}
let changed = false;
setInterval(() => {
  if (!changed) return;
  changed = false;
  for (const c of clients) {
    if (hasAccess(c.user, c.period)) c.res.write(`data: ${JSON.stringify(topView(c.period, c.user))}\n\n`);
  }
}, 1000);
setInterval(() => clients.forEach(c => c.res.write(': ping\n\n')), 25e3);

// ---------- роутер ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'content-type,x-token', 'Access-Control-Allow-Methods': 'GET,POST' });
    return res.end();
  }
  try {
    const readOnly = req.method === 'GET' || req.method === 'HEAD'; // HEAD шлют сервисы мониторинга
    if (readOnly && p === '/health') {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      return res.end('ok');
    }
    if (readOnly && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
    }
    if (!p.startsWith('/api/')) return send(res, 404, { error: 'not_found' });

    // регистрация
    if (p === '/api/register' && req.method === 'POST') {
      const b = await readBody(req);
      const nick = String(b.nick || '').trim().slice(0, 20);
      const region = String(b.region || '').trim().slice(0, 40);
      if (nick.length < 2) return send(res, 400, { error: 'nick_too_short' });
      if (Object.values(db.users).some(u => u.nick.toLowerCase() === nick.toLowerCase())) return send(res, 409, { error: 'nick_taken' });
      const u = { id: crypto.randomBytes(6).toString('hex'), token: crypto.randomBytes(24).toString('hex'), nick, region, country: 'RU',
        coins: 0, diamonds: 0, streak: 0, unlocks: {}, created: Date.now(),
        pts: 0, olymp: 0, seconds: 0, av: 0, photo: null };
      db.users[u.id] = u; byToken.set(u.token, u); save();
      return send(res, 201, { token: u.token, user: me(u) });
    }

    // публичные аватарки (для <img>, без токена)
    const ma = req.method === 'GET' && p.match(/^\/api\/avatar\/(\w+)$/);
    if (ma) {
      const au = db.users[ma[1]];
      if (!au || !au.photo) return send(res, 404, { error: 'no_avatar' });
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=31536000, immutable' });
      return res.end(Buffer.from(au.photo.split(',')[1], 'base64'));
    }

    // всё остальное требует токен
    const token = req.headers['x-token'] || url.searchParams.get('token');
    const user = byToken.get(token);
    if (!user) return send(res, 401, { error: 'unauthorized' });

    if (p === '/api/me') return send(res, 200, { user: me(user), prices: PRICES });

    // время на сайте: клиент шлёт ping раз в 30 секунд, пока вкладка открыта
    if (p === '/api/ping' && req.method === 'POST') {
      const n = Date.now(), d = n - (user.lastPing || 0);
      user.lastPing = n;
      if (d >= 20e3) { user.seconds = (user.seconds || 0) + (d > 90e3 ? 30 : Math.round(d / 1000)); save(); }
      return send(res, 200, { ok: true });
    }

    // фото профиля (клиент присылает уменьшенный JPEG 96x96 как data URL)
    if (p === '/api/avatar' && req.method === 'POST') {
      const n = Date.now();
      if (n - (user.lastAv || 0) < 3000) return send(res, 429, { error: 'too_fast' });
      user.lastAv = n;
      const b = await readBody(req, 40e3);
      if (b.remove) { user.photo = null; user.av = 0; }
      else {
        const img = String(b.image || '');
        if (img.length > 30000 || !/^data:image\/jpeg;base64,[A-Za-z0-9+\/=]+$/.test(img)) return send(res, 400, { error: 'bad_image' });
        user.photo = img; user.av = n;
      }
      for (const k in topCache) delete topCache[k];
      changed = true; save();
      return send(res, 200, { user: me(user) });
    }

    if (p === '/api/task' && req.method === 'GET') {
      const kind = url.searchParams.get('kind') === 'olymp' ? 'olymp' : 'easy';
      return send(res, 200, newTask(kind, user));
    }

    if (p === '/api/answer' && req.method === 'POST') {
      if (limited(user, 400)) return send(res, 429, { error: 'too_fast' });
      const b = await readBody(req);
      const t = tasks.get(b.id);
      if (!t || t.user !== user.id) return send(res, 404, { error: 'task_not_found' });
      tasks.delete(b.id);
      const ok = Number(b.answer) === t.answer;
      let gained = null;
      if (ok) {
        const r = REWARD[t.kind];
        user.coins += r.coins; user.streak += 1; user.pts += r.points; user.olymp += r.olymp;
        let diamond = 0;
        if (user.streak % STREAK_FOR_DIAMOND === 0) { user.diamonds += 1; diamond = 1; }
        db.events.push({ u: user.id, p: r.points, o: r.olymp, t: Date.now() });
        gained = { points: r.points, olymp: r.olymp, coins: r.coins, diamond };
        changed = true;
      } else user.streak = 0;
      save();
      return send(res, 200, { correct: ok, correctAnswer: ok ? undefined : t.answer, gained, user: me(user) });
    }

    if (p === '/api/unlock' && req.method === 'POST') {
      const b = await readBody(req);
      const pr = PRICES[b.period];
      if (!pr || pr.amount === 0) return send(res, 400, { error: 'bad_period' });
      if ((user.unlocks[b.period] || 0) > Date.now()) return send(res, 409, { error: 'already_unlocked' });
      if (user[pr.cur] < pr.amount) return send(res, 402, { error: 'not_enough', need: pr });
      user[pr.cur] -= pr.amount;
      user.unlocks[b.period] = Date.now() + UNLOCK_MS;
      save();
      return send(res, 200, { user: me(user) });
    }

    let m;
    if ((m = p.match(/^\/api\/profile\/(\w+)$/))) {
      const u = db.users[m[1]];
      if (!u || !u.nick) return send(res, 404, { error: 'not_found' });
      return send(res, 200, { id: u.id, nick: u.nick, region: u.region, av: u.av || 0,
        pts: u.pts || 0, olymp: u.olymp || 0, seconds: u.seconds || 0, created: u.created });
    }
    if ((m = p.match(/^\/api\/top\/(\w+)$/))) {
      if (!PRICES[m[1]]) return send(res, 400, { error: 'bad_period' });
      if (!hasAccess(user, m[1])) return send(res, 403, { error: 'locked', price: PRICES[m[1]] });
      return send(res, 200, topView(m[1], user));
    }

    if ((m = p.match(/^\/api\/stream\/(\w+)$/))) {
      if (!PRICES[m[1]]) return send(res, 400, { error: 'bad_period' });
      if (!hasAccess(user, m[1])) return send(res, 403, { error: 'locked', price: PRICES[m[1]] });
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' });
      const c = { res, user, period: m[1] };
      clients.add(c);
      res.write(`data: ${JSON.stringify(topView(m[1], user))}\n\n`);
      req.on('close', () => clients.delete(c));
      return;
    }

    send(res, 404, { error: 'not_found' });
  } catch (e) {
    send(res, 400, { error: 'bad_request' });
  }
});

loadDb()
  .then(() => server.listen(PORT, () => console.log(`Сервер запущен: http://localhost:${PORT}`)))
  .catch(e => { console.error('Не удалось загрузить данные:', e.message); process.exit(1); });
