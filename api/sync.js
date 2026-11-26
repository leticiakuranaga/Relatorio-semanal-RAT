// Sincroniza RATx -> Upstash Redis. Incremental e tolerante a 429 (rate limit).
import { Redis } from '@upstash/redis';
import { ok } from './_auth.js';
const E = process.env, B = E.RATX_URL || 'http://pdvx.com.br/api';
const redis = new Redis({ url: E.UPSTASH_REDIS_REST_URL || E.KV_REST_API_URL, token: E.UPSTASH_REDIS_REST_TOKEN || E.KV_REST_API_TOKEN });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let token;

async function call(path, method = 'GET', body) {
  for (let i = 0; i < 6; i++) {
    const r = await fetch(B + path, {
      method, body: body && JSON.stringify(body),
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...(token && { Authorization: 'Bearer ' + token }) },
    });
    if (r.status === 429) { await sleep(2000 * (i + 1)); continue; }
    if (!r.ok) throw new Error(`${r.status} ${path}`);
    return r.json();
  }
  throw new Error('429 persistente ' + path);
}

async function login() {
  const cred = { email: E.RATX_EMAIL, password: E.RATX_PASSWORD, forceLogin: true };
  let r = await call('/login', 'POST', cred);
  if (r.empresas) r = await call('/logar', 'POST', { ...cred, companyId: E.RATX_COMPANY_ID, companyNome: E.RATX_COMPANY_NOME, companyCNPJ: E.RATX_COMPANY_CNPJ });
  if (!r.access_token) throw new Error('Login falhou: ' + (typeof r === 'string' ? r : JSON.stringify(r).slice(0, 120)));
  token = r.access_token;
}

const iso = (d) => new Date(d.getTime() - 3 * 36e5).toISOString().slice(0, 10); // BRT
const clean = (s = '', n = 800) => String(s).replace(/!\[[^\]]*\]\([^)]*\)/g, ' ').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/[#*`]/g, '').replace(/\s+/g, ' ').trim().slice(0, n);

async function detail(id) {
  const d = await call(`/ratx/${id}/details`), t = d.ratx ?? d.data ?? d;
  const it = t.interation ?? d.interation ?? [], total = t.total_inter ?? d.total_inter ?? it.length;
  let all = it;
  if (total > it.length) { // só busca o miolo quando há mais de 6 interações
    const x = await call(`/ratx/${id}/interacao?limit=9999&step=0`);
    all = [it[0], ...(x.data ?? x), ...it.slice(1)].filter(Boolean);
  }
  const last = all[all.length - 1] || {}, first = all.find((i) => i.int_type === 'P') || all[0] || {};
  const tags = [...new Set(all.flatMap((i) => [...String(i.int_interation || '').matchAll(/TAGs:\s*([^\n]+)/g)].flatMap((m) => m[1].split(/[,;]/).map((s) => s.trim()).filter(Boolean))))];
  return { problema: clean(first.int_interation), ultimo: clean(last.int_interation, 300), ultimoEm: last.created_at, autor: last.user?.pessoa?.pes_razao, tags, nInter: total, detalhadoEm: Date.now() };
}

export default async function handler(req, res) {
  if (!(ok(req) || (E.CRON_SECRET && req.headers.authorization === `Bearer ${E.CRON_SECRET}`))) return res.status(401).json({ error: 'unauthorized' });
  const t0 = Date.now(); let logged = false;
  try {
    await login(); logged = true;
    const now = new Date(), mon = new Date(now); mon.setDate(now.getDate() - ((now.getDay() + 6) % 7) - 7);
    const start = new Date(Math.min(new Date(now.getFullYear(), now.getMonth(), 1), mon));
    const closed = new Set(((await call('/ratx/modulos')).filter?.data || []).filter((m) => m.mod_type === 'T' && m.mod_status_close == 1).map((m) => m.id));
    const q = `f[0][column]=created_at&f[0][operator]=in_the_peroid_2&f[0][query_1]=${iso(start)}&f[0][query_2]=${iso(now)}&limit=9999`;
    const list = await call('/ratx/tickets?' + q);
    const old = (await redis.hgetall('ratx:tickets')) || {};
    const tickets = list.map((t) => ({ // a listagem já traz status/operador/prioridade/SLA
      ...(old[t.id] || {}), id: t.id, protocolo: t.protocolo, aberto: t.created_at, concluido: t.finished_at, cliente: t.empresa?.emp_nome,
      titulo: t.ratx_title, operador: t.user?.pessoa?.pes_razao, status: t.status?.mod_title, prioridade: t.ratx_priority?.[0]?.mod_title,
      slaOk: t.ratx_sla?.status, fechado: closed.has(t.ratx_status) || !!t.finished_at,
    }));
    // fila: nunca detalhados > abertos com detalhe antigo (>6h). Fechados já detalhados não são refeitos.
    const fila = tickets.filter((t) => !t.detalhadoEm || (!t.fechado && Date.now() - t.detalhadoEm > 6 * 36e5))
      .sort((a, b) => (a.detalhadoEm || 0) - (b.detalhadoEm || 0));
    let i = 0, erros = 0;
    const worker = async () => {
      while (i < fila.length && Date.now() - t0 < 240000) {
        const t = fila[i++];
        try { Object.assign(t, await detail(t.id)); } catch { erros++; }
        await sleep(300);
      }
    };
    await Promise.all([worker(), worker()]);
    for (let k = 0; k < tickets.length; k += 100)
      await redis.hset('ratx:tickets', Object.fromEntries(tickets.slice(k, k + 100).map((t) => [t.id, t])));
    const meta = { syncedAt: Date.now(), total: tickets.length, semHistorico: tickets.filter((t) => !t.detalhadoEm).length, erros };
    await redis.set('ratx:meta', meta);
    res.json(meta);
  } catch (e) {
    res.status(500).json({ error: e.message });
  } finally {
    if (logged) await call('/logout').catch(() => {});
  }
}
