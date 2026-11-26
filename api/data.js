import { Redis } from '@upstash/redis';
import { ok } from './_auth.js';
const redis = new Redis({ url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN });
export default async function handler(req, res) {
  if (!ok(req)) return res.status(401).json({ error: 'unauthorized' });
  const [h, meta] = await Promise.all([redis.hgetall('ratx:tickets'), redis.get('ratx:meta')]);
  res.setHeader('Cache-Control', 'no-store');
  res.json({ meta: meta || {}, tickets: Object.values(h || {}) });
}
