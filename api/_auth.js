import { timingSafeEqual } from 'crypto';
export const ok = (req) => {
  const a = Buffer.from(String(req.headers['x-dash-key'] || '')), b = Buffer.from(process.env.DASH_PASSWORD || '\0');
  return a.length === b.length && timingSafeEqual(a, b);
};
