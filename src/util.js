const crypto = require('node:crypto');

function hmac(secret, message) {
  return crypto.createHmac('sha256', secret).update(message).digest('hex');
}

// 恒定时间比较，避免签名校验的时序侧信道
function timingSafeEqualHex(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const ba = Buffer.from(a, 'hex');
  const bb = Buffer.from(b, 'hex');
  if (ba.length !== bb.length || ba.length === 0) return false;
  return crypto.timingSafeEqual(ba, bb);
}

function newId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function nowIso() {
  return new Date().toISOString();
}

function parseJson(text) {
  return JSON.parse(text);
}

module.exports = { hmac, timingSafeEqualHex, newId, nowIso, parseJson };
