'use strict';

const crypto = require('node:crypto');

// 来源报文签名：HMAC-SHA256，头部形如 sha256=<hex>（裸 hex 也接受）。
function computeSignature(secret, rawBody) {
  return crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
}

function verifySignature(secret, rawBody, signatureHeader) {
  if (!signatureHeader) return false;
  const provided = signatureHeader.startsWith('sha256=')
    ? signatureHeader.slice('sha256='.length)
    : signatureHeader;
  const expected = computeSignature(secret, rawBody);
  const providedBuf = Buffer.from(String(provided).trim(), 'hex'),
    expectedBuf = Buffer.from(expected, 'hex');
  if (providedBuf.length !== expectedBuf.length) return false;
  return crypto.timingSafeEqual(providedBuf, expectedBuf);
}

module.exports = { computeSignature, verifySignature };
