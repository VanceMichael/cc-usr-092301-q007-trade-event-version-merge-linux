'use strict';

const { openDatabase } = require('../src/database');
const { createEventService } = require('../src/event-service');
const { createApp } = require('../src/app');
const { computeSignature } = require('../src/sig');

// 可控时钟：测试用来决定 recorded_at / known_at
function mockClock(start = '2026-09-10T00:00:00.000Z') {
  let current = new Date(start);
  return {
    now: () => current,
    advance(ms) { current = new Date(current.getTime() + ms); return current; },
    set(iso) { current = new Date(iso); return current; },
    iso: () => current.toISOString(),
  };
}

function createHarness({ path = ':memory:', reviewThreshold } = {}) {
  const clock = mockClock();
  const db = openDatabase(path);
  const service = createEventService(db, { now: clock.now, reviewThreshold });
  service.registerInstitution({ code: 'A', name: '机构甲', secret: 'secret-A' });
  service.registerInstitution({ code: 'B', name: '机构乙', secret: 'secret-B' });
  service.registerInstitution({ code: 'C', name: '机构丙', secret: 'secret-C' });
  return {
    db, service, clock,
    secrets: { A: 'secret-A', B: 'secret-B', C: 'secret-C' },
    close: () => db.close(),
  };
}

const SECRETS = { A: 'secret-A', B: 'secret-B', C: 'secret-C' };

function send(service, institution, event, { messageId, secret = SECRETS[institution], signature } = {}) {
  const raw = JSON.stringify(event);
  const mid = messageId ?? `msg-${event.event_id}`;
  const sig = signature ?? `sha256=${computeSignature(secret, raw)}`;
  return service.ingest(institution, raw, sig, mid);
}

function startHttp(service, options = {}) {
  const app = createApp(service, options);
  return new Promise((resolve) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        server, port,
        close: () => new Promise((res) => server.close(res)),
      });
    });
  });
}

function httpRequest(port, method, path, { body, rawBody, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = rawBody ?? (body !== undefined ? JSON.stringify(body) : undefined);
    const req = require('node:http').request({
      host: '127.0.0.1', port, path, method,
      headers: {
        ...(data !== undefined ? {
          'content-length': Buffer.byteLength(data),
          'content-type': 'application/json',
        } : {}),
        ...headers,
      },
    }, (response) => {
      let text = '';
      response.on('data', (c) => { text += c; });
      response.on('end', () => resolve({
        status: response.statusCode,
        text,
        json: text ? JSON.parse(text) : null,
      }));
    });
    req.on('error', reject);
    if (data !== undefined) req.write(data);
    req.end();
  });
}

function signedRequest(port, institution, secret, event, messageId = `msg-${event.event_id}`) {
  const raw = JSON.stringify(event);
  return httpRequest(port, 'POST', `/v1/events/${institution}`, {
    rawBody: raw,
    headers: {
      'content-type': 'application/json',
      'x-message-id': messageId,
      'x-signature': `sha256=${computeSignature(secret, raw)}`,
    },
  });
}

module.exports = { createHarness, mockClock, send, startHttp, httpRequest, signedRequest, SECRETS };
