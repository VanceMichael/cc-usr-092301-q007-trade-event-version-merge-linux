'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, startHttp, httpRequest, signedRequest } = require('./helpers');

async function withServer(t, options) {
  const h = createHarness(options);
  const http = await startHttp(h.service, options?.app);
  t.after(async () => { await http.close(); h.close(); });
  return { ...h, http };
}

test('HTTP: 健康检查', async (t) => {
  const { http } = await withServer(t);
  const res = await httpRequest(http.port, 'GET', '/health');
  assert.equal(res.status, 200);
  assert.deepEqual(res.json, { status: 'ok' });
});

test('HTTP: 签名事件接收 202 与全链路', async (t) => {
  const { http } = await withServer(t);
  const res = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'h1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-HTTP', payload: { category: 'match', amount: 11 },
  });
  assert.equal(res.status, 202);
  assert.equal(res.json.outcome, 'accepted');

  const stats = await httpRequest(http.port, 'GET', '/v1/stats');
  assert.equal(stats.json.categories[0].total_amount, 11);
});

test('HTTP: 缺签名/坏签名返回 401，报文仍留存', async (t) => {
  const { http, service } = await withServer(t);
  const event = { event_id: 'h2', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-X' };
  const bad = await httpRequest(http.port, 'POST', '/v1/events/A', {
    body: event,
    headers: { 'content-type': 'application/json', 'x-message-id': 'msg-h2', 'x-signature': 'sha256=ff' },
  });
  assert.equal(bad.status, 401);
  assert.equal(bad.json.error.code, 'ERR_BAD_SIGNATURE');
  assert.equal(service.listFailedMessages().length, 1);

  // 缺 X-Message-Id
  const noId = await httpRequest(http.port, 'POST', '/v1/events/A', { body: event });
  assert.equal(noId.status, 400);
});

test('HTTP: 乱序撤销补发经接口自动补做', async (t) => {
  const { http } = await withServer(t);
  await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'hc', action: 'cancel', business_time: '2026-09-03T00:00:00Z', target_ref: 'A-OO',
  });
  const fact = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'hf', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-OO', payload: { category: 'match', amount: 5 },
  });
  const tl = await httpRequest(http.port, 'GET', `/v1/projects/${fact.json.projectId}/timeline`);
  assert.equal(tl.json.versions.find((v) => v.kind === 'fact').state, 'cancelled');
});

test('HTTP: 审核接口裁决，并发/重复裁决 409', async (t) => {
  const { http } = await withServer(t);
  const created = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'hr', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-REV', confidence: 0.2, payload: { category: 'match', amount: 10 },
  });
  const reviewId = created.json.reviewId;
  const d1 = await httpRequest(http.port, 'POST', `/v1/reviews/${reviewId}/decide`, {
    body: { decision: 'approved', reviewer: 'ops1' },
  });
  assert.equal(d1.status, 200);
  const d2 = await httpRequest(http.port, 'POST', `/v1/reviews/${reviewId}/decide`, {
    body: { decision: 'rejected', reviewer: 'ops2' },
  });
  assert.equal(d2.status, 409);
  assert.equal(d2.json.error.code, 'ERR_REVIEW_NOT_OPEN');
});

test('HTTP: 跨机构合并双方确认，非当事方 403', async (t) => {
  const { http } = await withServer(t);
  await signedRequest(http.port, 'A', 'secret-A', { event_id: 'a', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-1', payload: { category: 'park' } });
  await signedRequest(http.port, 'B', 'secret-B', { event_id: 'b', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'B-1', payload: { category: 'park' } });
  const m = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'm', action: 'merge', business_time: '2026-09-05T00:00:00Z',
    surviving_ref: 'A-1', absorbed_ref: 'B-1', target_institution: 'B',
  });
  const mergeId = m.json.merge_id;
  const outsider = await httpRequest(http.port, 'POST', `/v1/merges/${mergeId}/confirm`, { body: { institution: 'C' } });
  assert.equal(outsider.status, 403);
  const ok = await httpRequest(http.port, 'POST', `/v1/merges/${mergeId}/confirm`, { body: { institution: 'B' } });
  assert.equal(ok.status, 200);
  assert.equal(ok.json.status, 'confirmed');
});

test('HTTP: 通知 drain 后重复 drain 不重复投递', async (t) => {
  let calls = 0;
  const { http } = await withServer(t, { app: { deliver: async () => { calls++; } } });
  await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'nd', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-ND', payload: { category: 'match' },
  });
  const d1 = await httpRequest(http.port, 'POST', '/v1/notifications/drain');
  assert.equal(d1.status, 200);
  const firstCalls = calls;
  assert.ok(firstCalls >= 1);
  const d2 = await httpRequest(http.port, 'POST', '/v1/notifications/drain');
  assert.equal(d2.json.delivered, 0);
  assert.equal(calls, firstCalls, '下游投递次数不随重放增长');
});

test('HTTP: 映射有效期维护与历史时点解析', async (t) => {
  const { http } = await withServer(t);
  const f1 = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'm1', action: 'fact', business_time: '2026-01-01T00:00:00Z',
    external_ref: 'A-OLD', payload: { category: 'park' },
  });
  const f2 = await signedRequest(http.port, 'A', 'secret-A', {
    event_id: 'm2', action: 'fact', business_time: '2026-06-01T00:00:00Z',
    external_ref: 'A-NEW', payload: { category: 'park' },
  });
  // 自 2026-07-01 起 A-OLD 改指新项目
  const relink = await httpRequest(http.port, 'PUT', '/v1/mappings', {
    body: {
      institution: 'A', external_ref: 'A-OLD', project_id: f2.json.projectId,
      valid_from: '2026-07-01T00:00:00Z',
    },
  });
  assert.equal(relink.status, 200);
  const mappings = await httpRequest(http.port, 'GET', '/v1/mappings?institution=A&external_ref=A-OLD');
  assert.equal(mappings.json.mappings.length, 2, '保留新旧两个有效期区间');
  // 新区间指向新项目，旧区间在 2026-07-01 关闭
  const intervals = mappings.json.mappings;
  const current = intervals.find((m) => m.valid_to === null);
  const historical = intervals.find((m) => m.valid_to !== null);
  assert.equal(current.project_id, f2.json.projectId);
  assert.equal(historical.project_id, f1.json.projectId);
  assert.equal(historical.valid_to, '2026-07-01T00:00:00Z');
});
