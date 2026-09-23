const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('通知：业务事件产生一条 outbox 通知，flush 后投递一次，再次 flush 不重复', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });

  // 投递前：pending
  const pending = h.service.listNotifications('pending');
  assert.equal(pending.length, 1);
  assert.equal(pending[0].kind, 'project_created');

  const first = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(first.body.delivered.length, 1);
  assert.equal(h.delivered.length, 1);

  // 任何重放：再 flush 不会重复投递已交付通知
  const second = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(second.body.examined, 0);
  assert.equal(h.delivered.length, 1);

  // 重放同一来源消息（重复投递）也不产生新通知
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const third = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(third.body.examined, 0);
  assert.equal(h.delivered.length, 1);
  await h.close();
});

test('通知：下游暂时失败时保留 pending，恢复后仅投递一次', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });

  // 第一次投递失败
  h.failNextNotifications(1);
  const failed = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(failed.body.delivered.length, 0);
  assert.equal(failed.body.failed.length, 1);
  assert.equal(h.delivered.length, 0);
  // 仍是 pending，记录了失败原因与尝试次数
  let row = h.service.listNotifications('pending')[0];
  assert.equal(row.attempts, 1);
  assert.ok(row.last_error);

  // 下游恢复后再次 flush：投递成功，且只投递一次
  const ok = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(ok.body.delivered.length, 1);
  assert.equal(h.delivered.length, 1);

  // 再 flush 无重复
  await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(h.delivered.length, 1);
  row = h.service.listNotifications('delivered')[0];
  assert.equal(row.attempts, 2);
  assert.ok(row.delivered_at);
  await h.close();
});

test('通知幂等键：同一版本同类通知即使重复生成也只落一条', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const pid = created.body.result.projectId;
  const versionId = h.db.prepare('SELECT version_id FROM project_versions WHERE project_id=?').get(pid).version_id;

  // 手工重复触发同一幂等键（模拟异常重放），INSERT OR IGNORE 保证只有一条
  h.db.prepare(
    `INSERT OR IGNORE INTO notifications(notification_id, idempotency_key, kind, project_id, version_id, payload, created_at)
     VALUES('ntf_dup', ?, 'project_created', ?, ?, '{}', ?)`,
  ).run('project_created:' + versionId, pid, versionId, new Date().toISOString());

  const rows = h.db.prepare('SELECT * FROM notifications WHERE idempotency_key=?').all('project_created:' + versionId);
  assert.equal(rows.length, 1);
  assert.notEqual(rows[0].notification_id, 'ntf_dup'); // 保留的是首次那条
  await h.close();
});

test('失败消息恢复成功后只发一次通知（恢复不产生重复通知）', async () => {
  const h = await createHarness();
  // 修订先到失败（处理在发通知前抛错，故无通知）
  const rev = await h.ingest('a', 'rev-1', { op: 'revise', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  assert.equal(rev.status, 503);
  assert.equal(h.service.listNotifications().length, 0);

  // 创建到达（产生 project_created），随后恢复修订（产生 project_revised）
  await h.ingest('a', 'crt-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 90 } });
  const fid = h.service.listFailedMessages()[0].id;
  await h.service.recoverMessage(fid);

  const flush = await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(flush.body.delivered.length, 2); // created + revised，各一次
  await h.request('POST', '/notifications/flush', { body: {} });
  assert.equal(h.delivered.length, 2);
  await h.close();
});

test('撤销通知在乱序场景下也只投递一次', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-1' });
  // 被撤销补发晚到（不再产生撤销通知）
  await h.ingest('a', 'm-late', { op: 'upsert', ref: 'A-1', version: 1, eventType: 'match', facts: { amount: 100 } });

  await h.request('POST', '/notifications/flush', { body: {} });
  const kinds = h.delivered.map((n) => n.kind);
  assert.deepEqual(kinds.sort(), ['project_created', 'project_revoked']);
  assert.equal(kinds.filter((k) => k === 'project_revoked').length, 1);
  await h.close();
});
