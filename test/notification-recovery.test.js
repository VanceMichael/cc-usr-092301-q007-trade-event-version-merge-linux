'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, send } = require('./helpers');

test('通知发件箱：版本生效即入队，投递成功后重放不重复触发', async () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'n1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-N', payload: { category: 'match', amount: 10 },
  });
  const pending = h.service.listOutbox('pending');
  assert.ok(pending.length >= 1, '生效版本产生待发通知');

  let calls = 0;
  const r1 = await h.service.drainNotifications(async () => { calls++; });
  assert.equal(r1.delivered, pending.length);
  assert.equal(calls, pending.length);

  // 再次 drain：已交付的不再触发投递器
  const r2 = await h.service.drainNotifications(async () => { calls++; });
  assert.equal(r2.delivered, 0);
  assert.equal(calls, pending.length);
  assert.equal(h.service.listDeliveries().length, pending.length);
  h.close();
});

test('通知投递失败后重试，同一条通知恰好投递一次', async () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'n2', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-N2', payload: { category: 'match' },
  });
  let calls = 0;
  // 首次失败
  const r1 = await h.service.drainNotifications(async () => {
    calls++;
    throw new Error('downstream timeout');
  }, { maxAttempts: 3 });
  assert.equal(r1.failed.length, calls === 1 ? 1 : r1.failed.length);
  assert.equal(calls, 1);

  // 退避期间不可见
  let immediate = await h.service.drainNotifications(async () => { calls++; });
  assert.equal(immediate.scanned, 0, '退避窗口内不重复投递');

  // 重新入队后投递成功
  h.service.requeueStaleNotifications();
  const r2 = await h.service.drainNotifications(async () => { calls++; });
  assert.equal(r2.delivered, 1);
  assert.equal(calls, 2, '失败重试用尽后只多投递一次');

  // 再重放仍然不重复
  await h.service.drainNotifications(async () => { calls++; });
  assert.equal(calls, 2);
  h.close();
});

test('乱序撤销补做生效也只通知一次最终状态', async () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'cearly', action: 'cancel', business_time: '2026-09-03T00:00:00Z',
    target_ref: 'A-Q',
  });
  // 待补做期间没有 cancel 通知
  assert.equal(h.service.listOutbox().filter((o) => o.notification_type === 'version_cancel').length, 0);
  send(h.service, 'A', {
    event_id: 'flate', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-Q', payload: { category: 'match', amount: 5 },
  });
  const cancels = h.service.listOutbox().filter((o) => o.notification_type === 'version_cancel');
  assert.equal(cancels.length, 1, '补做只产生一条 cancel 通知');
  h.close();
});

test('失败消息恢复：坏签名报文留存，机构按同一 message_id 正确补发后入库', () => {
  const h = createHarness();
  const event = {
    event_id: 'recover-1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-RC', payload: { category: 'match', amount: 3 },
  };
  assert.throws(() => send(h.service, 'A', event, { signature: 'sha256=00' }),
    (err) => err.status === 401);
  assert.equal(h.service.listFailedMessages().length, 1);

  // 正确签名、同一 message_id 补发
  const ok = send(h.service, 'A', event, { messageId: 'msg-recover-1' });
  assert.equal(ok.outcome, 'accepted');
  assert.equal(h.service.listFailedMessages().length, 0);
  assert.equal(h.service.stats().categories[0].total_amount, 3);
  h.close();
});

test('recoverFailed：直接修复留存报文后批量恢复', () => {
  const h = createHarness();
  // 模拟一条签名有效但处理时出错（此处构造为处理前残留）的留存报文
  const raw = JSON.stringify({
    event_id: 'rc2', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-RC2', payload: { category: 'park', amount: 42 },
  });
  h.db.prepare(
    `INSERT INTO source_messages(institution_code, message_id, event_type, raw_payload, signature, signature_valid, received_at, processed)
     VALUES('A','msg-rc2','fact',?, 'sig', 1, ?, 0)`,
  ).run(raw, '2026-09-09T00:00:00Z');
  assert.equal(h.service.listFailedMessages().length, 1);
  const result = h.service.recoverFailed();
  assert.equal(result.recovered.length, 1);
  assert.equal(result.failed.length, 0);
  assert.equal(h.service.stats().categories[0].total_amount, 42);
  h.close();
});
