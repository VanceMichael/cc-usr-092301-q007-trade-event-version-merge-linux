const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test('时间线：完整版本链可追溯 create→revise→revoke', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const pid = created.body.result.projectId;
  await sleep(2);
  await h.ingest('a', 'm-2', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 120 } });
  await sleep(2);
  await h.ingest('a', 'm-3', { op: 'revoke', ref: 'A-1' });

  const tl = h.service.getTimeline(pid);
  const chain = tl.timeline.map((v) => v.changeType);
  assert.ok(chain.includes('create'));
  assert.ok(chain.includes('revise'));
  assert.ok(chain.includes('revoke'));
  // 每个版本都能追溯到来源消息
  for (const v of tl.timeline) assert.ok(v.sourceMessageId);
  // 当前无生效快照（已撤销）
  assert.equal(tl.snapshot, null);
  await h.close();
});

test('as-of 重建：只反映当时已知信息，后来的修订/撤销不出现在历史时刻', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const pid = created.body.result.projectId;

  // 时刻 T1：仅创建
  const t1 = h.service.getTimeline(pid).timeline[0].recordedAt;
  let view = h.service.getTimeline(pid, t1);
  assert.equal(view.timeline.length, 1);
  assert.equal(view.timeline[0].statusAt, 'effective');
  assert.equal(view.snapshot.facts.amount, 100);

  // 后来修订
  await sleep(2);
  await h.ingest('a', 'm-2', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 200 } });
  const afterRevise = h.service.getTimeline(pid).timeline;
  const t2 = afterRevise[afterRevise.length - 1].recordedAt;

  // 回到 T1：仍只看到金额 100（修订尚未发生），且 create 当时是 effective
  view = h.service.getTimeline(pid, t1);
  assert.equal(view.snapshot.facts.amount, 100);

  // T2 时刻看到金额 200
  view = h.service.getTimeline(pid, t2);
  assert.equal(view.snapshot.facts.amount, 200);

  // 后来撤销
  await sleep(2);
  await h.ingest('a', 'm-3', { op: 'revoke', ref: 'A-1' });
  // 当前无快照
  assert.equal(h.service.getTimeline(pid).snapshot, null);
  // 但回到 T2，项目当时仍生效、金额 200（撤销尚未发生）
  view = h.service.getTimeline(pid, t2);
  assert.equal(view.snapshot.facts.amount, 200);
  assert.equal(view.snapshot.statusAt, 'effective');
  await h.close();
});

test('乱序补发：晚到事件落 revoked，但在撤销前的历史时刻该项目仍按当时已知重建', async () => {
  const h = await createHarness();
  // 撤销先到
  await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-1', eventTime: '2026-09-21T00:00:00Z' });
  // 被撤销的补发晚到（无更早事件时间证明，按已撤销处理）
  await h.ingest('a', 'm-late', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  // 该补发版本当前状态为 revoked，时间线可追溯
  const projects = h.db.prepare('SELECT project_id FROM project_versions').all().map((r) => r.project_id);
  const pid = projects[0];
  const tl = h.service.getTimeline(pid);
  assert.equal(tl.timeline[tl.timeline.length - 1].statusAt, 'revoked');
  await h.close();
});

test('统计：只计已生效版本，pending/revoked/superseded/rejected 排除', async () => {
  const h = await createHarness();
  // 1 个正常生效
  await h.ingest('a', 'a-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  // 1 个待审（pending）
  await h.ingest('a', 'a-2', { op: 'upsert', ref: 'A-2', eventType: 'signing', confidence: 0.2, facts: { amount: 50 } });
  // 1 个被撤销
  await h.ingest('b', 'b-1', { op: 'upsert', ref: 'B-1', eventType: 'signing', facts: { amount: 70 } });
  await h.ingest('b', 'b-2', { op: 'revoke', ref: 'B-1' });

  const stats = h.service.getStats();
  assert.equal(stats.effectiveProjectCount, 1);
  assert.equal(stats.byEventType.match, 1);
  assert.equal(stats.byEventType.signing, undefined);
  assert.equal(stats.totalAmount, 100); // 仅生效项目金额
  await h.close();
});

test('统计 as-of：历史口径只数当时已生效的版本', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'a-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const pid = created.body.result.projectId;
  const t1 = h.service.getTimeline(pid).timeline[0].recordedAt;

  await sleep(2);
  await h.ingest('a', 'a-2', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 200 } });
  await sleep(2);
  await h.ingest('a', 'a-3', { op: 'revoke', ref: 'A-1' });

  // T1：1 个生效，金额 100
  const s1 = h.service.getStats(t1);
  assert.equal(s1.effectiveProjectCount, 1);
  assert.equal(s1.totalAmount, 100);
  // 当前：撤销后 0 个
  assert.equal(h.service.getStats().effectiveProjectCount, 0);
  await h.close();
});
