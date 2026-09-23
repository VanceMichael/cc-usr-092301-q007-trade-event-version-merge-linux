const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('乱序1：撤销先于补发到达，被撤销的旧事件不复活项目', async () => {
  const h = await createHarness();
  // 撤销先到（此时项目尚不存在）
  const revoke = await h.ingest('a', 'r-1', {
    op: 'revoke', ref: 'A-1', eventTime: '2026-09-21T00:00:00Z',
  });
  assert.equal(revoke.status, 202);
  assert.equal(revoke.body.result.tombstoned, true);
  assert.equal(revoke.body.result.projectId, null);
  // 统计中没有项目
  assert.equal((await h.request('GET', '/stats')).body.effectiveProjectCount, 0);

  // 被撤销的补发（更早的事件时间）晚到 -> 被压制为 revoked，不产生生效版本
  const late = await h.ingest('a', 'm-late', {
    op: 'upsert', ref: 'A-1', eventType: 'match',
    eventTime: '2026-09-19T00:00:00Z', facts: { amount: 100 },
  });
  assert.equal(late.body.result.suppressed, true);
  assert.equal(late.body.result.reason, 'already_revoked');
  assert.equal((await h.request('GET', '/stats')).body.effectiveProjectCount, 0);

  // 该版本状态为 revoked
  const v = late.body.result.versions[0];
  assert.equal(v.status, 'revoked');

  // 撤销之后发生的新事件（更晚事件时间）允许正常重建为新项目
  const fresh = await h.ingest('a', 'm-new', {
    op: 'upsert', ref: 'A-1', eventType: 'match',
    eventTime: '2026-09-22T00:00:00Z', facts: { amount: 200 },
  });
  assert.equal(fresh.body.result.created, true);
  assert.notEqual(fresh.body.result.projectId, late.body.result.projectId);
  assert.equal((await h.request('GET', '/stats')).body.effectiveProjectCount, 1);
  await h.close();
});

test('乱序2：撤销晚于已生效事件到达，项目被撤销且补发被墓碑拦截', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', {
    op: 'upsert', ref: 'A-1', eventType: 'match', eventTime: '2026-09-20T00:00:00Z', facts: { amount: 100 },
  });
  const pid = created.body.result.projectId;

  const revoked = await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-1' });
  assert.equal(revoked.body.result.revoked, true);
  assert.equal(revoked.body.result.projectId, pid);

  // 当前快照为空（已撤销）
  const tl = h.service.getTimeline(pid);
  assert.equal(tl.snapshot, null);

  // 撤销后旧事件的补发到达 -> 仍被拦截
  const replay = await h.ingest('a', 'm-replay', {
    op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 },
  });
  assert.equal(replay.body.result.suppressed, true);
  assert.equal(replay.body.result.projectId, pid); // 仍挂在原项目，不新建
  await h.close();
});

test('乱序3：修订先于创建到达进入 failed，创建到达后恢复成功', async () => {
  const h = await createHarness();
  // 显式 revise 先到 -> 503，消息落 failed
  const rev = await h.ingest('a', 'rev-1', { op: 'revise', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  assert.equal(rev.status, 503);
  let failed = (await h.request('GET', '/messages/failed')).body.messages;
  assert.equal(failed.length, 1);
  assert.equal(failed[0].status, 'failed');
  const failedId = failed[0].id;

  // 创建未到前恢复仍失败（幂等地保留 failed）
  const early = await h.request('POST', `/messages/${failedId}/recover`);
  assert.equal(early.status, 503);

  // 创建到达
  const created = await h.ingest('a', 'crt-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 90 } });
  const pid = created.body.result.projectId;

  // 恢复修订
  const recovered = await h.request('POST', `/messages/${failedId}/recover`);
  assert.equal(recovered.status, 200);
  assert.equal(recovered.body.status, 'processed');

  const tl = h.service.getTimeline(pid).timeline;
  assert.deepEqual(tl.map((v) => v.changeType), ['create', 'revise']);
  assert.equal(h.service.getTimeline(pid).snapshot.facts.amount, 100);

  // 已处理消息不能再次恢复
  const again = await h.request('POST', `/messages/${failedId}/recover`);
  assert.equal(again.status, 409);
  await h.close();
});

test('乱序4：低置信待审事件先被撤销，审核任务自动关闭', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', {
    op: 'upsert', ref: 'A-1', eventType: 'match', confidence: 0.3, facts: { amount: 50 },
  });
  const pid = created.body.result.projectId;
  assert.equal(created.body.result.reviewRequired, true);

  // 撤销在人工决策前到达
  await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-1' });
  const open = h.service.listOpenReviews();
  assert.equal(open.length, 0);
  assert.equal(h.service.getStats().effectiveProjectCount, 0);

  // 对已关闭审核决策 -> 409
  const decide = await h.request('POST', `/reviews/${created.body.result.reviewId}/decide`, {
    body: { decision: 'approve', expectedLockVersion: 1 },
  });
  assert.equal(decide.status, 409);
  await h.close();
});

test('拆分：一个已生效项目拆成两个可追溯子项目，父项目撤销', async () => {
  const h = await createHarness();
  const parent = await h.ingest('a', 'p-1', {
    op: 'upsert', ref: 'A-9', eventType: 'signing', facts: { amount: 900 },
  });
  const pid = parent.body.result.projectId;

  const split = await h.ingest('a', 'p-2', {
    op: 'split', ref: 'A-9',
    parts: [
      { ref: 'A-9-X', facts: { amount: 400 } },
      { ref: 'A-9-Y', facts: { amount: 500 } },
    ],
  });
  assert.equal(split.status, 202);
  assert.equal(split.body.result.children.length, 2);
  const [c1, c2] = split.body.result.children;

  // 父项目无生效快照
  assert.equal(h.service.getTimeline(pid).snapshot, null);
  // 子项目各自有 split_out 版本并指回父项目
  const t1 = h.service.getTimeline(c1).timeline;
  assert.equal(t1[0].changeType, 'split_out');
  assert.equal(t1[0].relatedProjectId, pid);
  // 子项目标识可独立路由并修订
  const upd = await h.ingest('a', 'p-3', { op: 'upsert', ref: 'A-9-X', eventType: 'signing', facts: { amount: 401 } });
  assert.equal(upd.body.result.projectId, c1);

  // 非主报机构不能拆分
  const other = await h.ingest('b', 'b-9', {
    op: 'split', ref: 'A-9', parts: [{ ref: 'X' }, { ref: 'Y' }],
  });
  // B 无 A-9 映射 -> 404，若映射存在则 403；二者都表明不允许
  assert.ok([403, 404].includes(other.status));
  await h.close();
});

test('拆分对非生效项目返回 409', async () => {
  const h = await createHarness();
  await h.ingest('a', 'p-1', { op: 'upsert', ref: 'A-9', eventType: 'signing', facts: {} });
  await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-9' });
  const split = await h.ingest('a', 'p-2', {
    op: 'split', ref: 'A-9', parts: [{ ref: 'X' }, { ref: 'Y' }],
  });
  assert.equal(split.status, 409);
  await h.close();
});
