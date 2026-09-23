const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('低置信事件进入人工审核，批准后才生效并补映射', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'm-1', {
    op: 'upsert', ref: 'A-1', eventType: 'match', confidence: 0.4, facts: { amount: 50 },
  });
  const pid = created.body.result.projectId;
  assert.equal(created.body.result.reviewRequired, true);
  // 待审期间统计不含
  assert.equal(h.service.getStats().effectiveProjectCount, 0);

  const reviewId = created.body.result.reviewId;
  const res = await h.request('POST', `/reviews/${reviewId}/decide`, {
    body: { decision: 'approve', expectedLockVersion: 1, actor: 'alice' },
  });
  assert.equal(res.status, 200);
  assert.equal(res.body.decision, 'approve');
  assert.equal(h.service.getStats().effectiveProjectCount, 1);

  // 映射已确认，后续事件正常路由
  const next = await h.ingest('a', 'm-2', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 55 } });
  assert.equal(next.body.result.projectId, pid);
  await h.close();
});

test('人工 link 决定把低置信事件关联到已有项目', async () => {
  const h = await createHarness();
  const existing = await h.ingest('b', 'b-1', {
    op: 'upsert', ref: 'B-1', eventType: 'match', facts: { city: 'SP' },
  });
  const targetPid = existing.body.result.projectId;

  const pending = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-9', eventType: 'match', confidence: 0.2, facts: { amount: 77 },
  });
  const orphanPid = pending.body.result.projectId;
  const reviewId = pending.body.result.reviewId;

  const res = await h.request('POST', `/reviews/${reviewId}/decide`, {
    body: { decision: 'link', targetProjectId: targetPid, expectedLockVersion: 1, actor: 'alice' },
  });
  assert.equal(res.status, 200);
  assert.equal(res.body.projectId, targetPid);

  // 目标项目事实合并，字段来源保留为上报机构 A
  const facts = h.service.getTimeline(targetPid).snapshot.facts;
  assert.equal(facts.city, 'SP');
  assert.equal(facts.amount, 77);

  // A 的标识现在路由到目标项目
  const later = await h.ingest('a', 'a-2', { op: 'upsert', ref: 'A-9', eventType: 'match', facts: { amount: 78 } });
  assert.equal(later.body.result.projectId, targetPid);

  // 原挂起项目的版本被 rejected，不产生第二个生效项目
  assert.notEqual(orphanPid, targetPid);
  assert.equal(h.service.getStats().effectiveProjectCount, 1);
  await h.close();
});

test('人工 reject 决定拒绝事件，版本标记 rejected', async () => {
  const h = await createHarness();
  const pending = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-9', eventType: 'match', confidence: 0.2, facts: {},
  });
  const pid = pending.body.result.projectId;
  const res = await h.request('POST', `/reviews/${pending.body.result.reviewId}/decide`, {
    body: { decision: 'reject', expectedLockVersion: 1 },
  });
  assert.equal(res.status, 200);
  assert.equal(h.service.getStats().effectiveProjectCount, 0);
  const tl = h.service.getTimeline(pid).timeline;
  assert.equal(tl[tl.length - 1].statusAt, 'rejected');
  await h.close();
});

test('并发审核冲突：过期锁版本被拒绝(409)，正确锁版本成功', async () => {
  const h = await createHarness();
  const pending = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-9', eventType: 'match', confidence: 0.2, facts: {},
  });
  const reviewId = pending.body.result.reviewId;

  // 先读出当前 lockVersion=1
  const got = await h.request('GET', `/reviews/${reviewId}`);
  assert.equal(got.body.lockVersion, 1);

  // 用过期版本号 0 决策 -> 409
  const stale = await h.request('POST', `/reviews/${reviewId}/decide`, {
    body: { decision: 'approve', expectedLockVersion: 0 },
  });
  assert.equal(stale.status, 409);

  // 正确版本号成功
  const ok = await h.request('POST', `/reviews/${reviewId}/decide`, {
    body: { decision: 'approve', expectedLockVersion: 1 },
  });
  assert.equal(ok.status, 200);

  // 成功后再用旧锁版本（或不带）决策 -> 409（任务已决）
  const again = await h.request('POST', `/reviews/${reviewId}/decide`, {
    body: { decision: 'reject', expectedLockVersion: 1 },
  });
  assert.equal(again.status, 409);
  await h.close();
});

test('待审期间同标识的并发事件被挂起(503)，审核批准后恢复可正常归并', async () => {
  const h = await createHarness();
  const created = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-9', eventType: 'match', confidence: 0.2, facts: { amount: 50 },
  });
  const pid = created.body.result.projectId;

  // 审核未决时，同标识再来一条高置信事件 -> 503 可重试，不会另建项目
  const blocked = await h.ingest('a', 'a-2', {
    op: 'upsert', ref: 'A-9', eventType: 'match', facts: { amount: 60 },
  });
  assert.equal(blocked.status, 503);
  const fid = h.service.listFailedMessages()[0].id;

  // 批准后恢复该挂起消息 -> 作为修订归并到同一项目
  await h.request('POST', `/reviews/${created.body.result.reviewId}/decide`, {
    body: { decision: 'approve', expectedLockVersion: 1 },
  });
  const recovered = await h.request('POST', `/messages/${fid}/recover`);
  assert.equal(recovered.status, 200);
  assert.equal(recovered.body.result.projectId, pid);
  assert.equal(h.service.getTimeline(pid).snapshot.facts.amount, 60);
  // 始终只有一个项目
  assert.equal(h.service.getStats().effectiveProjectCount, 1);
  await h.close();
});

test('两个并发决策只有一个生效：直接调用服务模拟竞争提交', async () => {
  const h = await createHarness();
  const pending = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-9', eventType: 'match', confidence: 0.2, facts: {},
  });
  const reviewId = pending.body.result.reviewId;
  const review = h.service.getReview(reviewId);

  // 双方都读到 lock_version=1；第一个成功，第二个因乐观锁失败
  const first = h.service.decideReview(reviewId, { decision: 'approve', expectedLockVersion: review.lock_version });
  assert.equal(first.decision, 'approve');
  assert.throws(
    () => h.service.decideReview(reviewId, { decision: 'reject', expectedLockVersion: review.lock_version }),
    (e) => e.status === 409,
  );
  // 最终状态以第一个决策为准
  assert.equal(h.service.getReview(reviewId).status, 'approved');
  await h.close();
});
