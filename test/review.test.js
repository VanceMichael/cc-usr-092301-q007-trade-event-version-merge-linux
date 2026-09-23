'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, send } = require('./helpers');

test('低置信度事件进入人工审核，裁决前不计统计', () => {
  const h = createHarness();
  const r = send(h.service, 'A', {
    event_id: 'lc1', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-LC', confidence: 0.5,
    payload: { category: 'match', amount: 77 },
  });
  assert.equal(r.outcome, 'pending_review');
  assert.ok(r.reviewId);
  assert.deepEqual(h.service.stats().categories, []);

  const open = h.service.listReviews('open');
  assert.equal(open.length, 1);
  assert.equal(open[0].reason, 'low_confidence_entity');

  const decided = h.service.decideReview(r.reviewId, { decision: 'approved', reviewer: 'zhang' });
  assert.equal(decided.decision, 'approved');
  assert.equal(h.service.stats().categories[0].total_amount, 77);
  assert.equal(h.service.listReviews('open').length, 0);
  h.close();
});

test('审核驳回：实体与待审版本作废，映射关闭，后续版本不再计入', () => {
  const h = createHarness();
  const r = send(h.service, 'A', {
    event_id: 'lc2', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-BAD', confidence: 0.3, payload: { category: 'match', amount: 999 },
  });
  h.service.decideReview(r.reviewId, { decision: 'rejected', reviewer: 'li', note: '无法证实' });
  assert.deepEqual(h.service.stats().categories, []);
  const project = h.db.prepare('SELECT status FROM projects WHERE project_id=?').get(r.projectId);
  assert.equal(project.status, 'discarded');
  const mapping = h.db.prepare('SELECT valid_to FROM identifier_mappings WHERE institution_code=? AND external_ref=?').get('A', 'A-BAD');
  assert.ok(mapping.valid_to, '驳回关闭映射有效期');
  h.close();
});

test('审核裁决实体关系：低置信度编号并入既有项目', () => {
  const h = createHarness();
  const established = send(h.service, 'A', {
    event_id: 'e1', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-KNOWN', payload: { category: 'park', amount: 100 },
  });
  const r = send(h.service, 'B', {
    event_id: 'e2', action: 'fact', business_time: '2026-09-02T00:00:00Z',
    external_ref: 'B-NEW', confidence: 0.4, payload: { category: 'park', amount: 50 },
  });
  assert.equal(r.outcome, 'pending_review');

  // 审核员裁定 B-NEW 就是既有项目
  h.service.decideReview(r.reviewId, {
    decision: 'approved', reviewer: 'wang',
    target: { project_id: established.projectId },
  });
  const stats = h.service.stats();
  assert.equal(stats.active_projects, 1, '并入后只有一个项目');
  assert.equal(stats.categories[0].total_amount, 150);
  const mapping = h.service.resolveMapping('B', 'B-NEW', h.clock.iso());
  assert.equal(mapping.project_id, established.projectId);
  h.close();
});

test('并发审核冲突：两人同时裁决同一任务，后到者收到 409', () => {
  const h = createHarness();
  const r = send(h.service, 'A', {
    event_id: 'race', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-RACE', confidence: 0.2, payload: { category: 'match' },
  });
  const task = h.db.prepare('SELECT * FROM review_tasks WHERE review_id=?').get(r.reviewId);
  assert.equal(task.status, 'open');

  // 两个先后提交的事务模拟并发裁决（乐观锁 UPDATE ... WHERE status='open'）
  h.db.exec('BEGIN IMMEDIATE');
  const first = h.db.prepare(
    "UPDATE review_tasks SET status='approved', decided_at=?, decided_by='racer-1' WHERE review_id=? AND status='open'",
  ).run(new Date().toISOString(), r.reviewId);
  assert.equal(first.changes, 1);
  h.db.exec('COMMIT');

  h.db.exec('BEGIN IMMEDIATE');
  const second = h.db.prepare(
    "UPDATE review_tasks SET status='rejected', decided_at=?, decided_by='racer-2' WHERE review_id=? AND status='open'",
  ).run(new Date().toISOString(), r.reviewId);
  assert.equal(second.changes, 0, '第二个裁决者影响行数为 0');
  h.db.exec('ROLLBACK');

  // 服务层再次裁决必须报 409
  assert.throws(
    () => h.service.decideReview(r.reviewId, { decision: 'approved', reviewer: 'late' }),
    (err) => err.status === 409,
  );
  h.close();
});

test('通过服务接口串行双裁：第二个裁决者收到冲突且不改变结果', () => {
  const h = createHarness();
  const r = send(h.service, 'A', {
    event_id: 'race2', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-R2', confidence: 0.2, payload: { category: 'match', amount: 10 },
  });
  h.service.decideReview(r.reviewId, { decision: 'approved', reviewer: 'one' });
  assert.throws(
    () => h.service.decideReview(r.reviewId, { decision: 'rejected', reviewer: 'two' }),
    (err) => err.status === 409 && err.code === 'ERR_REVIEW_NOT_OPEN',
  );
  assert.equal(h.service.stats().categories[0].total_amount, 10, '首个裁决生效，驳回未覆盖');
  h.close();
});
