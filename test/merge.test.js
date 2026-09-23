'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, send } = require('./helpers');

test('跨机构合并：单方确认 pending，双方确认后才生效', () => {
  const h = createHarness();
  const a = send(h.service, 'A', {
    event_id: 'af', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-P', payload: { category: 'park', amount: 100, name: '甲园区' },
  });
  const b = send(h.service, 'B', {
    event_id: 'bf', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'B-Q', payload: { category: 'park', amount: 200, name: '乙园区' },
  });

  const merge = send(h.service, 'A', {
    event_id: 'mg', action: 'merge', business_time: '2026-09-08T00:00:00Z',
    surviving_ref: 'A-P', absorbed_ref: 'B-Q', target_institution: 'B',
    payload: { reason: '两园一体化' },
  });
  assert.equal(merge.outcome, 'pending_counterpart');

  // 非当事方不能确认
  assert.throws(() => h.service.confirmMerge(merge.merge_id, 'C'),
    (err) => err.status === 403 && err.code === 'ERR_NOT_PARTY');

  // 仅一方确认时统计仍包含两个实体
  assert.equal(h.service.stats().active_projects, 2);

  h.service.confirmMerge(merge.merge_id, 'B');
  const proposal = h.service.getMerge(merge.merge_id);
  assert.equal(proposal.status, 'confirmed');
  assert.ok(proposal.applied_version_id);

  // B 的旧编号现在解析到 A 的存续项目
  const mapping = h.service.resolveMapping('B', 'B-Q', '2026-09-09T00:00:00Z');
  assert.equal(mapping.project_id, a.projectId);

  // 合并生效前的历史时点，B-Q 仍指向原项目
  const before = h.service.resolveMapping('B', 'B-Q', '2026-09-01T12:00:00Z');
  assert.equal(before.project_id, b.projectId);

  // 重复确认不报错也不产生第二个合并版本
  assert.throws(() => h.service.confirmMerge(merge.merge_id, 'B'),
    (err) => err.status === 409 && err.code === 'ERR_MERGE_STATE');
  const mergeVersions = h.db.prepare("SELECT COUNT(*) AS n FROM event_versions WHERE kind='merge'").get().n;
  assert.equal(mergeVersions, 1);
  h.close();
});

test('跨机构合并可被任一方拒绝', () => {
  const h = createHarness();
  send(h.service, 'A', { event_id: 'a', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-1', payload: { category: 'x' } });
  send(h.service, 'B', { event_id: 'b', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'B-1', payload: { category: 'x' } });
  const merge = send(h.service, 'A', {
    event_id: 'm', action: 'merge', business_time: '2026-09-03T00:00:00Z',
    surviving_ref: 'A-1', absorbed_ref: 'B-1', target_institution: 'B',
  });
  h.service.rejectMerge(merge.merge_id, 'B', '不是同一项目');
  assert.equal(h.service.getMerge(merge.merge_id).status, 'rejected');
  assert.equal(h.service.stats().active_projects, 2, '拒绝后实体不合并');
  h.close();
});

test('乱序合并：合并通知先于双方事实到达，实体补齐并经双方确认后合并', () => {
  const h = createHarness();
  const early = send(h.service, 'A', {
    event_id: 'm-early', action: 'merge', business_time: '2026-09-08T00:00:00Z',
    surviving_ref: 'A-LATE', absorbed_ref: 'B-LATE', target_institution: 'B',
  });
  assert.equal(early.outcome, 'awaiting_target');

  send(h.service, 'A', { event_id: 'a-late', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-LATE', payload: { category: 'park', amount: 10 } });
  // 只有 A 方实体时仍在等待
  assert.equal(h.db.prepare('SELECT COUNT(*) AS n FROM merge_proposals').get().n, 0);
  const b = send(h.service, 'B', { event_id: 'b-late', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'B-LATE', payload: { category: 'park', amount: 20 } });

  // B 方事实补齐：暂存合并自动转为提案，B 确认生效
  const proposal = h.db.prepare("SELECT * FROM merge_proposals WHERE status='pending_counterpart'").get();
  assert.ok(proposal, '前向暂存的合并已转为提案');
  h.service.confirmMerge(proposal.merge_id, 'B');
  assert.equal(h.service.getMerge(proposal.merge_id).status, 'confirmed');
  assert.equal(h.service.stats().active_projects, 1);
  h.close();
});

test('同机构合并无需双方确认，直接生效', () => {
  const h = createHarness();
  send(h.service, 'A', { event_id: 'a1', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-1', payload: { category: 'park', amount: 1 } });
  send(h.service, 'A', { event_id: 'a2', action: 'fact', business_time: '2026-09-02T00:00:00Z', external_ref: 'A-2', payload: { category: 'park', amount: 2 } });
  const merge = send(h.service, 'A', {
    event_id: 'mm', action: 'merge', business_time: '2026-09-05T00:00:00Z',
    surviving_ref: 'A-1', absorbed_ref: 'A-2', target_institution: 'A',
  });
  assert.equal(merge.outcome, 'merged');
  assert.equal(h.service.stats().active_projects, 1);
  h.close();
});
