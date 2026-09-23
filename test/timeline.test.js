'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, send } = require('./helpers');

test('时间线按“当时已知信息”重建：晚到撤销不改变历史时点的状态', () => {
  const h = createHarness();
  h.clock.set('2026-09-10T00:00:00.000Z');

  // t1 事实入库（系统在 09-10 才知道这件 09-01 发生的事）
  const fact = send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-TL', payload: { category: 'match', amount: 100 },
  });
  const t1 = h.clock.iso();

  // 09-12 撤销通知到达（撤销业务时间为 09-05）
  h.clock.set('2026-09-12T09:00:00.000Z');
  send(h.service, 'A', {
    event_id: 'c1', action: 'cancel', business_time: '2026-09-05T00:00:00Z',
    target_ref: 'A-TL', payload: { reason: '客户撤单' },
  });
  const t2 = h.clock.iso();

  // 在 09-11 查询：当时还不知道撤销，事实仍 active
  const past = h.service.timeline(fact.projectId, '2026-09-11T00:00:00.000Z');
  assert.equal(past.versions.length, 1, '历史时点看不到尚未入库的撤销版本');
  assert.equal(past.versions[0].state, 'active');

  // 在现在查询：事实已 cancelled
  const now = h.service.timeline(fact.projectId, t2);
  const states = Object.fromEntries(now.versions.map((v) => [v.event_id, v.state]));
  assert.equal(states.f1, 'cancelled');
  assert.equal(states.c1, 'active');

  // as_of 早于事实入库：时间线为空
  const before = h.service.timeline(fact.projectId, '2026-09-09T00:00:00.000Z');
  assert.equal(before.versions.length, 0);
  h.close();
});

test('时间线反映修订链与业务时间排序', () => {
  const h = createHarness();
  h.clock.set('2026-09-10T00:00:00.000Z');
  const fact = send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-R', payload: { category: 'match', amount: 100, name: '初版' },
  });
  h.clock.advance(86_400_000);
  send(h.service, 'A', {
    event_id: 'r1', action: 'revision', business_time: '2026-09-03T00:00:00Z',
    target_ref: 'A-R', payload: { category: 'match', amount: 140, name: '修订版' },
  });
  const tl = h.service.timeline(fact.projectId);
  assert.deepEqual(tl.versions.map((v) => [v.kind, v.state]),
    [['fact', 'superseded'], ['revision', 'active']]);
  // parent 链可追溯
  const link = h.db.prepare("SELECT * FROM version_links WHERE relation='revision_base'").get();
  assert.ok(link);
  h.close();
});

test('统计的时间点语义：as_of 只统计当时已知且生效的版本', () => {
  const h = createHarness();
  h.clock.set('2026-09-10T00:00:00.000Z');
  send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-S', payload: { category: 'match', amount: 100 },
  });
  h.clock.set('2026-09-15T00:00:00.000Z');
  send(h.service, 'A', {
    event_id: 'c1', action: 'cancel', business_time: '2026-09-08T00:00:00Z',
    target_ref: 'A-S',
  });
  const at11 = h.service.stats('2026-09-11T00:00:00.000Z');
  assert.equal(at11.categories[0].total_amount, 100, '历史时点统计包含当时仍生效的事实');
  const atNow = h.service.stats('2026-09-16T00:00:00.000Z');
  assert.deepEqual(atNow.categories, []);
  h.close();
});

test('合并后时间线在存续项目上可见吸收关系', () => {
  const h = createHarness();
  const a = send(h.service, 'A', { event_id: 'a', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-1', payload: { category: 'park', amount: 100 } });
  const b = send(h.service, 'B', { event_id: 'b', action: 'fact', business_time: '2026-09-02T00:00:00Z', external_ref: 'B-1', payload: { category: 'park', amount: 200 } });
  const m = send(h.service, 'A', { event_id: 'm', action: 'merge', business_time: '2026-09-07T00:00:00Z', surviving_ref: 'A-1', absorbed_ref: 'B-1', target_institution: 'B' });
  h.service.confirmMerge(m.merge_id, 'B');

  const survivingTl = h.service.timeline(a.projectId);
  const mergeEntry = survivingTl.versions.find((v) => v.kind === 'merge');
  assert.ok(mergeEntry, '存续项目时间线包含合并版本');
  assert.equal(mergeEntry.state, 'active');

  const absorbedTl = h.service.timeline(b.projectId);
  assert.ok(absorbedTl.versions.some((v) => v.kind === 'merge_in' && v.state === 'merged'),
    '被吸收项目时间线标记 merged');
  h.close();
});
