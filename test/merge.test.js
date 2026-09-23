const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

async function twoProjects(h) {
  const a = await h.ingest('a', 'a-1', {
    op: 'upsert', ref: 'A-1', eventType: 'signing', facts: { amount: 100, owner: 'A' },
  });
  const b = await h.ingest('b', 'b-1', {
    op: 'upsert', ref: 'B-1', eventType: 'signing', facts: { city: 'SP' },
  });
  return { pa: a.body.result.projectId, pb: b.body.result.projectId };
}

test('跨机构合并：提案须对方确认，单方不能完成', async () => {
  const h = await createHarness();
  const { pa, pb } = await twoProjects(h);
  const proposal = await h.ingest('a', 'a-m', { op: 'merge', ref: 'A-1', fromProjectId: pa, intoProjectId: pb });
  assert.equal(proposal.body.result.mergeProposed, true);
  const mergeId = proposal.body.result.mergeId;
  assert.equal(proposal.body.result.requiresConfirmationFrom, 'org-b');

  // 提案方不能确认自己的提案
  const self = await h.request('POST', `/merges/${mergeId}/confirm`, { headers: { 'x-org-id': 'org-a' } });
  assert.equal(self.status, 403);

  // 无关机构不能确认
  const other = await h.request('POST', `/merges/${mergeId}/confirm`, { headers: { 'x-org-id': 'org-c' } });
  assert.equal(other.status, 403);

  // 合并完成前统计仍是两个生效项目（并入方各一个）
  assert.equal(h.service.getStats().effectiveProjectCount, 2);

  // 确认方确认 -> 合并生效
  const confirmed = await h.request('POST', `/merges/${mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });
  assert.equal(confirmed.status, 200);
  assert.equal(confirmed.body.merged, true);
  assert.equal(confirmed.body.intoProjectId, pb);

  // 重复确认 -> 409
  const twice = await h.request('POST', `/merges/${mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });
  assert.equal(twice.status, 409);

  // 合并后只剩并入项目一个生效版本（from 已撤销，into 旧版本 superseded）
  assert.equal(h.service.getStats().effectiveProjectCount, 1);
  const facts = h.service.getTimeline(pb).snapshot.facts;
  assert.equal(facts.amount, 100);
  assert.equal(facts.city, 'SP');
  await h.close();
});

test('合并后标识重指向：发起方的旧 ref 路由到并入项目', async () => {
  const h = await createHarness();
  const { pa, pb } = await twoProjects(h);
  const proposal = await h.ingest('a', 'a-m', { op: 'merge', ref: 'A-1', fromProjectId: pa, intoProjectId: pb });
  await h.request('POST', `/merges/${proposal.body.result.mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });

  const upd = await h.ingest('a', 'a-2', { op: 'upsert', ref: 'A-1', eventType: 'signing', facts: { amount: 110 } });
  assert.equal(upd.body.result.projectId, pb);
  assert.equal(upd.body.result.revised, true);
  const facts = h.service.getTimeline(pb).snapshot.facts;
  assert.equal(facts.amount, 110);
  assert.equal(facts.city, 'SP'); // 对方字段保留
  await h.close();
});

test('机构只能纠正自己提交的字段，跨机构覆盖被拒绝', async () => {
  const h = await createHarness();
  const { pb } = await (async () => {
    const { pa, pb } = await twoProjects(h);
    const proposal = await h.ingest('a', 'a-m', { op: 'merge', ref: 'A-1', fromProjectId: pa, intoProjectId: pb });
    await h.request('POST', `/merges/${proposal.body.result.mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });
    return { pa, pb };
  })();

  // A 改 B 的字段 city -> 403
  const blocked = await h.ingest('a', 'a-x', { op: 'upsert', ref: 'A-1', eventType: 'signing', facts: { city: 'RJ' } });
  assert.equal(blocked.status, 403);
  assert.equal(blocked.body.error, 'not_your_fact');

  // A 改自己的 amount -> 成功
  const own = await h.ingest('a', 'a-y', { op: 'upsert', ref: 'A-1', eventType: 'signing', facts: { amount: 130 } });
  assert.equal(own.body.result.revised, true);

  // B 改自己的 city -> 成功
  const bOwn = await h.ingest('b', 'b-2', { op: 'upsert', ref: 'B-1', eventType: 'signing', facts: { city: 'RJ' } });
  assert.equal(bOwn.body.result.revised, true);
  const facts = h.service.getTimeline(pb).snapshot.facts;
  assert.equal(facts.amount, 130);
  assert.equal(facts.city, 'RJ');
  await h.close();
});

test('确认方可拒绝合并提案', async () => {
  const h = await createHarness();
  const { pa, pb } = await twoProjects(h);
  const proposal = await h.ingest('a', 'a-m', { op: 'merge', ref: 'A-1', fromProjectId: pa, intoProjectId: pb });
  const mergeId = proposal.body.result.mergeId;
  const rejected = await h.request('POST', `/merges/${mergeId}/reject`, {
    headers: { 'x-org-id': 'org-b' }, body: { reason: 'not same project' },
  });
  assert.equal(rejected.status, 200);
  assert.equal(rejected.body.status, 'rejected');
  // 两项目仍各自生效
  assert.equal(h.service.getStats().effectiveProjectCount, 2);
  // 拒绝后不能再确认
  const confirm = await h.request('POST', `/merges/${mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });
  assert.equal(confirm.status, 409);
  await h.close();
});

test('被合并项目所有方之外的机构不能发起合并（403）', async () => {
  const h = await createHarness();
  const { pa, pb } = await twoProjects(h);
  // B 试图把 A 的项目并入自己
  const res = await h.ingest('b', 'b-x', { op: 'merge', ref: 'B-1', fromProjectId: pa, intoProjectId: pb });
  assert.equal(res.status, 403);
  await h.close();
});

test('同机构内部纠错性合并不需双方确认即生效', async () => {
  const h = await createHarness();
  const x = await h.ingest('a', 'a-1', { op: 'upsert', ref: 'A-1', eventType: 'signing', facts: { amount: 1 } });
  const y = await h.ingest('a', 'a-2', { op: 'upsert', ref: 'A-2', eventType: 'signing', facts: { amount: 2 } });
  const res = await h.ingest('a', 'a-3', { op: 'merge', ref: 'A-1', fromProjectId: x.body.result.projectId, intoProjectId: y.body.result.projectId });
  assert.equal(res.body.result.merged, true);
  assert.equal(h.service.getStats().effectiveProjectCount, 1);
  await h.close();
});
