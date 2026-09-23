const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('标识映射：创建即生效（confirmed, valid_to 为空），按机构隔离', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: {} });

  const mine = (await h.request('GET', '/mappings?orgId=org-a')).body.mappings;
  assert.equal(mine.length, 1);
  assert.equal(mine[0].externalRef, 'A-1');
  assert.equal(mine[0].status, 'confirmed');
  assert.equal(mine[0].validTo, null);
  assert.ok(mine[0].validFrom);

  const others = (await h.request('GET', '/mappings?orgId=org-b')).body.mappings;
  assert.equal(others.length, 0);
  await h.close();
});

test('标识映射：撤销后旧映射置过期（deprecated + valid_to），同标识重建产生新映射', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', {
    op: 'upsert', ref: 'A-1', eventType: 'match', eventTime: '2026-09-20T00:00:00Z', facts: {},
  });
  await h.ingest('a', 'r-1', { op: 'revoke', ref: 'A-1', eventTime: '2026-09-21T00:00:00Z' });

  let mappings = h.service.listMappings('org-a');
  assert.equal(mappings[0].status, 'deprecated');
  assert.ok(mappings[0].valid_to);

  // 撤销之后的新事件重建 -> 新 confirmed 映射指向新项目
  await h.ingest('a', 'm-2', {
    op: 'upsert', ref: 'A-1', eventType: 'match', eventTime: '2026-09-22T00:00:00Z', facts: {},
  });
  mappings = h.service.listMappings('org-a');
  const live = mappings.filter((m) => m.status === 'confirmed');
  const dead = mappings.filter((m) => m.status === 'deprecated');
  assert.equal(live.length, 1);
  assert.equal(dead.length, 1);
  assert.notEqual(live[0].project_id, dead[0].project_id);
  await h.close();
});

test('合并后被并入项目的标识映射重指向并入项目，旧映射置过期', async () => {
  const h = await createHarness();
  const a = await h.ingest('a', 'a-1', { op: 'upsert', ref: 'A-1', eventType: 'signing', facts: {} });
  const b = await h.ingest('b', 'b-1', { op: 'upsert', ref: 'B-1', eventType: 'signing', facts: {} });
  const pa = a.body.result.projectId;
  const pb = b.body.result.projectId;
  const proposal = await h.ingest('a', 'a-m', { op: 'merge', ref: 'A-1', fromProjectId: pa, intoProjectId: pb });
  await h.request('POST', `/merges/${proposal.body.result.mergeId}/confirm`, { headers: { 'x-org-id': 'org-b' } });

  const aMappings = h.service.listMappings('org-a');
  const live = aMappings.filter((m) => m.status === 'confirmed');
  assert.equal(live.length, 1);
  assert.equal(live[0].project_id, pb); // 重指向
  assert.equal(live[0].external_ref, 'A-1');
  await h.close();
});

test('确定性业务错误（未知 op）进入死信，不重试', async () => {
  const h = await createHarness();
  const res = await h.ingest('a', 'm-x', { op: 'teleport', ref: 'A-1' });
  assert.equal(res.status, 400);
  const failed = (await h.request('GET', '/messages/failed')).body.messages;
  assert.equal(failed.length, 1);
  assert.equal(failed[0].status, 'dead_letter');

  // 死信也允许人工触发恢复（重新执行业务逻辑）；此处仍因未知 op 失败
  const recover = await h.request('POST', `/messages/${failed[0].id}/recover`);
  assert.equal(recover.status, 400);
  await h.close();
});
