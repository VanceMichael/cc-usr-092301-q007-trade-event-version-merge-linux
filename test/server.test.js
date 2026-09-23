const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('健康入口可用', async () => {
  const h = await createHarness();
  const res = await h.request('GET', '/health');
  assert.equal(res.status, 200);
  assert.deepEqual(res.body, { status: 'ok' });
  await h.close();
});

test('机构登记后可在 /orgs 创建', async () => {
  const h = await createHarness({ orgs: [] });
  const res = await h.request('POST', '/orgs', { body: { orgId: 'org-x', name: 'X', hmacSecret: 'shh' } });
  assert.equal(res.status, 201);
  assert.equal(res.body.orgId, 'org-x');
  await h.close();
});
