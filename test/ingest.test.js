const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./helpers');

test('来源报文与签名校验结果都被保存', async () => {
  const h = await createHarness();
  const event = { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } };
  const res = await h.ingest('a', 'm-1', event);
  assert.equal(res.status, 202);
  const row = h.db.prepare('SELECT * FROM source_messages WHERE msg_id=?').get('m-1');
  assert.equal(row.org_id, 'org-a');
  assert.equal(row.signature_valid, 1);
  assert.equal(JSON.parse(row.raw_payload).ref, 'A-1');
  assert.ok(row.signature && row.signature.length === 64);
  await h.close();
});

test('错误签名被拒绝（401），报文仍留痕为 rejected', async () => {
  const h = await createHarness();
  const res = await h.ingest('a', 'm-bad', { op: 'upsert', ref: 'A-1' }, { sign: false });
  // sign:false 不带签名
  assert.equal(res.status, 401);
  let row = h.db.prepare("SELECT * FROM source_messages WHERE msg_id='m-bad'").get();
  assert.equal(row.signature_valid, 0);
  assert.equal(row.signature_error, 'missing_signature');
  assert.equal(row.status, 'rejected');

  // 错误签名值
  const raw = JSON.stringify({ op: 'upsert', ref: 'A-2' });
  const res2 = await h.request('POST', '/ingest/org-a', {
    raw, headers: { 'x-msg-id': 'm-bad2', 'x-signature': '0'.repeat(64) },
  });
  assert.equal(res2.status, 401);
  row = h.db.prepare("SELECT * FROM source_messages WHERE msg_id='m-bad2'").get();
  assert.equal(row.signature_error, 'bad_signature');
  assert.equal(row.status, 'rejected');
  // 签名失败不产生任何项目版本
  assert.equal(h.db.prepare('SELECT COUNT(*) AS n FROM project_versions').get().n, 0);
  await h.close();
});

test('未知机构投递返回 404', async () => {
  const h = await createHarness({ orgs: ['a'] });
  const res = await h.request('POST', '/ingest/org-zzz', {
    body: { op: 'upsert', ref: 'Z-1' },
    headers: { 'x-msg-id': 'm', 'x-signature': '0'.repeat(64) },
  });
  assert.equal(res.status, 404);
  await h.close();
});

test('重放同一 (org,msg_id) 幂等：不产生新版本', async () => {
  const h = await createHarness();
  const event = { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } };
  const first = await h.ingest('a', 'm-1', event);
  const pid = first.body.result.projectId;
  const again = await h.ingest('a', 'm-1', event);
  assert.equal(again.status, 202);
  assert.equal(again.body.duplicate, true);
  assert.equal(again.body.status, 'processed');
  const versions = h.db.prepare('SELECT COUNT(*) AS n FROM project_versions WHERE project_id=?').get(pid).n;
  assert.equal(versions, 1);
  await h.close();
});

test('内容相同的重复事件不产生新版本；不同事实产生修订版本', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  const dup = await h.ingest('a', 'm-2', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 100 } });
  assert.equal(dup.body.result.duplicate, true);
  assert.equal(dup.body.result.versions.length, 0);

  const rev = await h.ingest('a', 'm-3', { op: 'upsert', ref: 'A-1', eventType: 'match', facts: { amount: 120 } });
  assert.equal(rev.body.result.revised, true);
  assert.equal(rev.body.result.versions.length, 1);
  await h.close();
});

test('显式 create 对已存在标识返回 409', async () => {
  const h = await createHarness();
  await h.ingest('a', 'm-1', { op: 'create', ref: 'A-1', eventType: 'match', facts: {} });
  const res = await h.ingest('a', 'm-2', { op: 'create', ref: 'A-1', eventType: 'match', facts: {} });
  assert.equal(res.status, 409);
  await h.close();
});
