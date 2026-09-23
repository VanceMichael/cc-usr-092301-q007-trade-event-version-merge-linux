'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHarness, send } = require('./helpers');

test('重复报文/重复事件幂等：不产生新版本', () => {
  const h = createHarness();
  const event = {
    event_id: 'evt-1', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-100', payload: { category: 'match', amount: 1000, name: '撮合单100' },
  };
  const first = send(h.service, 'A', event);
  assert.equal(first.outcome, 'accepted');

  // 相同 message_id 重放（补发）
  const replay = send(h.service, 'A', event, { messageId: 'msg-evt-1' });
  assert.equal(replay.outcome, 'duplicate');
  assert.equal(replay.versionId, first.versionId);

  // 新 message_id 但相同 event_id（另一方渠道重复转发）：报文留存但不产生版本
  const replay2 = send(h.service, 'A', event, { messageId: 'msg-evt-1-redelivered' });
  assert.equal(replay2.outcome, 'duplicate');

  const versions = h.db.prepare('SELECT COUNT(*) AS n FROM event_versions').get().n;
  assert.equal(versions, 1);
  const messages = h.db.prepare('SELECT COUNT(*) AS n FROM source_messages').get().n;
  assert.equal(messages, 2, '两次投递各留存一份报文');
  assert.equal(h.db.prepare('SELECT COUNT(*) AS n FROM source_messages WHERE processed=1').get().n, 2,
    '重复转发也标记为已处理');
  const ingestLogs = h.db.prepare("SELECT result FROM event_ingest_log WHERE event_id='evt-1'").all();
  assert.ok(ingestLogs.some((l) => l.result === 'duplicate'), '重放留有可追溯痕迹');
  h.close();
});

test('修订链：旧版本 superseded，统计只算最新生效版本', () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-1', payload: { category: 'match', amount: 100 },
  });
  send(h.service, 'A', {
    event_id: 'r1', action: 'revision', business_time: '2026-09-02T08:00:00Z',
    target_ref: 'A-1', payload: { category: 'match', amount: 120 },
  });
  const stats = h.service.stats();
  assert.deepEqual(stats.categories, [{ category: 'match', count: 1, total_amount: 120 }]);
  h.close();
});

test('机构只能纠正自己提交的事实：B 修订 A 的编号被拒', () => {
  const h = createHarness();
  const f = send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T08:00:00Z',
    external_ref: 'A-1', payload: { category: 'match', amount: 100 },
  });
  assert.throws(
    () => send(h.service, 'B', {
      event_id: 'b1', action: 'revision', business_time: '2026-09-02T08:00:00Z',
      target_ref: 'A-1', payload: { amount: 1 },
    }),
    (err) => err.status === 403 && err.code === 'ERR_FOREIGN_FACT',
  );
  // 事实未被改动
  const stats = h.service.stats();
  assert.equal(stats.categories[0].total_amount, 100);
  assert.equal(f.projectId.startsWith('prj_'), true);
  h.close();
});

test('乱序场景一：撤销先到、原始事实晚到，补发到达后撤销自动生效', () => {
  const h = createHarness();
  h.clock.set('2026-09-05T10:00:00Z');
  // 撤销先到（目标尚不存在）
  const early = send(h.service, 'A', {
    event_id: 'cancel-1', action: 'cancel', business_time: '2026-09-03T00:00:00Z',
    target_ref: 'A-X1', payload: { reason: '签约取消' },
  });
  assert.equal(early.outcome, 'awaiting_target');

  // 原始事实补发，比撤销通知更晚到达
  h.clock.advance(60_000);
  const fact = send(h.service, 'A', {
    event_id: 'fact-1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-X1', payload: { category: 'park', amount: 500 },
  });
  assert.equal(fact.outcome, 'accepted');

  const tl = h.service.timeline(fact.projectId);
  const states = Object.fromEntries(tl.versions.map((v) => [v.kind, v.state]));
  assert.equal(states.fact, 'cancelled', '晚到补发后，事实应被先到的撤销置为 cancelled');
  assert.equal(states.cancel, 'active');
  assert.deepEqual(h.service.stats().categories, [], '已撤销事实不进统计');

  const pending = h.db.prepare('SELECT COUNT(*) AS n FROM pending_commands').get().n;
  assert.equal(pending, 0, '补做完成的命令已清理');
  h.close();
});

test('乱序场景二：修订先到、事实晚到，按业务时间顺序形成修订链', () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'rev-early', action: 'revision', business_time: '2026-09-04T00:00:00Z',
    target_ref: 'A-Y', payload: { category: 'match', amount: 200 },
  });
  const fact = send(h.service, 'A', {
    event_id: 'fact-late', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-Y', payload: { category: 'match', amount: 100 },
  });
  const tl = h.service.timeline(fact.projectId);
  assert.equal(tl.versions[0].kind, 'fact');
  assert.equal(tl.versions[0].state, 'superseded');
  assert.equal(tl.versions[1].kind, 'revision');
  assert.equal(tl.versions[1].state, 'active');
  assert.equal(h.service.stats().categories[0].total_amount, 200);
  h.close();
});

test('撤销后同号重新报事实：新事实独立生效，历史可追溯', () => {
  const h = createHarness();
  send(h.service, 'A', {
    event_id: 'f1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-Z', payload: { category: 'match', amount: 100 },
  });
  send(h.service, 'A', {
    event_id: 'c1', action: 'cancel', business_time: '2026-09-02T00:00:00Z',
    target_ref: 'A-Z',
  });
  // 撤销之后机构用同一外部编号上报新事实
  const again = send(h.service, 'A', {
    event_id: 'f2', action: 'fact', business_time: '2026-09-05T00:00:00Z',
    external_ref: 'A-Z', payload: { category: 'match', amount: 300 },
  });
  assert.equal(again.outcome, 'accepted');
  const tl = h.service.timeline(again.projectId);
  assert.equal(tl.versions.find((v) => v.event_id === 'f2').state, 'active');
  assert.equal(h.service.stats().categories[0].total_amount, 300);
  h.close();
});

test('拆分：父版本 superseded，子项各自成为可追溯实体', () => {
  const h = createHarness();
  const parent = send(h.service, 'A', {
    event_id: 'big', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-BIG', payload: { category: 'project', amount: 900, name: '综合项目' },
  });
  const split = send(h.service, 'A', {
    event_id: 'sp1', action: 'split', business_time: '2026-09-06T00:00:00Z',
    target_ref: 'A-BIG',
    children: [
      { external_ref: 'A-S1', payload: { category: 'project', amount: 400, name: '一期' } },
      { external_ref: 'A-S2', payload: { category: 'project', amount: 500, name: '二期' } },
    ],
  });
  assert.equal(split.outcome, 'accepted');
  assert.equal(split.children.length, 2);
  const tl = h.service.timeline(parent.projectId);
  assert.equal(tl.versions.find((v) => v.event_id === 'big').state, 'superseded');
  const links = h.db.prepare("SELECT relation FROM version_links WHERE version_id=? AND relation='split_child'").all(split.versionId);
  assert.equal(links.length, 2);
  // 金额在子项上保持一致
  assert.equal(h.service.stats().categories[0].total_amount, 900);
  h.close();
});

test('签名校验失败：报文留存、事件不入库、返回 401', () => {
  const h = createHarness();
  assert.throws(
    () => send(h.service, 'A', {
      event_id: 'evil', action: 'fact', business_time: '2026-09-01T00:00:00Z', external_ref: 'A-1',
    }, { signature: 'sha256=deadbeef' }),
    (err) => err.status === 401 && err.code === 'ERR_BAD_SIGNATURE',
  );
  const row = h.db.prepare("SELECT signature_valid, processed FROM source_messages WHERE message_id='msg-evil'").get();
  assert.equal(row.signature_valid, 0);
  assert.equal(row.processed, 0);
  assert.equal(h.db.prepare('SELECT COUNT(*) AS n FROM events').get().n, 0);

  // 机构用正确签名补发同一 message_id 后正常入库
  const ok = send(h.service, 'A', {
    event_id: 'evil', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-1', payload: { category: 'match' },
  });
  assert.equal(ok.outcome, 'accepted');
  h.close();
});
