'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { openDatabase } = require('../src/database');
const { DatabaseSync } = require('node:sqlite');
const { mkdirSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join } = require('node:path');

test('迁移在文件库上幂等应用，重开不重复执行', () => {
  const dir = mkdirSync(join(tmpdir(), `trade-mig-${Date.now()}-${Math.random()}`), { recursive: true });
  const file = join(dir, 'trade.sqlite3');

  const db1 = openDatabase(file);
  const versions1 = db1.prepare('SELECT version FROM schema_versions ORDER BY version').all().map((r) => r.version);
  assert.deepEqual(versions1, [1, 2, 3, 4, 5]);
  // 关键表齐备
  for (const table of ['institutions', 'source_messages', 'identifier_mappings',
    'events', 'event_versions', 'version_links', 'pending_commands',
    'review_tasks', 'merge_proposals', 'merge_confirmations',
    'notification_outbox', 'notification_deliveries']) {
    assert.ok(db1.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table), `缺少表 ${table}`);
  }
  db1.close();

  // 重新打开：不新增版本、不报错
  const db2 = openDatabase(file);
  const versions2 = db2.prepare('SELECT COUNT(*) AS n FROM schema_versions').get().n;
  assert.equal(versions2, 5);
  db2.close();
});

test('从 v1 基线库升级：旧库可平滑迁移到最新版本', () => {
  const dir = mkdirSync(join(tmpdir(), `trade-up-${Date.now()}-${Math.random()}`), { recursive: true });
  const file = join(dir, 'legacy.sqlite3');

  // 手工构造只有 v1 的旧库（模拟历史基线）
  const legacy = new DatabaseSync(file);
  legacy.exec('PRAGMA foreign_keys=ON;');
  legacy.exec('CREATE TABLE schema_versions(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);');
  legacy.prepare('INSERT INTO schema_versions(version, applied_at) VALUES(1, ?)').run('2026-08-01T00:00:00Z');
  legacy.close();

  const db = openDatabase(file);
  const versions = db.prepare('SELECT version FROM schema_versions ORDER BY version').all().map((r) => r.version);
  assert.deepEqual(versions, [1, 2, 3, 4, 5], '补齐 v2-v5');
  // 迁移后服务立即可用：插入并解析一条事件
  const { createEventService } = require('../src/event-service');
  const svc = createEventService(db);
  svc.registerInstitution({ code: 'A', name: 'A', secret: 'k' });
  const { computeSignature } = require('../src/sig');
  const raw = JSON.stringify({
    event_id: 'u1', action: 'fact', business_time: '2026-09-01T00:00:00Z',
    external_ref: 'A-1', payload: { category: 'match', amount: 9 },
  });
  const r = svc.ingest('A', raw, `sha256=${computeSignature('k', raw)}`, 'msg-u1');
  assert.equal(r.outcome, 'accepted');
  db.close();
});
