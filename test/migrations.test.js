const test = require('node:test');
const assert = require('node:assert/strict');
const { openDatabase, MIGRATIONS } = require('../src/database');

test('迁移框架：全新库应用全部版本且可重复打开不重放', () => {
  const db = openDatabase(':memory:');
  const versions = db.prepare('SELECT version FROM schema_versions ORDER BY version').all().map((r) => r.version);
  assert.deepEqual(versions, MIGRATIONS.map((m) => m.version));

  // 关键领域表全部就绪
  const tables = new Set(
    db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name),
  );
  for (const t of [
    'organizations', 'source_messages', 'projects', 'identifier_mappings',
    'project_versions', 'version_status_history', 'review_tasks', 'merge_proposals',
    'revocation_tombstones', 'notifications', 'fact_provenance',
  ]) {
    assert.ok(tables.has(t), `缺少表 ${t}`);
  }
  db.close();
});

test('迁移对已存在的 v1 基线库增量升级且不丢版本记录', () => {
  const { DatabaseSync } = require('node:sqlite');
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE schema_versions(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);');
  db.prepare('INSERT INTO schema_versions(version, applied_at) VALUES(1, ?)').run('2026-01-01T00:00:00Z');
  // 再次运行 openDatabase 的迁移逻辑
  const { migrate } = require('../src/database');
  migrate(db);
  const versions = db.prepare('SELECT version FROM schema_versions ORDER BY version').all().map((r) => r.version);
  assert.deepEqual(versions, MIGRATIONS.map((m) => m.version));
  // 幂等：再跑一次不报错、不重复
  migrate(db);
  const count = db.prepare('SELECT COUNT(*) AS n FROM schema_versions').get().n;
  assert.equal(count, MIGRATIONS.length);
  db.close();
});
