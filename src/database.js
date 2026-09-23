const { DatabaseSync } = require('node:sqlite');
const { mkdirSync } = require('node:fs');
const { dirname, resolve } = require('node:path');

// 版本化迁移。每个迁移必须向后兼容且可重复应用到一个全新数据库
// （schema_versions 记录已应用版本，已存在的版本不会重放）。
const MIGRATIONS = [
  {
    version: 1,
    description: '基线：迁移版本表',
    up: `
      CREATE TABLE IF NOT EXISTS schema_versions(
        version INTEGER PRIMARY KEY,
        description TEXT,
        applied_at TEXT NOT NULL
      );
    `,
  },
  {
    version: 2,
    description: '事件归并领域模型：机构、来源报文、标识映射、项目版本、审核、合并提案、通知发件箱、撤销墓碑',
    up: `
      -- 成员机构，HMAC 密钥用于校验来源报文签名
      CREATE TABLE IF NOT EXISTS organizations(
        org_id     TEXT PRIMARY KEY,
        name       TEXT NOT NULL,
        hmac_secret TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      -- 来源报文：原始字节、签名校验结果与处理状态（失败恢复依赖于此）
      CREATE TABLE IF NOT EXISTS source_messages(
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        org_id          TEXT NOT NULL,
        msg_id          TEXT NOT NULL,
        event_type      TEXT,
        op              TEXT NOT NULL,
        external_ref    TEXT,
        raw_payload     TEXT NOT NULL,
        signature       TEXT,
        signature_valid INTEGER NOT NULL DEFAULT 0,
        signature_error TEXT,
        received_at     TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'received',
                          -- received | processed | failed | dead_letter | rejected
        retry_count     INTEGER NOT NULL DEFAULT 0,
        last_error      TEXT,
        processed_at    TEXT,
        UNIQUE(org_id, msg_id),
        FOREIGN KEY(org_id) REFERENCES organizations(org_id)
      );
      CREATE INDEX IF NOT EXISTS idx_source_messages_status
        ON source_messages(status, received_at);
      CREATE INDEX IF NOT EXISTS idx_source_messages_org_ref
        ON source_messages(org_id, external_ref);

      -- 规范化项目实体（跨机构同一合作项目归并到同一 project_id）
      CREATE TABLE IF NOT EXISTS projects(
        project_id  TEXT PRIMARY KEY,
        primary_org TEXT NOT NULL,
        created_at  TEXT NOT NULL,
        FOREIGN KEY(primary_org) REFERENCES organizations(org_id)
      );

      -- 机构标识映射，按有效期维护；低置信度映射先 proposed，审核后 confirmed
      CREATE TABLE IF NOT EXISTS identifier_mappings(
        mapping_id       TEXT PRIMARY KEY,
        org_id           TEXT NOT NULL,
        external_ref     TEXT NOT NULL,
        project_id       TEXT NOT NULL,
        valid_from       TEXT NOT NULL,
        valid_to         TEXT,
        confidence       REAL NOT NULL DEFAULT 1,
        status           TEXT NOT NULL DEFAULT 'confirmed',
                           -- proposed | confirmed | deprecated
        created_message_id INTEGER,
        created_at       TEXT NOT NULL,
        FOREIGN KEY(org_id) REFERENCES organizations(org_id),
        FOREIGN KEY(project_id) REFERENCES projects(project_id),
        FOREIGN KEY(created_message_id) REFERENCES source_messages(id)
      );
      CREATE INDEX IF NOT EXISTS idx_mappings_lookup
        ON identifier_mappings(org_id, external_ref, status);
      CREATE INDEX IF NOT EXISTS idx_mappings_project
        ON identifier_mappings(project_id);
      -- 不变式：同一机构的同一标识在任一时刻最多只有一条生效(confirmed)映射。
      -- 历史映射以 deprecated 保留，可有多条。
      CREATE UNIQUE INDEX IF NOT EXISTS idx_mappings_unique_confirmed
        ON identifier_mappings(org_id, external_ref)
        WHERE status = 'confirmed';

      -- 项目版本链：重复/修订/撤销/拆分/合并均表现为可追溯版本
      CREATE TABLE IF NOT EXISTS project_versions(
        version_id               TEXT PRIMARY KEY,
        project_id               TEXT NOT NULL,
        version_no               INTEGER NOT NULL,
        change_type              TEXT NOT NULL,
                                   -- create | revise | revoke | split_out | merge
        event_type               TEXT,
        client_version           INTEGER,
        facts                    TEXT,
        source_org               TEXT NOT NULL,
        source_message_id        INTEGER NOT NULL,
        event_time               TEXT,
        recorded_at              TEXT NOT NULL,
        effective_at             TEXT,
        status                   TEXT NOT NULL,
                                   -- pending | effective | superseded | revoked | rejected
        superseded_by_version_id TEXT,
        revision_of_version_id   TEXT,
        related_project_id       TEXT,
        confidence               REAL NOT NULL DEFAULT 1,
        note                     TEXT,
        UNIQUE(project_id, version_no),
        FOREIGN KEY(project_id) REFERENCES projects(project_id),
        FOREIGN KEY(source_org) REFERENCES organizations(org_id),
        FOREIGN KEY(source_message_id) REFERENCES source_messages(id),
        FOREIGN KEY(superseded_by_version_id) REFERENCES project_versions(version_id),
        FOREIGN KEY(revision_of_version_id) REFERENCES project_versions(version_id),
        FOREIGN KEY(related_project_id) REFERENCES projects(project_id)
      );
      CREATE INDEX IF NOT EXISTS idx_versions_project_recorded
        ON project_versions(project_id, recorded_at);
      CREATE INDEX IF NOT EXISTS idx_versions_message
        ON project_versions(source_message_id);
      CREATE INDEX IF NOT EXISTS idx_versions_status
        ON project_versions(status);

      -- 版本状态流转历史：支撑"按当时已知信息重建时间线"（as-of 查询）。
      -- 即使乱序重放导致版本被延迟生效或事后撤销，每个状态的变更时刻都可追溯。
      CREATE TABLE IF NOT EXISTS version_status_history(
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        version_id     TEXT NOT NULL,
        from_status    TEXT,
        to_status      TEXT NOT NULL,
        changed_at     TEXT NOT NULL,
        reason         TEXT,
        FOREIGN KEY(version_id) REFERENCES project_versions(version_id)
      );
      CREATE INDEX IF NOT EXISTS idx_vsh_version_time
        ON version_status_history(version_id, changed_at);

      -- 字段级事实来源：机构只能纠正自己提交的字段。
      -- 跨机构合并时，来自双方的字段各自保留其来源机构。
      CREATE TABLE IF NOT EXISTS fact_provenance(
        project_id TEXT NOT NULL,
        fact_key   TEXT NOT NULL,
        org_id     TEXT NOT NULL,
        version_id TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(project_id, fact_key),
        FOREIGN KEY(project_id) REFERENCES projects(project_id),
        FOREIGN KEY(org_id) REFERENCES organizations(org_id)
      );

      -- 人工审核：低置信度时由人工决定实体关系（新建 / 关联到已有项目 / 拒绝）
      CREATE TABLE IF NOT EXISTS review_tasks(
        review_id          TEXT PRIMARY KEY,
        kind               TEXT NOT NULL DEFAULT 'identity',
        org_id             TEXT NOT NULL,
        external_ref       TEXT NOT NULL,
        project_id         TEXT,
        candidate_project_id TEXT,
        source_message_id  INTEGER NOT NULL,
        proposed_facts     TEXT,
        event_type         TEXT,
        event_time         TEXT,
        confidence         REAL NOT NULL,
        status             TEXT NOT NULL DEFAULT 'open',
                             -- open | approved | rejected
        decision           TEXT,
        decided_by         TEXT,
        decided_at         TEXT,
        resolved_version_id TEXT,
        lock_version       INTEGER NOT NULL DEFAULT 1,
        created_at         TEXT NOT NULL,
        FOREIGN KEY(org_id) REFERENCES organizations(org_id),
        FOREIGN KEY(project_id) REFERENCES projects(project_id),
        FOREIGN KEY(source_message_id) REFERENCES source_messages(id)
      );
      CREATE INDEX IF NOT EXISTS idx_reviews_status ON review_tasks(status);

      -- 跨机构合并提案，必须双方确认才生效
      CREATE TABLE IF NOT EXISTS merge_proposals(
        merge_id          TEXT PRIMARY KEY,
        from_project_id   TEXT NOT NULL,
        into_project_id   TEXT NOT NULL,
        proposed_by       TEXT NOT NULL,
        confirmer_org     TEXT NOT NULL,
        proposer_confirmed INTEGER NOT NULL DEFAULT 1,
        confirmer_confirmed INTEGER NOT NULL DEFAULT 0,
        status            TEXT NOT NULL DEFAULT 'pending',
                            -- pending | confirmed | rejected | cancelled
        source_message_id INTEGER,
        resulting_version_id TEXT,
        created_at        TEXT NOT NULL,
        decided_at        TEXT,
        FOREIGN KEY(from_project_id) REFERENCES projects(project_id),
        FOREIGN KEY(into_project_id) REFERENCES projects(project_id),
        FOREIGN KEY(proposed_by) REFERENCES organizations(org_id),
        FOREIGN KEY(confirmer_org) REFERENCES organizations(org_id),
        FOREIGN KEY(source_message_id) REFERENCES source_messages(id)
      );
      CREATE INDEX IF NOT EXISTS idx_merges_status ON merge_proposals(status);

      -- 撤销墓碑：撤销通知可能先于（被撤销的）补发消息到达。
      -- 记录某机构某标识下，<= up_to_version 的事件均已被撤销，
      -- 晚到的旧事件据此直接判定为 revoked，绝不复活项目。
      CREATE TABLE IF NOT EXISTS revocation_tombstones(
        tombstone_id      TEXT PRIMARY KEY,
        org_id            TEXT NOT NULL,
        external_ref      TEXT NOT NULL,
        up_to_version     INTEGER,
        up_to_event_time  TEXT,
        revoke_message_id INTEGER NOT NULL,
        recorded_at       TEXT NOT NULL,
        FOREIGN KEY(org_id) REFERENCES organizations(org_id),
        FOREIGN KEY(revoke_message_id) REFERENCES source_messages(id)
      );
      CREATE INDEX IF NOT EXISTS idx_tombstones_lookup
        ON revocation_tombstones(org_id, external_ref);

      -- 事务性发件箱：与业务版本在同一事务写入，投递端幂等
      CREATE TABLE IF NOT EXISTS notifications(
        notification_id   TEXT PRIMARY KEY,
        idempotency_key   TEXT NOT NULL UNIQUE,
        kind              TEXT NOT NULL,
        project_id        TEXT NOT NULL,
        version_id        TEXT NOT NULL,
        payload           TEXT NOT NULL,
        status            TEXT NOT NULL DEFAULT 'pending',
                            -- pending | delivered | failed
        attempts          INTEGER NOT NULL DEFAULT 0,
        created_at        TEXT NOT NULL,
        delivered_at      TEXT,
        last_error        TEXT,
        FOREIGN KEY(project_id) REFERENCES projects(project_id),
        FOREIGN KEY(version_id) REFERENCES project_versions(version_id)
      );
      CREATE INDEX IF NOT EXISTS idx_notifications_status
        ON notifications(status, created_at);
    `,
  },
];

function openDatabase(path = process.env.DATABASE_PATH || 'data/trade.sqlite3') {
  const resolved = path === ':memory:' ? ':memory:' : resolve(path);
  if (resolved !== ':memory:') {
    mkdirSync(dirname(resolved), { recursive: true });
  }
  const db = new DatabaseSync(resolved);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;');
  installTransactionHelper(db);
  migrate(db);
  return db;
}

// node:sqlite 的 DatabaseSync 没有 better-sqlite3 的 db.transaction()，
// 这里提供语义兼容的实现：外层用 BEGIN/COMMIT，嵌套用 SAVEPOINT。
function installTransactionHelper(db) {
  let depth = 0;
  db.transaction = (fn) => (...args) => {
    const level = depth;
    const savepoint = `sp_${level}_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
    if (level === 0) {
      db.exec('BEGIN IMMEDIATE');
    } else {
      db.exec(`SAVEPOINT ${savepoint}`);
    }
    depth += 1;
    try {
      const result = fn(...args);
      depth -= 1;
      if (level === 0) {
        db.exec('COMMIT');
      } else {
        db.exec(`RELEASE SAVEPOINT ${savepoint}`);
      }
      return result;
    } catch (err) {
      depth -= 1;
      if (level === 0) {
        db.exec('ROLLBACK');
      } else {
        db.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
        db.exec(`RELEASE SAVEPOINT ${savepoint}`);
      }
      throw err;
    }
  };
}

function migrate(db) {
  if (typeof db.transaction !== 'function') installTransactionHelper(db);
  db.exec(MIGRATIONS[0].up);
  // 兼容基线 v1 的旧表结构（只有 version, applied_at），补齐 description 列
  const cols = db.prepare('PRAGMA table_info(schema_versions)').all().map((c) => c.name);
  if (!cols.includes('description')) {
    db.exec('ALTER TABLE schema_versions ADD COLUMN description TEXT');
  }
  const applied = new Set(
    db.prepare('SELECT version FROM schema_versions').all().map((r) => r.version),
  );
  for (const migration of MIGRATIONS) {
    if (applied.has(migration.version)) continue;
    const apply = db.transaction(() => {
      db.exec(migration.up);
      db.prepare(
        'INSERT INTO schema_versions(version, description, applied_at) VALUES(?, ?, ?)',
      ).run(migration.version, migration.description, new Date().toISOString());
    });
    apply();
  }
}

module.exports = { openDatabase, migrate, MIGRATIONS };
