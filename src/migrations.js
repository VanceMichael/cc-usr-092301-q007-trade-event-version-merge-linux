'use strict';

// 数据库迁移：每个迁移带版本号、名称与 DDL，按顺序在事务内应用并登记到 schema_versions。
// 时间轴约定：
//   business_time  事件业务时间（合作方宣称的发生时间，可能乱序/晚到）
//   recorded_at    系统入库时间（查询“当时已知信息”使用）
//   effective_from/effective_to 版本在业务时间轴上的生效区间

const migrations = [
  {
    version: 1,
    name: 'baseline',
    sql: `
      CREATE TABLE IF NOT EXISTS schema_versions(
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
    `,
  },
  {
    version: 2,
    name: 'institutions_and_source_messages',
    sql: `
      CREATE TABLE institutions(
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        hmac_secret TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
      );

      -- 来源报文：原样保存报文与签名校验结果，失败/待补发的消息也能恢复
      CREATE TABLE source_messages(
        institution_code TEXT NOT NULL REFERENCES institutions(code),
        message_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        raw_payload TEXT NOT NULL,
        signature TEXT NOT NULL,
        signature_valid INTEGER NOT NULL,
        received_at TEXT NOT NULL,
        processed INTEGER NOT NULL DEFAULT 0,
        process_error TEXT,
        PRIMARY KEY (institution_code, message_id)
      );
      CREATE INDEX idx_source_messages_received
        ON source_messages(received_at);

      -- 每次事件投递尝试的痕迹（重放/重复可追溯，但不会产生新版本）
      CREATE TABLE event_ingest_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        institution_code TEXT NOT NULL,
        message_id TEXT,
        result TEXT NOT NULL,
        detail TEXT,
        received_at TEXT NOT NULL
      );
      CREATE INDEX idx_event_ingest_log_event
        ON event_ingest_log(event_id, institution_code);
    `,
  },
  {
    version: 3,
    name: 'projects_mappings_events_versions',
    sql: `
      CREATE TABLE projects(
        project_id TEXT PRIMARY KEY,
        display_name TEXT,
        owner_institution TEXT REFERENCES institutions(code),
        status TEXT NOT NULL DEFAULT 'active'
          CHECK (status IN ('active','merged','discarded')),
        merged_into_project TEXT REFERENCES projects(project_id),
        created_at TEXT NOT NULL
      );

      -- 机构标识映射：同一机构外部号在任意时刻只有一条有效映射，历史区间保留
      CREATE TABLE identifier_mappings(
        mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_code TEXT NOT NULL REFERENCES institutions(code),
        external_ref TEXT NOT NULL,
        project_id TEXT NOT NULL REFERENCES projects(project_id),
        valid_from TEXT NOT NULL,
        valid_to TEXT,
        confidence REAL NOT NULL DEFAULT 1,
        source_event_id TEXT,
        created_at TEXT NOT NULL
      );
      CREATE UNIQUE INDEX ux_identifier_mapping_current
        ON identifier_mappings(institution_code, external_ref)
        WHERE valid_to IS NULL;
      CREATE INDEX idx_identifier_mapping_project
        ON identifier_mappings(project_id);

      CREATE TABLE events(
        event_id TEXT PRIMARY KEY,
        institution_code TEXT NOT NULL REFERENCES institutions(code),
        message_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        business_time TEXT NOT NULL,
        received_at TEXT NOT NULL,
        external_ref TEXT,
        project_id TEXT REFERENCES projects(project_id),
        payload_json TEXT NOT NULL,
        confidence REAL NOT NULL DEFAULT 1
      );

      -- 版本谱系：重复不产生版本；修订使旧版本 superseded；撤销置 cancelled；
      -- 拆分/合并通过 version_links 连接，全部可追溯
      CREATE TABLE event_versions(
        version_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id TEXT NOT NULL REFERENCES projects(project_id),
        version_seq INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN
          ('fact','revision','cancel','split','merge')),
        event_id TEXT REFERENCES events(event_id),
        parent_version_id INTEGER REFERENCES event_versions(version_id),
        status TEXT NOT NULL CHECK (status IN
          ('pending_review','active','superseded','cancelled','rejected')),
        effective_from TEXT NOT NULL,
        effective_to TEXT,
        payload_json TEXT NOT NULL,
        fact_institution TEXT NOT NULL REFERENCES institutions(code),
        needs_review INTEGER NOT NULL DEFAULT 0,
        recorded_at TEXT NOT NULL,
        UNIQUE(project_id, version_seq)
      );
      CREATE INDEX idx_event_versions_effective
        ON event_versions(project_id, effective_from, effective_to);
      CREATE INDEX idx_event_versions_event ON event_versions(event_id);

      CREATE TABLE version_links(
        link_id INTEGER PRIMARY KEY AUTOINCREMENT,
        version_id INTEGER NOT NULL REFERENCES event_versions(version_id),
        linked_version_id INTEGER NOT NULL REFERENCES event_versions(version_id),
        relation TEXT NOT NULL CHECK (relation IN
          ('split_child','merge_parent','revision_base','cancel_base')),
        UNIQUE(version_id, linked_version_id, relation)
      );
      CREATE INDEX idx_version_links_linked ON version_links(linked_version_id);

      -- 前向引用：撤销/修订先到、原始报文晚到时暂存，目标出现后按业务时间补做
      CREATE TABLE pending_commands(
        command_id INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_code TEXT NOT NULL REFERENCES institutions(code),
        target_external_ref TEXT NOT NULL,
        command_type TEXT NOT NULL CHECK (command_type IN ('revision','cancel','split','merge')),
        event_id TEXT NOT NULL UNIQUE,
        business_time TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        message_id TEXT,
        created_at TEXT NOT NULL
      );
      CREATE INDEX idx_pending_commands_lookup
        ON pending_commands(institution_code, target_external_ref, business_time);
    `,
  },
  {
    version: 4,
    name: 'review_and_merge_workflow',
    sql: `
      -- 低置信度实体关系：人工审核决定，决策带乐观锁（并发冲突时 409）
      CREATE TABLE review_tasks(
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id TEXT NOT NULL REFERENCES projects(project_id),
        version_id INTEGER REFERENCES event_versions(version_id),
        mapping_id INTEGER REFERENCES identifier_mappings(mapping_id),
        reason TEXT NOT NULL,
        proposed_institution TEXT,
        proposed_ref TEXT,
        confidence REAL,
        status TEXT NOT NULL DEFAULT 'open'
          CHECK (status IN ('open','approved','rejected')),
        created_at TEXT NOT NULL,
        decided_at TEXT,
        decided_by TEXT,
        decision_note TEXT
      );
      CREATE INDEX idx_review_tasks_status ON review_tasks(status);

      -- 跨机构合并：双方确认后才落版本；同机构合并直接生效
      CREATE TABLE merge_proposals(
        merge_id INTEGER PRIMARY KEY AUTOINCREMENT,
        surviving_project_id TEXT NOT NULL REFERENCES projects(project_id),
        absorbed_project_id TEXT NOT NULL REFERENCES projects(project_id),
        institution_a TEXT NOT NULL REFERENCES institutions(code),
        institution_b TEXT NOT NULL REFERENCES institutions(code),
        status TEXT NOT NULL CHECK (status IN
          ('pending_counterpart','confirmed','rejected','cancelled')),
        applied_version_id INTEGER REFERENCES event_versions(version_id),
        initiating_event_id TEXT,
        business_time TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE UNIQUE INDEX ux_merge_proposal_open
        ON merge_proposals(surviving_project_id, absorbed_project_id)
        WHERE status IN ('pending_counterpart','confirmed');

      CREATE TABLE merge_confirmations(
        merge_id INTEGER NOT NULL REFERENCES merge_proposals(merge_id),
        institution_code TEXT NOT NULL REFERENCES institutions(code),
        confirmed_at TEXT NOT NULL,
        PRIMARY KEY (merge_id, institution_code)
      );
    `,
  },
  {
    version: 5,
    name: 'notification_outbox',
    sql: `
      -- 发件箱：版本生效时入队；dedupe_key 保证任何重放都不会重复通知
      CREATE TABLE notification_outbox(
        outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
        dedupe_key TEXT NOT NULL UNIQUE,
        project_id TEXT NOT NULL,
        version_id INTEGER,
        notification_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
          CHECK (status IN ('pending','delivered','failed')),
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        visible_after TEXT NOT NULL,
        created_at TEXT NOT NULL,
        delivered_at TEXT
      );
      CREATE INDEX idx_notification_outbox_drain
        ON notification_outbox(status, visible_after);

      -- 投递事实表：与下游“已交付”一一对应，UNIQUE 兜底幂等
      CREATE TABLE notification_deliveries(
        dedupe_key TEXT PRIMARY KEY,
        outbox_id INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        delivered_at TEXT NOT NULL
      );
    `,
  },
];

module.exports = { migrations };
