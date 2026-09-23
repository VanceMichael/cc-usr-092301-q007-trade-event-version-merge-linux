'use strict';

const { verifySignature } = require('./sig');

const REVIEW_THRESHOLD = 0.8;
const BACKOFF_MS = [100, 500, 2000, 10000];

class ServiceError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function createEventService(db, options = {}) {
  const reviewThreshold = options.reviewThreshold ?? REVIEW_THRESHOLD;
  // 可注入时钟（测试用来确定性回放 recorded_at）
  const clock = options.now || (() => new Date());
  const nowIso = () => clock().toISOString();

  // ---------- 基础 ----------
  const all = (sql, ...params) => db.prepare(sql).all(...params);
  const get = (sql, ...params) => db.prepare(sql).get(...params);
  const run = (sql, ...params) => db.prepare(sql).run(...params);

  let txDepth = 0;
  function withTx(fn) {
    if (txDepth > 0) return fn(); // 支持事务嵌套（如接入事务内触发前向引用补做）
    db.exec('BEGIN IMMEDIATE');
    txDepth++;
    try {
      const result = fn();
      db.exec('COMMIT');
      return result;
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    } finally {
      txDepth--;
    }
  }

  function logIngest(eventId, institution, messageId, result, detail) {
    run(
      `INSERT INTO event_ingest_log(event_id, institution_code, message_id, result, detail, received_at)
       VALUES(?,?,?,?,?,?)`,
      eventId ?? null, institution, messageId ?? null, result, detail ?? null, nowIso(),
    );
  }

  // ---------- 机构 ----------
  function registerInstitution({ code, name, secret }) {
    if (!code || !name || !secret) throw new ServiceError(400, 'ERR_BAD_PAYLOAD', 'code/name/secret 必填');
    run(
      `INSERT INTO institutions(code, name, hmac_secret, active, created_at)
       VALUES(?,?,?,1,?)
       ON CONFLICT(code) DO UPDATE SET name=excluded.name, hmac_secret=excluded.hmac_secret, active=1`,
      code, name, secret, nowIso(),
    );
    return getInstitution(code);
  }
  const getInstitution = (code) => get('SELECT * FROM institutions WHERE code=? AND active=1', code);

  // ---------- 项目与标识映射 ----------
  function createProject(ownerInstitution, displayName) {
    let projectId;
    do {
      projectId = `prj_${cryptoRandom()}`;
    } while (get('SELECT 1 FROM projects WHERE project_id=?', projectId));
    run(
      `INSERT INTO projects(project_id, display_name, owner_institution, status, created_at)
       VALUES(?,?,?, 'active', ?)`,
      projectId, displayName ?? null, ownerInstitution ?? null, nowIso(),
    );
    return projectId;
  }

  function cryptoRandom() {
    return require('node:crypto').randomBytes(8).toString('hex');
  }

  // at 时点有效的映射：valid_from <= at 且 (valid_to 为空或 valid_to > at)
  function resolveMapping(institutionCode, externalRef, at = nowIso()) {
    return get(
      `SELECT * FROM identifier_mappings
       WHERE institution_code=? AND external_ref=? AND valid_from<=?
         AND (valid_to IS NULL OR valid_to>?)
       ORDER BY valid_from DESC LIMIT 1`,
      institutionCode, externalRef, at, at,
    );
  }

  function currentMapping(institutionCode, externalRef) {
    return get(
      `SELECT * FROM identifier_mappings WHERE institution_code=? AND external_ref=? AND valid_to IS NULL`,
      institutionCode, externalRef,
    );
  }

  function createMapping(institutionCode, externalRef, projectId, validFrom, confidence = 1, sourceEventId = null) {
    const info = run(
      `INSERT INTO identifier_mappings(institution_code, external_ref, project_id, valid_from, confidence, source_event_id, created_at)
       VALUES(?,?,?,?,?,?,?)`,
      institutionCode, externalRef, projectId, validFrom, confidence, sourceEventId, nowIso(),
    );
    return get('SELECT * FROM identifier_mappings WHERE mapping_id=?', info.lastInsertRowid);
  }

  // 机构维护映射：指向变化时自动关闭旧映射（新区间从 validFrom 起）
  function upsertMapping(institutionCode, externalRef, projectId, { validFrom = nowIso(), confidence = 1 } = {}) {
    if (!getInstitution(institutionCode)) throw new ServiceError(404, 'ERR_INSTITUTION_UNKNOWN', '机构不存在');
    if (!get('SELECT 1 FROM projects WHERE project_id=?', projectId)) {
      throw new ServiceError(404, 'ERR_UNKNOWN_PROJECT', '目标项目不存在');
    }
    return withTx(() => {
      const existing = currentMapping(institutionCode, externalRef);
      if (existing) {
        if (existing.project_id === projectId) return existing;
        run('UPDATE identifier_mappings SET valid_to=? WHERE mapping_id=?', validFrom, existing.mapping_id);
      }
      return createMapping(institutionCode, externalRef, projectId, validFrom, confidence);
    });
  }

  // 机构只能维护自己的映射有效期
  function expireMapping(institutionCode, externalRef, validTo = nowIso()) {
    return withTx(() => {
      const existing = currentMapping(institutionCode, externalRef);
      if (!existing) throw new ServiceError(404, 'ERR_MAPPING_NOT_FOUND', '当前有效映射不存在');
      run('UPDATE identifier_mappings SET valid_to=? WHERE mapping_id=?', validTo, existing.mapping_id);
      return get('SELECT * FROM identifier_mappings WHERE mapping_id=?', existing.mapping_id);
    });
  }

  function listMappings(institutionCode, externalRef) {
    let sql = 'SELECT * FROM identifier_mappings WHERE 1=1';
    const params = [];
    if (institutionCode) { sql += ' AND institution_code=?'; params.push(institutionCode); }
    if (externalRef) { sql += ' AND external_ref=?'; params.push(externalRef); }
    sql += ' ORDER BY valid_from DESC';
    return all(sql, ...params);
  }

  // ---------- 版本 ----------
  function nextVersionSeq(projectId) {
    return get('SELECT COALESCE(MAX(version_seq),0)+1 AS n FROM event_versions WHERE project_id=?', projectId).n;
  }

  function insertVersion({
    projectId, kind, eventId, parentVersionId = null, status, effectiveFrom,
    payload, factInstitution, needsReview = false,
  }) {
    const seq = nextVersionSeq(projectId);
    const info = run(
      `INSERT INTO event_versions(project_id, version_seq, kind, event_id, parent_version_id,
         status, effective_from, payload_json, fact_institution, needs_review, recorded_at)
       VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
      projectId, seq, kind, eventId ?? null, parentVersionId, status, effectiveFrom,
      JSON.stringify(payload ?? {}), factInstitution, needsReview ? 1 : 0, nowIso(),
    );
    return get('SELECT * FROM event_versions WHERE version_id=?', info.lastInsertRowid);
  }

  function addLink(versionId, linkedVersionId, relation) {
    run(
      `INSERT OR IGNORE INTO version_links(version_id, linked_version_id, relation) VALUES(?,?,?)`,
      versionId, linkedVersionId, relation,
    );
  }

  function enqueueNotification(projectId, versionId, type, payload) {
    const dedupeKey = versionId ? `version:${versionId}` : `${type}:${projectId}:${cryptoHash(JSON.stringify(payload))}`;
    run(
      `INSERT OR IGNORE INTO notification_outbox(dedupe_key, project_id, version_id, notification_type, payload_json, visible_after, created_at)
       VALUES(?,?,?,?,?,?,?)`,
      dedupeKey, projectId, versionId ?? null, type, JSON.stringify(payload ?? {}), nowIso(), nowIso(),
    );
  }

  function cryptoHash(s) {
    return require('node:crypto').createHash('sha256').update(s).digest('hex').slice(0, 16);
  }

  // 版本生效时入队下游通知；pending_review 版本不通知
  function notifyEffectiveVersion(version, extra = {}) {
    enqueueNotification(version.project_id, version.version_id, `version_${version.kind}`, {
      project_id: version.project_id,
      version_id: version.version_id,
      kind: version.kind,
      effective_from: version.effective_from,
      payload: JSON.parse(version.payload_json),
      ...extra,
    });
  }

  function openReview(projectId, versionId, mappingId, reason, { proposedInstitution = null, proposedRef = null, confidence = null } = {}) {
    const info = run(
      `INSERT INTO review_tasks(project_id, version_id, mapping_id, reason, proposed_institution, proposed_ref, confidence, status, created_at)
       VALUES(?,?,?,?,?,?,?, 'open', ?)`,
      projectId, versionId ?? null, mappingId ?? null, reason, proposedInstitution, proposedRef, confidence, nowIso(),
    );
    enqueueNotification(projectId, null, 'review_required', {
      review_id: info.lastInsertRowid, project_id: projectId, reason, confidence,
    });
    return info.lastInsertRowid;
  }

  // 找到本机构在项目上当前生效的自有事实版本（机构只能纠正自己提交的事实）
  function ownActiveVersion(projectId, institutionCode) {
    return get(
      `SELECT * FROM event_versions
       WHERE project_id=? AND fact_institution=? AND status='active'
       ORDER BY effective_from DESC, version_seq DESC LIMIT 1`,
      projectId, institutionCode,
    );
  }

  // ---------- 实体解析与 fact ----------
  // 返回 {projectId, version, reviewId?}；低置信度新号进入待审
  function applyFact(event, institution, receivedAt) {
    const bt = event.business_time;
    const confidence = numberOf(event.confidence, 1);
    let mapping = event.external_ref ? currentMapping(institution.code, event.external_ref) : null;
    let projectId;
    let needsReview = false;

    if (mapping) {
      projectId = mapping.project_id;
      // 项目归属尚在人审：新事实同样挂起
      const pending = get(
        `SELECT 1 FROM event_versions WHERE project_id=? AND status='pending_review' LIMIT 1`,
        projectId,
      );
      if (pending) needsReview = true;
    } else {
      projectId = createProject(institution.code, event.payload?.name ?? event.external_ref ?? null);
      mapping = createMapping(institution.code, event.external_ref, projectId, bt, confidence, event.event_id);
      if (confidence < reviewThreshold) needsReview = true;
    }

    const version = insertVersion({
      projectId,
      kind: 'fact',
      eventId: event.event_id,
      status: needsReview ? 'pending_review' : 'active',
      effectiveFrom: bt,
      payload: event.payload ?? {},
      factInstitution: institution.code,
      needsReview,
    });

    let reviewId = null;
    if (needsReview) {
      reviewId = openReview(projectId, version.version_id, mapping.mapping_id, 'low_confidence_entity', {
        proposedInstitution: institution.code,
        proposedRef: event.external_ref,
        confidence,
      });
    } else {
      notifyEffectiveVersion(version);
    }
    return { outcome: needsReview ? 'pending_review' : 'accepted', projectId, versionId: version.version_id, reviewId };
  }

  // ---------- 修订 / 撤销（含前向引用） ----------
  function queueCommand(event, institution, commandType) {
    run(
      `INSERT INTO pending_commands(institution_code, target_external_ref, command_type, event_id, business_time, payload_json, message_id, created_at)
       VALUES(?,?,?,?,?,?,?,?)`,
      institution.code, event.target_ref, commandType, event.event_id, event.business_time,
      JSON.stringify(event), event.message_id ?? null, nowIso(),
    );
    return { outcome: 'awaiting_target', command_type: commandType };
  }

  // 机构只能纠正自己提交的事实：目标项目存在但本机构没有自有生效版本即拒绝
  function assertOwnFact(projectId, institutionCode, action) {
    const own = ownActiveVersion(projectId, institutionCode);
    if (own) return own;
    const foreign = get(
      "SELECT 1 FROM event_versions WHERE project_id=? AND status='active' LIMIT 1",
      projectId,
    );
    if (foreign) {
      throw new ServiceError(403, 'ERR_FOREIGN_FACT',
        `机构 ${institutionCode} 不能${action}其他机构提交的事实`);
    }
    return null;
  }

  // 机构只能纠正自己提交的事实：
  // 先用本机构映射定位项目；本机构无映射时，识别该编号是否为他机构所有
  function resolveOwnFactProject(institutionCode, targetRef, action) {
    const mapping = currentMapping(institutionCode, targetRef);
    if (mapping) {
      return { mapping, base: assertOwnFact(mapping.project_id, institutionCode, action) };
    }
    const foreign = get(
      'SELECT * FROM identifier_mappings WHERE external_ref=? AND valid_to IS NULL LIMIT 1',
      targetRef,
    );
    if (foreign && foreign.institution_code !== institutionCode) {
      throw new ServiceError(403, 'ERR_FOREIGN_FACT',
        `编号 ${targetRef} 属于机构 ${foreign.institution_code}，${institutionCode} 不能${action}其他机构提交的事实`);
    }
    return { mapping: null, base: null };
  }

  function applyRevision(event, institution) {
    const { mapping, base } = resolveOwnFactProject(institution.code, event.target_ref, '修订');
    if (!mapping) return queueCommand(event, institution, 'revision');
    return { outcome: 'accepted', ...commitRevisionOrCancel(event, institution, mapping.project_id, base, 'revision') };
  }

  function applyCancel(event, institution) {
    const { mapping, base } = resolveOwnFactProject(institution.code, event.target_ref, '撤销');
    if (!mapping) return queueCommand(event, institution, 'cancel');
    return { outcome: 'accepted', ...commitRevisionOrCancel(event, institution, mapping.project_id, base, 'cancel') };
  }

  function commitRevisionOrCancel(event, institution, projectId, base, kind) {
    run('UPDATE event_versions SET status=?, effective_to=? WHERE version_id=?', 'superseded', event.business_time, base.version_id);
    const version = insertVersion({
      projectId,
      kind,
      eventId: event.event_id,
      parentVersionId: base.version_id,
      status: 'active',
      effectiveFrom: event.business_time,
      payload: kind === 'cancel' ? { cancelled: true, reason: event.payload?.reason ?? null } : (event.payload ?? {}),
      factInstitution: institution.code,
    });
    addLink(version.version_id, base.version_id, kind === 'cancel' ? 'cancel_base' : 'revision_base');
    notifyEffectiveVersion(version);
    return { projectId, versionId: version.version_id };
  }

  // ---------- 拆分 ----------
  function applySplit(event, institution) {
    const { mapping, base } = resolveOwnFactProject(institution.code, event.target_ref, '拆分');
    if (!mapping) return queueCommand(event, institution, 'split');
    if (!Array.isArray(event.children) || event.children.length === 0) {
      throw new ServiceError(400, 'ERR_BAD_PAYLOAD', 'split 需要 children 列表');
    }
    const projectId = mapping.project_id;
    run('UPDATE event_versions SET status=?, effective_to=? WHERE version_id=?', 'superseded', event.business_time, base.version_id);
    const splitVersion = insertVersion({
      projectId,
      kind: 'split',
      eventId: event.event_id,
      parentVersionId: base.version_id,
      status: 'active',
      effectiveFrom: event.business_time,
      payload: { split_into: event.children.map((c) => c.external_ref), note: event.payload?.note ?? null },
      factInstitution: institution.code,
    });
    const children = event.children.map((child) => {
      const childProjectId = createProject(institution.code, child.payload?.name ?? child.external_ref);
      const childMapping = createMapping(institution.code, child.external_ref, childProjectId, event.business_time, 1, event.event_id);
      const childVersion = insertVersion({
        projectId: childProjectId,
        kind: 'fact',
        eventId: null,
        status: 'active',
        effectiveFrom: event.business_time,
        payload: child.payload ?? {},
        factInstitution: institution.code,
      });
      addLink(splitVersion.version_id, childVersion.version_id, 'split_child');
      notifyEffectiveVersion(childVersion, { split_from: projectId });
      return { external_ref: child.external_ref, project_id: childProjectId, version_id: childVersion.version_id, mapping_id: childMapping.mapping_id };
    });
    notifyEffectiveVersion(splitVersion, { children });
    return { outcome: 'accepted', projectId, versionId: splitVersion.version_id, children };
  }

  // ---------- 合并（跨机构须双方确认） ----------
  function findOpenMerge(survivingId, absorbedId) {
    return get(
      `SELECT * FROM merge_proposals
       WHERE status IN ('pending_counterpart','confirmed')
         AND ((surviving_project_id=? AND absorbed_project_id=?)
           OR (surviving_project_id=? AND absorbed_project_id=?))`,
      survivingId, absorbedId, absorbedId, survivingId,
    );
  }

  function applyMergeEvent(event, institution) {
    if (!event.surviving_ref || !event.absorbed_ref || !event.target_institution) {
      throw new ServiceError(400, 'ERR_BAD_PAYLOAD', 'merge 需要 surviving_ref / absorbed_ref / target_institution');
    }
    const survivingMapping = currentMapping(institution.code, event.surviving_ref);
    const counterpartMapping = currentMapping(event.target_institution, event.absorbed_ref);
    if (!survivingMapping || !counterpartMapping) {
      // 任一方实体尚未上报：前向暂存，项目出现后补做
      run(
        `INSERT INTO pending_commands(institution_code, target_external_ref, command_type, event_id, business_time, payload_json, message_id, created_at)
         VALUES(?,?, 'merge', ?,?,?,?,?)
         ON CONFLICT(event_id) DO NOTHING`,
        institution.code, event.absorbed_ref, event.event_id, event.business_time,
        JSON.stringify(event), event.message_id ?? null, nowIso(),
      );
      return { outcome: 'awaiting_target', command_type: 'merge' };
    }
    const survivingId = survivingMapping.project_id;
    const absorbedId = counterpartMapping.project_id;
    if (survivingId === absorbedId) throw new ServiceError(409, 'ERR_SAME_PROJECT', '两个编号已指向同一项目');

    const reverse = findOpenMerge(survivingId, absorbedId);
    if (reverse) {
      // 对方此前已发起：本次直接视为对方确认；已确认则幂等返回
      if (reverse.status === 'confirmed') {
        return { outcome: 'merged', merge_id: reverse.merge_id, project_id: reverse.surviving_project_id, version_id: reverse.applied_version_id };
      }
      confirmMerge(reverse.merge_id, institution.code);
      const refreshed = getMerge(reverse.merge_id);
      return { outcome: refreshed.status === 'confirmed' ? 'merged' : 'pending_counterpart', merge_id: reverse.merge_id, project_id: survivingId };
    }

    const sameInstitution = institution.code === event.target_institution;
    const info = run(
      `INSERT INTO merge_proposals(surviving_project_id, absorbed_project_id, institution_a, institution_b,
         status, initiating_event_id, business_time, payload_json, created_at, updated_at)
       VALUES(?,?,?,?,?,?,?,?,?,?)`,
      survivingId, absorbedId, institution.code, event.target_institution,
      sameInstitution ? 'confirmed' : 'pending_counterpart',
      event.event_id, event.business_time, JSON.stringify(event.payload ?? {}), nowIso(), nowIso(),
    );
    const mergeId = info.lastInsertRowid;
    run('INSERT OR IGNORE INTO merge_confirmations(merge_id, institution_code, confirmed_at) VALUES(?,?,?)',
      mergeId, institution.code, nowIso());
    if (sameInstitution) {
      const versionId = commitMerge(mergeId);
      return { outcome: 'merged', merge_id: mergeId, project_id: survivingId, version_id: versionId };
    }
    return { outcome: 'pending_counterpart', merge_id: mergeId, project_id: survivingId };
  }

  function getMerge(mergeId) {
    const proposal = get('SELECT * FROM merge_proposals WHERE merge_id=?', mergeId);
    if (!proposal) throw new ServiceError(404, 'ERR_MERGE_NOT_FOUND', '合并提案不存在');
    proposal.confirmations = all('SELECT institution_code, confirmed_at FROM merge_confirmations WHERE merge_id=?', mergeId);
    return proposal;
  }

  function confirmMerge(mergeId, institutionCode) {
    return withTx(() => {
      const proposal = get('SELECT * FROM merge_proposals WHERE merge_id=?', mergeId);
      if (!proposal) throw new ServiceError(404, 'ERR_MERGE_NOT_FOUND', '合并提案不存在');
      if (proposal.status !== 'pending_counterpart') {
        throw new ServiceError(409, 'ERR_MERGE_STATE', `提案状态为 ${proposal.status}，不能确认`, { status: proposal.status });
      }
      if (![proposal.institution_a, proposal.institution_b].includes(institutionCode)) {
        throw new ServiceError(403, 'ERR_NOT_PARTY', '只有合并双方机构可以确认');
      }
      run('INSERT OR IGNORE INTO merge_confirmations(merge_id, institution_code, confirmed_at) VALUES(?,?,?)',
        mergeId, institutionCode, nowIso());
      run('UPDATE merge_proposals SET updated_at=? WHERE merge_id=?', nowIso(), mergeId);
      const confirmed = all('SELECT institution_code FROM merge_confirmations WHERE merge_id=?', mergeId)
        .map((r) => r.institution_code);
      const both = [proposal.institution_a, proposal.institution_b].every((c) => confirmed.includes(c));
      if (!both) return getMerge(mergeId);
      const versionId = commitMerge(mergeId);
      return { ...getMerge(mergeId), applied_version_id: versionId };
    });
  }

  function rejectMerge(mergeId, institutionCode, note) {
    return withTx(() => {
      const proposal = get('SELECT * FROM merge_proposals WHERE merge_id=?', mergeId);
      if (!proposal) throw new ServiceError(404, 'ERR_MERGE_NOT_FOUND', '合并提案不存在');
      if (![proposal.institution_a, proposal.institution_b].includes(institutionCode)) {
        throw new ServiceError(403, 'ERR_NOT_PARTY', '只有合并双方机构可以拒绝');
      }
      if (proposal.status !== 'pending_counterpart') {
        throw new ServiceError(409, 'ERR_MERGE_STATE', `提案状态为 ${proposal.status}，不能拒绝`);
      }
      run("UPDATE merge_proposals SET status='rejected', updated_at=? WHERE merge_id=?", nowIso(), mergeId);
      return getMerge(mergeId);
    });
  }

  // 双方齐后落合并版本（事务内调用）
  function commitMerge(mergeId) {
    const proposal = get('SELECT * FROM merge_proposals WHERE merge_id=?', mergeId);
    const survivingId = proposal.surviving_project_id;
    const absorbedId = proposal.absorbed_project_id;
    const bt = proposal.business_time;

    const absorbedVersions = all(
      "SELECT * FROM event_versions WHERE project_id=? AND status='active'", absorbedId,
    );
    for (const v of absorbedVersions) {
      run('UPDATE event_versions SET status=?, effective_to=? WHERE version_id=?', 'superseded', bt, v.version_id);
    }
    const mergeVersion = insertVersion({
      projectId: survivingId,
      kind: 'merge',
      eventId: proposal.initiating_event_id,
      status: 'active',
      effectiveFrom: bt,
      payload: { ...JSON.parse(proposal.payload_json), absorbed_project_id: absorbedId },
      factInstitution: proposal.institution_a,
    });
    for (const v of absorbedVersions) addLink(mergeVersion.version_id, v.version_id, 'merge_parent');

    run("UPDATE projects SET status='merged', merged_into_project=? WHERE project_id=?", survivingId, absorbedId);
    // 被吸收项目的全部当前映射：关闭旧区间，同号转指存续项目
    const mappings = all('SELECT * FROM identifier_mappings WHERE project_id=? AND valid_to IS NULL', absorbedId);
    for (const m of mappings) {
      run('UPDATE identifier_mappings SET valid_to=? WHERE mapping_id=?', bt, m.mapping_id);
      createMapping(m.institution_code, m.external_ref, survivingId, bt, m.confidence, null);
    }
    run("UPDATE merge_proposals SET status='confirmed', applied_version_id=?, updated_at=? WHERE merge_id=?",
      mergeVersion.version_id, nowIso(), mergeId);
    notifyEffectiveVersion(mergeVersion, { absorbed_project_id: absorbedId });
    return mergeVersion.version_id;
  }

  // ---------- 前向引用补做（补发晚到场景） ----------
  function drainPendingCommands() {
    const applied = [];
    let pending = all(
      `SELECT * FROM pending_commands ORDER BY business_time ASC, command_id ASC`,
    );
    for (const cmd of pending) {
      try {
        const event = JSON.parse(cmd.payload_json);
        event.event_id = cmd.event_id;
        event.message_id = cmd.message_id;
        const institution = getInstitution(cmd.institution_code);
        if (!institution) continue;
        let result = null;
        if (['revision', 'cancel', 'split'].includes(cmd.command_type)
          && currentMapping(cmd.institution_code, cmd.target_external_ref)) {
          // 映射已出现：正常补做；若事实属他机构，apply 会抛 ERR_FOREIGN_FACT 并被下面清理
          const fn = cmd.command_type === 'revision' ? applyRevision
            : cmd.command_type === 'cancel' ? applyCancel : applySplit;
          result = fn(event, institution);
        } else if (cmd.command_type === 'merge') {
          const other = event.target_institution;
          if (currentMapping(cmd.institution_code, event.surviving_ref) && currentMapping(other, event.absorbed_ref)) {
            result = applyMergeEvent(event, institution);
          }
        }
        if (result && result.outcome !== 'awaiting_target') {
          run('DELETE FROM pending_commands WHERE command_id=?', cmd.command_id);
          applied.push({ command_id: cmd.command_id, type: cmd.command_type, result });
        }
      } catch (error) {
        // 他方事实不可纠正：命令永久无法生效，登记后移除；其它失败保留待下次恢复
        if (error.code === 'ERR_FOREIGN_FACT') {
          run('DELETE FROM pending_commands WHERE command_id=?', cmd.command_id);
        }
      }
    }
    return applied;
  }

  // ---------- 接入 ----------
  function dispatch(event, institution) {
    switch (event.action) {
      case 'fact': return applyFact(event, institution);
      case 'revision': return applyRevision(event, institution);
      case 'cancel': return applyCancel(event, institution);
      case 'split': return applySplit(event, institution);
      case 'merge': return applyMergeEvent(event, institution);
      default: throw new ServiceError(400, 'ERR_BAD_ACTION', `未知 action: ${event.action}`);
    }
  }

  const VALID_ACTIONS = new Set(['fact', 'revision', 'cancel', 'split', 'merge']);

  function validateEvent(event) {
    if (!event || typeof event !== 'object') throw new ServiceError(400, 'ERR_BAD_PAYLOAD', '报文不是 JSON 对象');
    if (!event.event_id) throw new ServiceError(400, 'ERR_BAD_PAYLOAD', '缺少 event_id');
    if (!VALID_ACTIONS.has(event.action)) throw new ServiceError(400, 'ERR_BAD_ACTION', '缺少或非法 action');
    if (!event.business_time || Number.isNaN(Date.parse(event.business_time))) {
      throw new ServiceError(400, 'ERR_BAD_PAYLOAD', '缺少或非法 business_time');
    }
    if (event.action !== 'merge' && !event.external_ref && !event.target_ref) {
      throw new ServiceError(400, 'ERR_BAD_PAYLOAD', '缺少 external_ref/target_ref');
    }
  }

  function processValidEvent(event, institution, messageId) {
    if (get('SELECT 1 FROM events WHERE event_id=?', event.event_id)) {
      const v = get('SELECT * FROM event_versions WHERE event_id=?', event.event_id);
      // 事件已处理过：本次转发（新 message_id）不再产生版本，报文直接标记已处理
      if (messageId != null) {
        run('UPDATE source_messages SET processed=1, process_error=NULL WHERE institution_code=? AND message_id=?',
          institution.code, messageId);
      }
      return { outcome: 'duplicate', projectId: v?.project_id, versionId: v?.version_id };
    }
    return withTx(() => {
      run(
        `INSERT INTO events(event_id, institution_code, message_id, event_type, business_time, received_at, external_ref, payload_json, confidence)
         VALUES(?,?,?,?,?,?,?,?,?)`,
        event.event_id, institution.code, messageId ?? null, event.action, event.business_time,
        nowIso(), event.external_ref ?? event.target_ref ?? null,
        JSON.stringify(event.payload ?? {}), numberOf(event.confidence, 1),
      );
      event.message_id ??= messageId ?? event.event_id;
      const result = dispatch(event, institution);
      drainPendingCommands();
      return result;
    });
  }

  function numberOf(value, fallback) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
  }

  function ingest(institutionCode, rawBody, signatureHeader, messageId, receivedAt = nowIso()) {
    const institution = getInstitution(institutionCode);
    if (!institution) throw new ServiceError(401, 'ERR_INSTITUTION_UNKNOWN', '未知或停用机构');
    const signatureValid = verifySignature(institution.hmac_secret, rawBody, signatureHeader);

    const existing = get('SELECT * FROM source_messages WHERE institution_code=? AND message_id=?',
      institutionCode, messageId ?? null);
    if (existing?.processed) {
      const parsed = safeParse(existing.raw_payload);
      logIngest(parsed?.event_id ?? null, institutionCode, messageId, 'duplicate', 'message already processed');
      const v = parsed ? get('SELECT * FROM event_versions WHERE event_id=?', parsed.event_id) : null;
      return { outcome: 'duplicate', projectId: v?.project_id, versionId: v?.version_id };
    }

    let parsed = null;
    try { parsed = JSON.parse(rawBody); } catch { parsed = null; }

    run(
      `INSERT INTO source_messages(institution_code, message_id, event_type, raw_payload, signature, signature_valid, received_at, processed, process_error)
       VALUES(?,?,?,?,?,?,?,0,?)
       ON CONFLICT(institution_code, message_id) DO UPDATE SET
         raw_payload=excluded.raw_payload, signature=excluded.signature,
         signature_valid=excluded.signature_valid, received_at=excluded.received_at,
         process_error=NULL`,
      institutionCode, messageId ?? null, parsed?.action ?? 'unknown', rawBody,
      signatureHeader ?? '', signatureValid ? 1 : 0, receivedAt,
      signatureValid ? (parsed ? null : 'malformed json') : 'invalid signature',
    );

    if (!signatureValid) {
      logIngest(parsed?.event_id ?? null, institutionCode, messageId, 'signature_invalid', null);
      throw new ServiceError(401, 'ERR_BAD_SIGNATURE', '签名校验失败，报文已留存待补发');
    }
    if (!parsed) {
      logIngest(null, institutionCode, messageId, 'malformed', null);
      throw new ServiceError(400, 'ERR_BAD_PAYLOAD', '报文不是合法 JSON（已留存）');
    }
    validateEvent(parsed);

    try {
      const result = processValidEvent(parsed, institution, messageId);
      run('UPDATE source_messages SET processed=1, process_error=NULL WHERE institution_code=? AND message_id=?',
        institutionCode, messageId ?? null);
      logIngest(parsed.event_id, institutionCode, messageId, result.outcome, null);
      return result;
    } catch (error) {
      if (error.status >= 400 && error.status < 500 && error.code !== 'ERR_BAD_SIGNATURE') {
        // 确定性业务拒绝（如越权纠正、非法动作）：终态，不再进入恢复队列
        run('UPDATE source_messages SET processed=1, process_error=? WHERE institution_code=? AND message_id=?',
          `rejected: ${error.code} ${error.message}`, institutionCode, messageId ?? null);
      } else {
        run('UPDATE source_messages SET processed=0, process_error=? WHERE institution_code=? AND message_id=?',
          String(error.message ?? error), institutionCode, messageId ?? null);
      }
      logIngest(parsed.event_id, institutionCode, messageId, 'error', String(error.message ?? error));
      throw error;
    }
  }

  function safeParse(s) { try { return JSON.parse(s); } catch { return null; } }

  // ---------- 失败消息恢复 ----------
  function listFailedMessages() {
    return all('SELECT institution_code, message_id, event_type, signature_valid, received_at, process_error FROM source_messages WHERE processed=0 ORDER BY received_at');
  }

  function recoverFailed() {
    const rows = all('SELECT * FROM source_messages WHERE processed=0 AND signature_valid=1 ORDER BY received_at');
    const recovered = [];
    const failed = [];
    for (const row of rows) {
      const event = safeParse(row.raw_payload);
      const institution = getInstitution(row.institution_code);
      if (!event || !institution) { failed.push({ message_id: row.message_id, reason: 'malformed json' }); continue; }
      try {
        validateEvent(event);
        const result = processValidEvent(event, institution, row.message_id);
        run('UPDATE source_messages SET processed=1, process_error=NULL WHERE institution_code=? AND message_id=?',
          row.institution_code, row.message_id);
        recovered.push({ message_id: row.message_id, result });
      } catch (error) {
        run('UPDATE source_messages SET process_error=? WHERE institution_code=? AND message_id=?',
          String(error.message ?? error), row.institution_code, row.message_id);
        failed.push({ message_id: row.message_id, reason: String(error.message ?? error) });
      }
    }
    return { recovered, failed };
  }

  // ---------- 人工审核（乐观锁，并发冲突报错） ----------
  function decideReview(reviewId, { decision, reviewer, note = null, target = null }) {
    return withTx(() => {
      const task = get('SELECT * FROM review_tasks WHERE review_id=?', reviewId);
      if (!task) throw new ServiceError(404, 'ERR_REVIEW_NOT_FOUND', '审核任务不存在');
      if (task.status !== 'open') {
        throw new ServiceError(409, 'ERR_REVIEW_NOT_OPEN', `任务已被 ${task.decided_by} 于 ${task.decided_at} 处理`, {
          status: task.status, decided_by: task.decided_by,
        });
      }
      const version = get('SELECT * FROM event_versions WHERE version_id=?', task.version_id);
      if (!version || version.status !== 'pending_review') {
        throw new ServiceError(409, 'ERR_REVIEW_STATE', '对应版本不处于待审状态');
      }

      if (decision === 'rejected') {
        const info = run(
          `UPDATE review_tasks SET status='rejected', decided_at=?, decided_by=?, decision_note=?
           WHERE review_id=? AND status='open'`, nowIso(), reviewer, note, reviewId);
        if (info.changes === 0) throw new ServiceError(409, 'ERR_REVIEW_CONFLICT', '审核并发冲突，请刷新后重试');
        // 该待定实体上的其余待审版本一并驳回，项目作废
        run("UPDATE event_versions SET status='rejected' WHERE project_id=? AND status='pending_review'", task.project_id);
        run("UPDATE review_tasks SET status='rejected', decided_at=?, decided_by=?, decision_note='cascaded by related rejection' WHERE project_id=? AND status='open'",
          nowIso(), reviewer, task.project_id);
        run("UPDATE projects SET status='discarded' WHERE project_id=?", task.project_id);
        run('UPDATE identifier_mappings SET valid_to=? WHERE mapping_id=? AND valid_to IS NULL', nowIso(), task.mapping_id);
        return { review_id: reviewId, decision: 'rejected', project_id: task.project_id };
      }

      if (decision !== 'approved') throw new ServiceError(400, 'ERR_BAD_DECISION', 'decision 必须是 approved/rejected');

      let targetProjectId = task.project_id;
      // 审核员裁决实体关系：target 指向既有项目则并入，否则作为独立新实体
      if (target && (target.project_id || target.external_ref)) {
        if (target.project_id) {
          targetProjectId = target.project_id;
        } else {
          const m = currentMapping(target.institution ?? task.proposed_institution, target.external_ref);
          if (!m) throw new ServiceError(404, 'ERR_TARGET_NOT_FOUND', '裁决目标编号无有效映射');
          targetProjectId = m.project_id;
        }
        if (!get('SELECT 1 FROM projects WHERE project_id=?', targetProjectId)) {
          throw new ServiceError(404, 'ERR_TARGET_NOT_FOUND', '裁决目标项目不存在');
        }
      }

      const info = run(
        `UPDATE review_tasks SET status='approved', decided_at=?, decided_by=?, decision_note=?
         WHERE review_id=? AND status='open'`, nowIso(), reviewer, note, reviewId);
      if (info.changes === 0) throw new ServiceError(409, 'ERR_REVIEW_CONFLICT', '审核并发冲突，请刷新后重试');

      if (targetProjectId !== task.project_id) {
        // 并入既有实体：迁移该临时项目全部待审版本与映射，
        // 实体关系一经裁决，其余待审事实级联通过
        const pendings = all('SELECT version_id FROM event_versions WHERE project_id=? AND status=?',
          task.project_id, 'pending_review');
        for (const p of pendings) {
          const seq = nextVersionSeq(targetProjectId);
          run('UPDATE event_versions SET project_id=?, version_seq=?, status=? WHERE version_id=?',
            targetProjectId, seq, 'active', p.version_id);
        }
        run("UPDATE review_tasks SET status='approved', decided_at=?, decided_by=?, decision_note='cascaded by entity merge decision' WHERE project_id=? AND status='open'",
          nowIso(), reviewer, task.project_id);
        run('UPDATE identifier_mappings SET project_id=? WHERE mapping_id=?', targetProjectId, task.mapping_id);
        run("UPDATE projects SET status='discarded' WHERE project_id=?", task.project_id);
        for (const p of pendings) {
          const v = get('SELECT * FROM event_versions WHERE version_id=?', p.version_id);
          notifyEffectiveVersion(v, { merged_into_by_review: true });
        }
      } else {
        run("UPDATE event_versions SET status='active' WHERE version_id=?", task.version_id);
        notifyEffectiveVersion({ ...version, status: 'active' });
      }
      drainPendingCommands();
      return { review_id: reviewId, decision: 'approved', project_id: targetProjectId };
    });
  }

  function listReviews(status = 'open') {
    return all(
      `SELECT r.*, m.institution_code, m.external_ref
       FROM review_tasks r LEFT JOIN identifier_mappings m ON m.mapping_id=r.mapping_id
       WHERE r.status=? ORDER BY r.created_at`, status,
    );
  }

  // ---------- 时间线（按当时已知信息重建） ----------
  function loadVersionKnowledge() {
    const rows = all(
      `SELECT v.*, rt.status AS review_status, rt.decided_at AS review_decided_at
       FROM event_versions v LEFT JOIN review_tasks rt ON rt.version_id=v.version_id`,
    );
    const byId = new Map();
    for (const v of rows) {
      const known = v.needs_review ? (v.review_status === 'approved' || v.review_status === 'rejected' ? v.review_decided_at : null) : v.recorded_at;
      byId.set(v.version_id, { v, known });
    }
    // 后继版本：parent 链（修订/撤销/拆分直接取代父版本）与合并吸收（link）
    for (const v of rows) {
      if (v.parent_version_id) {
        const base = byId.get(v.parent_version_id);
        const child = byId.get(v.version_id);
        if (base && child) {
          const relation = v.kind === 'cancel' ? 'cancel_base'
            : v.kind === 'merge' ? 'merge_parent' : 'revision_base';
          (base.successors ||= []).push({ successor: child, relation });
        }
      }
    }
    for (const link of all('SELECT * FROM version_links')) {
      const child = byId.get(link.version_id);      // 新版本（revision/cancel/split/merge）
      const base = byId.get(link.linked_version_id); // 被取代版本
      if (child && base && ['revision_base', 'cancel_base', 'merge_parent'].includes(link.relation)) {
        (base.successors ||= []).push({ successor: child, relation: link.relation });
      }
    }
    return byId;
  }

  function stateAt(entry, asOf) {
    const v = entry.v;
    if (v.needs_review) {
      if (v.review_status === 'rejected' && entry.known && entry.known <= asOf) return 'rejected';
      if (!entry.known || entry.known > asOf) return 'pending_review';
    }
    for (const s of entry.successors ?? []) {
      if (s.successor.known && s.successor.known <= asOf) {
        if (s.relation === 'cancel_base') return 'cancelled';
        if (s.relation === 'merge_parent') return 'merged';
        return 'superseded';
      }
    }
    return 'active';
  }

  function serializeVersion(entry, state) {
    const v = entry.v;
    return {
      version_id: v.version_id,
      project_id: v.project_id,
      version_seq: v.version_seq,
      kind: v.kind,
      event_id: v.event_id,
      state,
      effective_from: v.effective_from,
      effective_to: v.effective_to,
      fact_institution: v.fact_institution,
      payload: safeParse(v.payload_json) ?? {},
      recorded_at: v.recorded_at,
      known_at: entry.known,
    };
  }

  function timeline(projectId, asOf = nowIso()) {
    const project = get('SELECT * FROM projects WHERE project_id=?', projectId);
    if (!project) throw new ServiceError(404, 'ERR_UNKNOWN_PROJECT', '项目不存在');
    const knowledge = loadVersionKnowledge();
    const versions = [];
    for (const entry of knowledge.values()) {
      if (entry.v.project_id !== projectId) continue;
      if (entry.v.recorded_at > asOf) continue;
      versions.push(serializeVersion(entry, stateAt(entry, asOf)));
    }
    // 入边：被其它项目合并（merge 版本挂在存续项目上）
    const incoming = all(
      `SELECT mv.* FROM version_links l
         JOIN event_versions mv ON mv.version_id=l.version_id
       WHERE l.relation='merge_parent' AND l.linked_version_id IN
         (SELECT version_id FROM event_versions WHERE project_id=?) AND mv.recorded_at<=?`,
      projectId, asOf,
    );
    for (const mv of incoming) versions.push({
      version_id: mv.version_id, project_id: mv.project_id, kind: 'merge_in',
      state: 'merged', effective_from: mv.effective_from, recorded_at: mv.recorded_at,
      payload: safeParse(mv.payload_json) ?? {}, fact_institution: mv.fact_institution,
    });
    // 出边：拆分子项 / 合并吸收项
    const links = all(
      `SELECT l.relation, l.version_id, l.linked_version_id,
              pv.project_id AS parent_project, cv.project_id AS child_project
       FROM version_links l
         JOIN event_versions pv ON pv.version_id=l.version_id
         JOIN event_versions cv ON cv.version_id=l.linked_version_id
       WHERE pv.project_id=? OR cv.project_id=?`, projectId, projectId,
    );
    versions.sort((a, b) => a.effective_from.localeCompare(b.effective_from) || a.version_id - b.version_id);
    return { project, versions, links: links.map((l) => ({ relation: l.relation, version_id: l.version_id, linked_version_id: l.linked_version_id })) };
  }

  // ---------- 统计（仅已生效版本） ----------
  function stats(asOf = nowIso()) {
    const knowledge = loadVersionKnowledge();
    const byCategory = new Map();
    const activeProjects = new Set();
    for (const entry of knowledge.values()) {
      if (!entry.known || entry.known > asOf || entry.v.recorded_at > asOf) continue;
      if (stateAt(entry, asOf) !== 'active') continue;
      // 只有事实/修订承载业务数值；撤销、拆分父版本、合并等墓碑不计
      if (entry.v.kind !== 'fact' && entry.v.kind !== 'revision') continue;
      activeProjects.add(entry.v.project_id);
      const payload = safeParse(entry.v.payload_json) ?? {};
      if (!payload.category) continue;
      const bucket = byCategory.get(payload.category) ?? { category: payload.category, count: 0, total_amount: 0 };
      bucket.count += 1;
      if (Number.isFinite(Number(payload.amount))) bucket.total_amount += Number(payload.amount);
      byCategory.set(payload.category, bucket);
    }
    return { as_of: asOf, active_projects: activeProjects.size, categories: [...byCategory.values()] };
  }

  // ---------- 通知发件箱（重放幂等） ----------
  async function drainNotifications(deliver, { batchSize = 50, asOf = nowIso(), maxAttempts = 5 } = {}) {
    const rows = all(
      `SELECT * FROM notification_outbox
       WHERE status IN ('pending','failed') AND visible_after<=?
       ORDER BY outbox_id LIMIT ?`, asOf, batchSize,
    );
    let delivered = 0;
    const stillFailed = [];
    for (const row of rows) {
      const already = get('SELECT 1 FROM notification_deliveries WHERE dedupe_key=?', row.dedupe_key);
      if (already) {
        run("UPDATE notification_outbox SET status='delivered', delivered_at=COALESCE(delivered_at,?) WHERE outbox_id=?",
          nowIso(), row.outbox_id);
        continue;
      }
      try {
        await deliver(JSON.parse(row.payload_json), row);
        db.exec('BEGIN IMMEDIATE');
        try {
          run(
            `INSERT INTO notification_deliveries(dedupe_key, outbox_id, payload_json, delivered_at) VALUES(?,?,?,?)`,
            row.dedupe_key, row.outbox_id, row.payload_json, nowIso(),
          );
          run("UPDATE notification_outbox SET status='delivered', delivered_at=?, attempts=attempts+1, last_error=NULL WHERE outbox_id=?",
            nowIso(), row.outbox_id);
          db.exec('COMMIT');
        } catch (e) { db.exec('ROLLBACK'); throw e; }
        delivered += 1;
      } catch (error) {
        const attempts = row.attempts + 1;
        const backoff = BACKOFF_MS[Math.min(attempts, BACKOFF_MS.length - 1)];
        const visible = new Date(Date.now() + backoff).toISOString();
        run(
          `UPDATE notification_outbox SET attempts=?, last_error=?, visible_after=?,
             status=CASE WHEN ?>=? THEN 'failed' ELSE 'pending' END WHERE outbox_id=?`,
          attempts, String(error.message ?? error), visible, attempts, maxAttempts, row.outbox_id,
        );
        stillFailed.push({ outbox_id: row.outbox_id, attempts, error: String(error.message ?? error) });
      }
    }
    return { scanned: rows.length, delivered, failed: stillFailed };
  }

  // 手动恢复：强制让所有未交付通知立即可投（忽略退避窗口）
  function requeueStaleNotifications() {
    const info = run(
      `UPDATE notification_outbox SET status='pending', visible_after=?
       WHERE status IN ('pending','failed') AND COALESCE(delivered_at,'')=''`,
      nowIso(),
    );
    return { requeued: info.changes };
  }

  function listDeliveries() {
    return all('SELECT dedupe_key, outbox_id, delivered_at FROM notification_deliveries ORDER BY delivered_at');
  }

  function listOutbox(status) {
    if (status) return all('SELECT * FROM notification_outbox WHERE status=? ORDER BY outbox_id', status);
    return all('SELECT * FROM notification_outbox ORDER BY outbox_id');
  }

  return {
    registerInstitution,
    ingest, recoverFailed, listFailedMessages,
    upsertMapping, expireMapping, listMappings, resolveMapping,
    decideReview, listReviews,
    getMerge, confirmMerge, rejectMerge,
    timeline, stats,
    drainNotifications, requeueStaleNotifications, listDeliveries, listOutbox,
    errors: { ServiceError },
  };
}

module.exports = { createEventService, ServiceError, REVIEW_THRESHOLD };
