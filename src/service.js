const { newId, parseJson, timingSafeEqualHex, hmac } = require('./util');

const CONFIDENCE_THRESHOLD = 0.85;
const MAX_RETRIES = 5;

class HttpError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

// 并发审核冲突
class ReviewConflictError extends HttpError {
  constructor(current) {
    super(409, 'review_conflict', '审核任务已被其他操作更新，请刷新后重试', { current });
  }
}

class EventService {
  constructor(db, { notify, confidenceThreshold = CONFIDENCE_THRESHOLD } = {}) {
    this.db = db;
    // notify(notification row) 为下游投递器；默认不落网（测试中替换）
    this.notify = notify || (async () => {});
    this.threshold = confidenceThreshold;
    this._lastStamp = '';
    this._prepareStatements();
  }

  // 单调时间戳：保证同一进程内连续产生的 recorded_at 严格递增，
  // 使版本排序与 as-of 重建在毫秒内连写多个版本时仍确定可测。
  _now() {
    let iso = new Date().toISOString();
    if (iso <= this._lastStamp) {
      iso = new Date(new Date(this._lastStamp).getTime() + 1).toISOString();
    }
    this._lastStamp = iso;
    return iso;
  }

  _prepareStatements() {
    const db = this.db;
    this.stmts = {
      getOrg: db.prepare('SELECT * FROM organizations WHERE org_id = ?'),
      insertOrg: db.prepare(
        'INSERT INTO organizations(org_id, name, hmac_secret, created_at) VALUES(?, ?, ?, ?)',
      ),
      getMessage: db.prepare('SELECT * FROM source_messages WHERE org_id = ? AND msg_id = ?'),
      insertMessage: db.prepare(
        `INSERT INTO source_messages
           (org_id, msg_id, event_type, op, external_ref, raw_payload, signature,
            signature_valid, signature_error, received_at, status)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received')`,
      ),
      markProcessed: db.prepare(
        "UPDATE source_messages SET status='processed', processed_at=?, last_error=NULL WHERE id=?",
      ),
      markFailed: db.prepare(
        'UPDATE source_messages SET status=?, retry_count=retry_count+1, last_error=? WHERE id=?',
      ),
      nextVersionNo: db.prepare(
        'SELECT COALESCE(MAX(version_no),0)+1 AS n FROM project_versions WHERE project_id = ?',
      ),
      insertVersion: db.prepare(
        `INSERT INTO project_versions(version_id, project_id, version_no, change_type,
            event_type, client_version, facts, source_org, source_message_id, event_time,
            recorded_at, effective_at, status, revision_of_version_id, related_project_id,
            confidence, note)
         VALUES(@version_id, @project_id, @version_no, @change_type, @event_type,
            @client_version, @facts, @source_org, @source_message_id, @event_time,
            @recorded_at, @effective_at, @status, @revision_of_version_id, @related_project_id,
            @confidence, @note)`,
      ),
      insertHistory: db.prepare(
        'INSERT INTO version_status_history(version_id, from_status, to_status, changed_at, reason) VALUES(?, ?, ?, ?, ?)',
      ),
      currentVersion: db.prepare(
        `SELECT * FROM project_versions
         WHERE project_id=? AND status IN ('effective','pending')
         ORDER BY version_no DESC LIMIT 1`,
      ),
      getVersion: db.prepare('SELECT * FROM project_versions WHERE version_id=?'),
      versionsOfMessage: db.prepare('SELECT * FROM project_versions WHERE source_message_id=?'),
      insertProject: db.prepare(
        'INSERT INTO projects(project_id, primary_org, created_at) VALUES(?, ?, ?)',
      ),
      getProject: db.prepare('SELECT * FROM projects WHERE project_id=?'),
      activeMapping: db.prepare(
        `SELECT * FROM identifier_mappings
         WHERE org_id=? AND external_ref=? AND status='confirmed'
         ORDER BY valid_from DESC, created_at DESC LIMIT 1`,
      ),
      anyConfirmedMapping: db.prepare(
        "SELECT * FROM identifier_mappings WHERE org_id=? AND external_ref=? AND status='confirmed' LIMIT 1",
      ),
      latestMappingAnyStatus: db.prepare(
        `SELECT * FROM identifier_mappings WHERE org_id=? AND external_ref=?
         ORDER BY created_at DESC LIMIT 1`,
      ),
      insertMapping: db.prepare(
        `INSERT INTO identifier_mappings(mapping_id, org_id, external_ref, project_id,
            valid_from, valid_to, confidence, status, created_message_id, created_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ),
      deprecateMappings: db.prepare(
        `UPDATE identifier_mappings SET status='deprecated', valid_to=?
         WHERE org_id=? AND external_ref=? AND status='confirmed' AND (valid_to IS NULL OR valid_to>?)`,
      ),
      insertReview: db.prepare(
        `INSERT INTO review_tasks(review_id, kind, org_id, external_ref, project_id,
            candidate_project_id, source_message_id, proposed_facts, event_type, event_time,
            confidence, status, lock_version, created_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 1, ?)`,
      ),
      getReview: db.prepare('SELECT * FROM review_tasks WHERE review_id=?'),
      openReviewForMessage: db.prepare(
        "SELECT * FROM review_tasks WHERE source_message_id=? AND status='open'",
      ),
      openReviewForKey: db.prepare(
        `SELECT * FROM review_tasks WHERE org_id=? AND external_ref=? AND status='open'
         ORDER BY created_at DESC LIMIT 1`,
      ),
      listOpenReviews: db.prepare('SELECT * FROM review_tasks WHERE status=? ORDER BY created_at'),
      closeReviewsForProject: db.prepare(
        `UPDATE review_tasks SET status='rejected', decision='revoked', decided_by='system',
            decided_at=?, lock_version=lock_version+1
         WHERE project_id=? AND status='open'`,
      ),
      resolveReview: db.prepare(
        `UPDATE review_tasks SET status=?, decision=?, decided_by=?, decided_at=?,
            resolved_version_id=?, candidate_project_id=?, lock_version=lock_version+1
         WHERE review_id=? AND lock_version=? AND status='open'`,
      ),
      insertTombstone: db.prepare(
        `INSERT INTO revocation_tombstones(tombstone_id, org_id, external_ref,
            up_to_version, up_to_event_time, revoke_message_id, recorded_at)
         VALUES(?, ?, ?, ?, ?, ?, ?)`,
      ),
      latestTombstone: db.prepare(
        `SELECT * FROM revocation_tombstones
         WHERE org_id=? AND external_ref=? ORDER BY recorded_at DESC LIMIT 1`,
      ),
      insertMerge: db.prepare(
        `INSERT INTO merge_proposals(merge_id, from_project_id, into_project_id, proposed_by,
            confirmer_org, proposer_confirmed, confirmer_confirmed, status, source_message_id,
            created_at)
         VALUES(?, ?, ?, ?, ?, 1, 0, 'pending', ?, ?)`,
      ),
      getMerge: db.prepare('SELECT * FROM merge_proposals WHERE merge_id=?'),
      confirmMerge: db.prepare(
        `UPDATE merge_proposals SET confirmer_confirmed=1, status='confirmed', decided_at=?
         WHERE merge_id=? AND status='pending'`),
      rejectMerge: db.prepare(
        `UPDATE merge_proposals SET status='rejected', decided_at=? WHERE merge_id=? AND status='pending'`,
      ),
      setMergeResult: db.prepare(
        'UPDATE merge_proposals SET resulting_version_id=? WHERE merge_id=?'),
      upsertFactOwner: db.prepare(
        `INSERT INTO fact_provenance(project_id, fact_key, org_id, version_id, updated_at)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(project_id, fact_key) DO UPDATE SET
           org_id=excluded.org_id, version_id=excluded.version_id, updated_at=excluded.updated_at`,
      ),
      getFactOwner: db.prepare(
        'SELECT org_id FROM fact_provenance WHERE project_id=? AND fact_key=?',
      ),
      factOwnersForProject: db.prepare(
        'SELECT fact_key, org_id FROM fact_provenance WHERE project_id=?',
      ),
      insertNotification: db.prepare(
        `INSERT INTO notifications(notification_id, idempotency_key, kind, project_id,
            version_id, payload, created_at)
         VALUES(?, ?, ?, ?, ?, ?, ?)`,
      ),
      pendingNotifications: db.prepare(
        "SELECT * FROM notifications WHERE status='pending' ORDER BY created_at LIMIT ?"),
      markNotification: db.prepare(
        'UPDATE notifications SET status=?, attempts=attempts+1, delivered_at=?, last_error=? WHERE notification_id=?'),
      listFailedMessages: db.prepare(
        "SELECT * FROM source_messages WHERE status IN ('failed','dead_letter') ORDER BY received_at"),
      getMessageById: db.prepare('SELECT * FROM source_messages WHERE id=?'),
    };
  }

  // ---------- 机构与密钥 ----------

  registerOrganization(orgId, name, hmacSecret) {
    const existing = this.stmts.getOrg.get(orgId);
    if (existing) return existing;
    this.stmts.insertOrg.run(orgId, name, hmacSecret, this._now());
    return this.stmts.getOrg.get(orgId);
  }

  // ---------- 接收与签名 ----------

  // 校验签名并保存来源报文。无论后续业务处理成败，原始报文与校验结果都留痕。
  // 幂等：同一 (org_id, msg_id) 重放返回既有记录，不重复处理、不重复通知。
  async ingest(orgId, msgId, rawBody, signature) {
    const org = this.stmts.getOrg.get(orgId);
    if (!org) throw new HttpError(404, 'unknown_org', `未知机构: ${orgId}`);

    const existing = this.stmts.getMessage.get(orgId, msgId);
    if (existing) {
      return { duplicate: true, message: existing, result: this._resultForMessage(existing) };
    }

    let payload;
    let signatureValid = 0;
    let signatureError = null;
    try {
      payload = JSON.parse(rawBody);
    } catch (e) {
      const msg = this._saveMessage(org, msgId, null, 'unknown', null, rawBody, signature, 0, 'invalid_json');
      this._failMessage(msg, new Error('非法 JSON 报文'), /*fatal*/ true);
      throw new HttpError(400, 'invalid_json', '报文不是合法 JSON');
    }

    const expected = hmac(org.hmac_secret, rawBody);
    if (!signature) {
      signatureValid = 0;
      signatureError = 'missing_signature';
    } else if (timingSafeEqualHex(signature, expected)) {
      signatureValid = 1;
    } else {
      signatureValid = 0;
      signatureError = 'bad_signature';
    }

    const op = payload.op || 'upsert';
    const externalRef = payload.ref || null;
    const msg = this._saveMessage(
      org, msgId, payload.eventType || null, op, externalRef,
      rawBody, signature || null, signatureValid, signatureError,
    );

    if (!signatureValid) {
      // 签名无效：报文留痕但拒绝进入业务处理
      this.stmts.markFailed.run('rejected', signatureError, msg.id);
      msg.status = 'rejected';
      throw new HttpError(401, signatureError, '签名校验失败', { messageId: msg.id });
    }

    // 业务处理失败不丢消息：标记 failed 等待恢复重处理
    let result;
    try {
      result = this._processMessage(msg, payload);
    } catch (e) {
      if (e instanceof HttpError && e.status < 500) {
        // 4xx 属于确定性拒绝（如越权），重试无意义，进入死信
        this.stmts.markFailed.run('dead_letter', `${e.code}: ${e.message}`, msg.id);
        msg.status = 'dead_letter';
        throw e;
      }
      this._failMessage(msg, e);
      throw e;
    }
    this.stmts.markProcessed.run(this._now(), msg.id);
    msg.status = 'processed';
    return { duplicate: false, message: this.stmts.getMessageById.get(msg.id), result };
  }

  _saveMessage(org, msgId, eventType, op, externalRef, rawBody, signature, valid, error) {
    const info = this.stmts.insertMessage.run(
      org.org_id, msgId, eventType, op, externalRef, rawBody,
      signature, valid, error, this._now(),
    );
    return this.stmts.getMessageById.get(Number(info.lastInsertRowid));
  }

  _failMessage(msg, err, fatal = false) {
    const nextRetry = msg.retry_count + 1;
    const status = fatal || nextRetry > MAX_RETRIES ? 'dead_letter' : 'failed';
    this.stmts.markFailed.run(status, String(err && err.message || err), msg.id);
  }

  _resultForMessage(msg) {
    if (msg.status === 'processed') {
      const versions = this.stmts.versionsOfMessage.all(msg.id);
      const review = this.stmts.openReviewForMessage.get(msg.id);
      return { versions, review: review || null };
    }
    return null;
  }

  // ---------- 失败消息恢复 ----------

  // 重处理失败消息：基于已保存的原始报文重新执行业务归并。
  // 已处理/已产生版本/已发通知的消息重放天然幂等（不会产生新版本或重复通知）。
  async recoverMessage(messageId) {
    const msg = this.stmts.getMessageById.get(messageId);
    if (!msg) throw new HttpError(404, 'not_found', '消息不存在');
    if (!['failed', 'dead_letter'].includes(msg.status)) {
      throw new HttpError(409, 'not_recoverable', `消息状态 ${msg.status} 不可恢复`);
    }
    const payload = JSON.parse(msg.raw_payload);
    const result = this._processMessage(msg, payload);
    this.stmts.markProcessed.run(this._now(), msg.id);
    return { message: this.stmts.getMessageById.get(messageId), result };
  }

  listFailedMessages() {
    return this.stmts.listFailedMessages.all();
  }

  // ---------- 核心归并 ----------

  _processMessage(msg, payload) {
    const op = msg.op;
    switch (op) {
      case 'revoke':
        return this._handleRevoke(msg, payload);
      case 'split':
        return this._handleSplit(msg, payload);
      case 'merge':
        return this._handleMergeRequest(msg, payload);
      case 'upsert':
      case 'revise':
      case 'create':
        return this._handleUpsert(msg, payload);
      default:
        throw new HttpError(400, 'unknown_op', `未知操作类型: ${op}`);
    }
  }

  _resolveProject(msg, payload) {
    const ref = payload.ref;
    if (!ref) throw new HttpError(400, 'missing_ref', '报文缺少项目标识 ref');
    const asOf = payload.eventTime || msg.received_at;
    // 消息路由按机构"当前确认"的映射归并（迟到、补发与恢复消息同样适用）；
    // 映射的有效期下界用于 as-of 重建，不用于消息路由，否则迟到消息会被误判为无主。
    const mapping = this.stmts.activeMapping.get(msg.org_id, ref);
    if (!mapping) return { projectId: null, asOf };
    return { projectId: mapping.project_id, asOf, mapping };
  }

  _handleUpsert(msg, payload) {
    const ref = payload.ref;
    if (!ref) throw new HttpError(400, 'missing_ref', '报文缺少项目标识 ref');

    const facts = { ...(payload.facts || {}) };
    const confidence = typeof payload.confidence === 'number' ? payload.confidence : 1;
    const eventTime = payload.eventTime || null;
    const clientVersion = payload.version || null;

    // 乱序：撤销可能先到。命中墓碑（版本号或事件时间落在撤销范围内）的
    // 旧事件直接登记为 revoked，绝不复活项目；撤销之后的新事件不受影响。
    const tombstone = this._findApplicableTombstone(msg.org_id, ref, clientVersion, eventTime);
    if (tombstone) {
      return this._runInTx(() => {
        let projectId = this._projectForTombstoned(msg.org_id, ref);
        let isNewProject = false;
        if (!projectId) {
          projectId = newId('prj');
          this.stmts.insertProject.run(projectId, msg.org_id, this._now());
          isNewProject = true;
        }
        // 建立指向该项目的映射，保证同一标识后续事件稳定路由到同一项目，
        // 而不是每次都新建项目；墓碑仍会拦截撤销范围内的旧事件。
        if (isNewProject) {
          this.stmts.insertMapping.run(
            newId('map'), msg.org_id, ref, projectId,
            eventTime || msg.received_at, null, 1, 'confirmed', msg.id, this._now());
        }
        const v = this._addVersion({
          projectId, msg, changeType: 'revoke', eventType: payload.eventType,
          clientVersion, facts, eventTime, status: 'revoked',
          note: '事件在到达前已被撤销（撤销先于补发到达）', confidence: 1,
        });
        return { suppressed: true, reason: 'already_revoked', projectId, versions: [v] };
      });
    }

    const { projectId } = this._resolveProject(msg, payload);

    if (!projectId) {
      // 该标识已有未决人工审核（低置信事件尚未确认身份）时，暂不处理，
      // 避免在审核决策前为同一标识再建第二个项目；审核落定后可经失败恢复自动归并。
      const openReview = this.stmts.openReviewForKey.get(msg.org_id, ref);
      if (openReview) {
        throw new HttpError(503, 'identity_review_pending',
          `标识 ${ref} 的身份审核 ${openReview.review_id} 尚未决定，等待审核后重试`);
      }
      // 乱序：修订先于创建到达。修订必须依附已有项目，此时创建事件可能在途，
      // 标记为可重试失败（不进死信），待创建到达后由失败消息恢复重新归并。
      if (msg.op === 'revise') {
        throw new HttpError(503, 'revision_before_create',
          `修订的标识 ${ref} 尚无归并项目，等待创建事件到达后重试`);
      }
      // 新标识：低置信度 => 人工审核决定实体关系
      if (confidence < this.threshold) {
        return this._runInTx(() => {
          const project = newId('prj');
          this.stmts.insertProject.run(project, msg.org_id, this._now());
          const candidates = this._candidateProjects(facts);
          const reviewId = newId('rev');
          this.stmts.insertReview.run(
            reviewId, 'identity', msg.org_id, ref, project,
            candidates[0] ? candidates[0].project_id : null,
            msg.id, JSON.stringify(facts), payload.eventType || null, eventTime,
            confidence, this._now(),
          );
          const v = this._addVersion({
            projectId: project, msg, changeType: 'create', eventType: payload.eventType,
            clientVersion, facts, eventTime, status: 'pending', confidence,
          });
          return { reviewRequired: true, reviewId, projectId: project, versions: [v] };
        });
      }
      return this._runInTx(() => this._createProject(msg, payload, facts, confidence, eventTime, clientVersion));
    }

    const current = this.stmts.currentVersion.get(projectId);

    if (!current || current.status === 'revoked') {
      // 项目已被整体撤销，又有撤销范围之外的合法新事件：开启新项目生命周期
      // （新项目 ID，旧映射置过期），已撤销项目的版本链仍完整可追溯。
      return this._runInTx(() => {
        this.stmts.deprecateMappings.run(this._now(), msg.org_id, ref, this._now());
        return this._createProject(msg, payload, facts, confidence, eventTime, clientVersion);
      });
    }

    // 越权：机构只能纠正自己提交的事实（字段级）。
    // 能解析到本项目说明该机构持有本项目的标识映射（含合并后重指向）；
    // 但它只能改自己拥有的字段，或新增尚无归属的字段，不得覆盖他方字段。
    this._assertCanCorrectFacts(projectId, facts, msg.org_id);

    // 修订 / 重复判定
    if (msg.op === 'create') {
      throw new HttpError(409, 'already_exists', `标识 ${ref} 已归并到项目 ${projectId}，重复创建被拒绝`);
    }

    // 字段级合并：以当前事实为底，仅用本报文提供的字段覆盖/增补，保留他方字段
    const currentFacts = current.facts ? parseJson(current.facts) : {};
    const effectiveFacts = { ...currentFacts, ...facts };

    const isDuplicate = this._isDuplicate(current, effectiveFacts, payload.eventType);
    return this._runInTx(() => {
      if (isDuplicate) {
        // 重复：不产生新版本（重放安全）
        return { duplicate: true, projectId, versions: [] };
      }
      const v = this._addVersion({
        projectId, msg, changeType: 'revise', eventType: payload.eventType,
        clientVersion, facts: effectiveFacts, eventTime, status: 'effective',
        revisionOf: current.version_id, confidence,
      });
      this._recordFactOwners(projectId, facts, msg.org_id, v.version_id);
      this._transition(current.version_id, 'effective', 'superseded', 'revised', this._now());
      this._emitNotification('project_revised', projectId, v, { orgId: msg.org_id, ref });
      return { revised: true, projectId, versions: [v] };
    });
  }

  _createProject(msg, payload, facts, confidence, eventTime, clientVersion, existingProjectId = null) {
    const projectId = existingProjectId || newId('prj');
    if (!existingProjectId) {
      this.stmts.insertProject.run(projectId, msg.org_id, this._now());
    }
    this.stmts.insertMapping.run(
      newId('map'), msg.org_id, payload.ref, projectId,
      eventTime || msg.received_at, null, confidence, 'confirmed', msg.id, this._now(),
    );
    const v = this._addVersion({
      projectId, msg, changeType: 'create', eventType: payload.eventType,
      clientVersion, facts, eventTime, status: 'effective', confidence,
    });
    this._recordFactOwners(projectId, facts, msg.org_id, v.version_id);
    this._emitNotification('project_created', projectId, v, { orgId: msg.org_id, ref: payload.ref });
    return { created: true, projectId, versions: [v] };
  }

  _isDuplicate(current, facts, eventType) {
    if (!current || current.event_type !== eventType) return false;
    const currentFacts = current.facts ? parseJson(current.facts) : {};
    const keys = new Set([...Object.keys(currentFacts), ...Object.keys(facts)]);
    for (const k of keys) {
      if (JSON.stringify(currentFacts[k]) !== JSON.stringify(facts[k])) return false;
    }
    return true;
  }

  _candidateProjects(facts) {
    // 基于简单特征（金额/伙伴/名称）给出候选，供人工审核参考
    return [];
  }

  _projectForTombstoned(orgId, ref) {
    // 撤销通常已把映射置为 deprecated，因此取该标识最近一次映射（任意状态）；
    // 若撤销先于任何事件到达，则没有映射可挂靠。
    const m = this.stmts.latestMappingAnyStatus.get(orgId, ref)
      || this.stmts.anyConfirmedMapping.get(orgId, ref);
    return m ? m.project_id : null;
  }

  _handleRevoke(msg, payload) {
    const ref = payload.ref;
    if (!ref) throw new HttpError(400, 'missing_ref', '撤销报文缺少 ref');
    const clientVersion = payload.version != null ? payload.version : null;
    const upTo = payload.upToVersion != null ? payload.upToVersion : clientVersion;
    const eventTime = payload.eventTime || null;

    return this._runInTx(() => {
      // 墓碑先落库：任何之后到达的、被撤销的补发消息都会被拦截
      this.stmts.insertTombstone.run(
        newId('tmb'), msg.org_id, ref, upTo, eventTime, msg.id, this._now(),
      );
      const mapping = this.stmts.activeMapping.get(msg.org_id, ref);
      let projectId = mapping ? mapping.project_id : null;
      // 低置信事件尚待审核时还没有 confirmed 映射，通过未决审核反查其项目
      if (!projectId) {
        const pendingReview = this.stmts.openReviewForKey.get(msg.org_id, ref);
        if (pendingReview) projectId = pendingReview.project_id;
      }
      let revokedVersion = null;

      if (projectId) {
        const current = this.stmts.currentVersion.get(projectId);
        if (current && current.status === 'effective' && this._inRevokeScope(current, upTo)) {
          revokedVersion = this._addVersion({
            projectId, msg, changeType: 'revoke', eventType: current.event_type,
            clientVersion: upTo, facts: parseJson(current.facts || '{}'), eventTime,
            status: 'revoked', confidence: 1, note: payload.reason || '机构撤销',
          });
          this._transition(current.version_id, 'effective', 'revoked', 'revoked', this._now());
          this.stmts.deprecateMappings.run(this._now(), msg.org_id, ref, this._now());
          this.stmts.closeReviewsForProject.run(this._now(), projectId);
          this._emitNotification('project_revoked', projectId, revokedVersion,
            { orgId: msg.org_id, ref, reason: payload.reason || null });
        } else if (current && current.status === 'pending' && this._inRevokeScope(current, upTo)) {
          // 乱序：低置信事件尚在人工审核，撤销先到——直接关闭审核
          revokedVersion = this._addVersion({
            projectId, msg, changeType: 'revoke', eventType: current.event_type,
            clientVersion: upTo, facts: parseJson(current.facts || '{}'), eventTime,
            status: 'revoked', confidence: 1, note: '待审事件被撤销',
          });
          this._transition(current.version_id, 'pending', 'revoked', 'revoked_before_review', this._now());
          this.stmts.closeReviewsForProject.run(this._now(), projectId);
        }
      } else {
        // 撤销先于创建到达：仅立墓碑（被撤销的补发到达时直接落 revoked）
        return { revoked: false, projectId: null, tombstoned: true, versions: [] };
      }
      return { revoked: !!revokedVersion, projectId, tombstoned: true, versions: revokedVersion ? [revokedVersion] : [] };
    });
  }

  _inRevokeScope(version, upTo) {
    if (upTo == null) return true;
    if (version.client_version == null) return true; // 无版本号时按整条标识撤销
    return version.client_version <= upTo;
  }

  // 判断一个晚到事件是否落在已有撤销墓碑范围内。
  // 安全默认：事件必须"自证"是撤销之后的新事件，否则一律视为被撤销的补发
  // （因为补发消息可能比撤销通知更晚到达，绝不能据此复活项目）。
  // 自证更新的两种信号（任一即可）：
  //   1) 携带严格更高的客户端版本号（> 撤销版本上界）；
  //   2) 携带严格晚于撤销时间边界的事件时间。
  // 既无版本号又无事件时间、或信号不更新者，按已撤销补发处理。
  _findApplicableTombstone(orgId, ref, clientVersion, eventTime) {
    const t = this.stmts.latestTombstone.get(orgId, ref);
    if (!t) return null;
    const versionBoundary = t.up_to_version != null ? t.up_to_version : null;
    const timeBoundary = t.up_to_event_time != null ? t.up_to_event_time : t.recorded_at;

    const provesNewerByVersion =
      versionBoundary != null && clientVersion != null && clientVersion > versionBoundary;
    const provesNewerByTime =
      eventTime != null && timeBoundary != null && eventTime > timeBoundary;

    return provesNewerByVersion || provesNewerByTime ? null : t;
  }

  _handleSplit(msg, payload) {
    let { projectId } = this._resolveProject(msg, payload);
    // 已撤销等情况下当前 confirmed 映射可能已过期，回退到最近的任意状态映射，
    // 以便下面返回语义正确的 409（项目非生效）而非 404（无项目）。
    if (!projectId) {
      const fallback = this.stmts.latestMappingAnyStatus.get(msg.org_id, payload.ref);
      projectId = fallback ? fallback.project_id : null;
    }
    if (!projectId) throw new HttpError(404, 'unmapped_ref', `标识 ${payload.ref} 尚无归并项目，无法拆分`);
    const parts = payload.parts;
    if (!Array.isArray(parts) || parts.length < 2) {
      throw new HttpError(400, 'bad_split', '拆分必须提供至少两个 parts');
    }
    const project = this.stmts.getProject.get(projectId);
    if (project.primary_org !== msg.org_id) {
      throw new HttpError(403, 'not_your_fact', '只有项目主报机构可以拆分项目');
    }
    const parent = this.stmts.currentVersion.get(projectId);
    if (!parent || parent.status !== 'effective') {
      throw new HttpError(409, 'project_not_active', `项目 ${projectId} 当前无已生效版本，不可拆分`);
    }

    return this._runInTx(() => {
      const result = { split: true, parentProjectId: projectId, children: [], versions: [] };
      parts.forEach((part, idx) => {
        const childId = part.projectId || newId('prj');
        if (!this.stmts.getProject.get(childId)) {
          this.stmts.insertProject.run(childId, msg.org_id, this._now());
        }
        if (part.ref) {
          this.stmts.insertMapping.run(
            newId('map'), msg.org_id, part.ref, childId,
            (part.eventTime || payload.eventTime || msg.received_at), null, 1,
            'confirmed', msg.id, this._now());
        }
        const v = this._addVersion({
          projectId: childId, msg,
          changeType: 'split_out', eventType: payload.eventType || (parent && parent.event_type),
          clientVersion: payload.version || null, facts: part.facts || {},
          eventTime: payload.eventTime || null, status: 'effective', confidence: 1,
          relatedProjectId: projectId, note: `从 ${projectId} 拆分 (${idx + 1}/${parts.length})`,
        });
        // 拆分出的事实由执行拆分的机构（父项目所有方）负责
        this._recordFactOwners(childId, part.facts || {}, msg.org_id, v.version_id);
        this._emitNotification('project_split_out', childId, v,
          { orgId: msg.org_id, parentProjectId: projectId });
        result.children.push(childId);
        result.versions.push(v);
      });
      // 父项目标记撤销（其事实已被各子项目继承），保留版本链可追溯
      if (parent && parent.status === 'effective') {
        this._transition(parent.version_id, 'effective', 'revoked', 'split', this._now());
      }
      return result;
    });
  }

  _handleMergeRequest(msg, payload) {
    const fromProjectId = payload.fromProjectId;
    const intoProjectId = payload.intoProjectId;
    if (!fromProjectId || !intoProjectId) {
      throw new HttpError(400, 'bad_merge', '合并必须提供 fromProjectId 与 intoProjectId');
    }
    if (fromProjectId === intoProjectId) {
      throw new HttpError(400, 'bad_merge', '不能合并项目自身');
    }
    const from = this.stmts.getProject.get(fromProjectId);
    const into = this.stmts.getProject.get(intoProjectId);
    if (!from || !into) throw new HttpError(404, 'project_not_found', '待合并项目不存在');
    if (from.primary_org !== msg.org_id) {
      throw new HttpError(403, 'not_your_fact', '只能由被合并项目的所有方发起合并');
    }
    if (from.primary_org === into.primary_org) {
      // 同一机构内部纠错性合并：直接生效
      return this._runInTx(() => this._applyMerge(null, fromProjectId, intoProjectId, msg, payload, 'same_org'));
    }
    // 跨机构：生成提案，等待对方确认
    return this._runInTx(() => {
      const mergeId = newId('mrg');
      this.stmts.insertMerge.run(
        mergeId, fromProjectId, intoProjectId, msg.org_id, into.primary_org, msg.id, this._now());
      return { mergeProposed: true, mergeId, requiresConfirmationFrom: into.primary_org };
    });
  }

  // 跨机构合并的对方确认（带机构鉴权）
  confirmMerge(mergeId, orgId, actor) {
    const merge = this.stmts.getMerge.get(mergeId);
    if (!merge) throw new HttpError(404, 'merge_not_found', '合并提案不存在');
    if (merge.status !== 'pending') {
      throw new HttpError(409, 'merge_not_pending', `合并提案状态 ${merge.status}，不可确认`);
    }
    if (merge.confirmer_org !== orgId) {
      throw new HttpError(403, 'not_confirmer', '只有被并入项目的所有机构可以确认该合并');
    }
    return this._runInTx(() => {
      this.stmts.confirmMerge.run(this._now(), mergeId);
      const msg = merge.source_message_id ? this.stmts.getMessageById.get(merge.source_message_id) : null;
      const payload = msg ? JSON.parse(msg.raw_payload) : {};
      const result = this._applyMerge(merge, merge.from_project_id, merge.into_project_id, msg, payload, actor || orgId);
      return result;
    });
  }

  rejectMerge(mergeId, orgId, reason) {
    const merge = this.stmts.getMerge.get(mergeId);
    if (!merge) throw new HttpError(404, 'merge_not_found', '合并提案不存在');
    if (merge.status !== 'pending') {
      throw new HttpError(409, 'merge_not_pending', `合并提案状态 ${merge.status}，不可操作`);
    }
    if (merge.confirmer_org !== orgId) {
      throw new HttpError(403, 'not_confirmer', '只有确认方可以拒绝该合并');
    }
    this.stmts.rejectMerge.run(this._now(), mergeId);
    return { mergeId, status: 'rejected', reason: reason || null };
  }

  _applyMerge(merge, fromProjectId, intoProjectId, msg, payload, actor) {
    const fromCurrent = this.stmts.currentVersion.get(fromProjectId);
    const intoCurrent = this.stmts.currentVersion.get(intoProjectId);
    const intoFacts = intoCurrent && intoCurrent.facts ? parseJson(intoCurrent.facts) : {};
    const fromFacts = fromCurrent && fromCurrent.facts ? parseJson(fromCurrent.facts) : {};
    const extraFacts = payload.facts || {};
    const mergedFacts = { ...intoFacts, ...fromFacts, ...extraFacts };

    // 合并后字段归属：并入项目原字段归原来源机构，被合并项目带来的字段归其来源，
    // 提案中显式携带的字段归提案方。冲突键以被合并项目来源为准，随后由提案方覆盖。
    const fieldOwners = new Map();
    for (const r of this.stmts.factOwnersForProject.all(intoProjectId)) fieldOwners.set(r.fact_key, r.org_id);
    for (const r of this.stmts.factOwnersForProject.all(fromProjectId)) fieldOwners.set(r.fact_key, r.org_id);
    const proposerOrg = (merge && merge.proposed_by) || (msg && msg.org_id) || actor;
    for (const key of Object.keys(extraFacts)) fieldOwners.set(key, proposerOrg);
    if (fieldOwners.size === 0) {
      // 无细粒度来源时退化为按项目主报机构归属
      if (intoCurrent) Object.keys(intoFacts).forEach((k) => fieldOwners.set(k, intoCurrent.source_org));
      if (fromCurrent) Object.keys(fromFacts).forEach((k) => fieldOwners.set(k, fromCurrent.source_org));
    }

    const v = this._addVersion({
      projectId: intoProjectId,
      msg: msg || { id: null, org_id: actor, received_at: this._now(), op: 'merge' },
      changeType: 'merge', eventType: intoCurrent && intoCurrent.event_type,
      clientVersion: payload.version || null, facts: mergedFacts,
      eventTime: payload.eventTime || null, status: 'effective', confidence: 1,
      relatedProjectId: fromProjectId,
      note: `合并自 ${fromProjectId}（跨机构双方确认）`,
    });
    for (const [key, orgId] of fieldOwners) {
      this.stmts.upsertFactOwner.run(intoProjectId, key, orgId, v.version_id, this._now());
    }
    if (intoCurrent && intoCurrent.status === 'effective') {
      this._transition(intoCurrent.version_id, 'effective', 'superseded', 'merged', this._now());
    }
    if (fromCurrent) {
      this._transition(fromCurrent.version_id, fromCurrent.status, 'revoked', 'merged_away', this._now());
    }
    // 被合并项目的标识映射重指向并入项目，保证后续报文正确路由。
    // 顺序：先读出、置旧过期，再插入新 confirmed（避免与"同一标识唯一 confirmed"约束冲突）；
    // 若目标项目已存在同一 (机构,标识) 的 confirmed 映射，则沿用既有映射。
    const remapped = this.db.prepare(
      "SELECT * FROM identifier_mappings WHERE project_id=? AND status='confirmed'",
    ).all(fromProjectId);
    this.db.prepare(
      "UPDATE identifier_mappings SET status='deprecated', valid_to=? WHERE project_id=? AND status='confirmed'",
    ).run(this._now(), fromProjectId);
    for (const m of remapped) {
      const existing = this.stmts.activeMapping.get(m.org_id, m.external_ref);
      if (existing) continue; // 目标已确认过同一标识，沿用既有映射
      this.stmts.insertMapping.run(
        newId('map'), m.org_id, m.external_ref, intoProjectId,
        this._now(), null, m.confidence, 'confirmed', msg ? msg.id : null, this._now());
    }

    this._emitNotification('projects_merged', intoProjectId, v,
      { fromProjectId, intoProjectId, by: actor });
    if (merge) {
      this.stmts.setMergeResult.run(v.version_id, merge.merge_id);
    }
    return { merged: true, fromProjectId, intoProjectId, version: v };
  }

  // ---------- 人工审核 ----------

  listOpenReviews() {
    return this.stmts.listOpenReviews.all('open');
  }

  getReview(reviewId) {
    const review = this.stmts.getReview.get(reviewId);
    if (!review) throw new HttpError(404, 'review_not_found', '审核任务不存在');
    return review;
  }

  // 人工决定实体关系：
  //   decision=approve          接受为独立新项目（pending 版本生效）
  //   decision=link, targetProjectId  关联到已有项目（版本迁移过去并修订映射）
  //   decision=reject           拒绝该事件（版本标记 rejected）
  // expectedLockVersion 实现乐观并发：并发决策时后提交方得到 409。
  decideReview(reviewId, { decision: rawDecision, targetProjectId, actor, expectedLockVersion }) {
    const review = this.getReview(reviewId);
    if (review.status !== 'open') {
      throw new ReviewConflictError(review);
    }
    if (expectedLockVersion != null && Number(expectedLockVersion) !== review.lock_version) {
      throw new ReviewConflictError(review);
    }
    let decision = rawDecision;
    if (!['approve', 'link', 'reject'].includes(decision)) {
      throw new HttpError(400, 'bad_decision', 'decision 必须是 approve | link | reject');
    }
    if (decision === 'link' && !targetProjectId) {
      throw new HttpError(400, 'missing_target', 'link 决策必须提供 targetProjectId');
    }
    if (decision === 'link' && !this.stmts.getProject.get(targetProjectId)) {
      throw new HttpError(404, 'target_not_found', '目标项目不存在');
    }
    // 关联到自身等价于批准为独立项目
    if (decision === 'link' && targetProjectId === review.project_id) {
      decision = 'approve';
    }

    return this._runInTx(() => {
      let resolvedVersionId = null;
      let target = review.project_id;

      if (decision === 'reject') {
        const pending = this.stmts.currentVersion.get(review.project_id);
        if (pending) {
          this._transition(pending.version_id, 'pending', 'rejected', 'review_rejected', this._now());
          resolvedVersionId = pending.version_id;
        }
      } else if (decision === 'link') {
        target = targetProjectId;
        // 把待审核事实作为修订版本挂到目标项目（字段级合并，保留目标项目已有字段）
        const msg = this.stmts.getMessageById.get(review.source_message_id);
        const current = this.stmts.currentVersion.get(targetProjectId);
        const proposedFacts = parseJson(review.proposed_facts || '{}');
        const linkedFacts = {
          ...(current && current.facts ? parseJson(current.facts) : {}),
          ...proposedFacts,
        };
        const v = this._addVersion({
          projectId: targetProjectId, msg, changeType: 'revise',
          eventType: review.event_type, clientVersion: null,
          facts: linkedFacts,
          eventTime: review.event_time, status: 'effective',
          revisionOf: current ? current.version_id : null,
          confidence: 1, note: `人工审核：${review.org_id}:${review.external_ref} 关联到本项目`,
        });
        // 关联进来的字段来源仍是上报机构，而非审核员
        this._recordFactOwners(targetProjectId, proposedFacts, review.org_id, v.version_id);
        if (current && current.status === 'effective') {
          this._transition(current.version_id, 'effective', 'superseded', 'review_link', this._now());
        }
        // 原挂起项目的 pending 版本作废，标识映射改指向目标项目
        const orphan = this.stmts.currentVersion.get(review.project_id);
        if (orphan) this._transition(orphan.version_id, 'pending', 'rejected', 'review_linked_elsewhere', this._now());
        this.stmts.deprecateMappings.run(this._now(), review.org_id, review.external_ref, this._now());
        this.stmts.insertMapping.run(
          newId('map'), review.org_id, review.external_ref, targetProjectId,
          this._now(), null, 1, 'confirmed', review.source_message_id, this._now());
        resolvedVersionId = v.version_id;
        this._emitNotification('project_revised', targetProjectId, v, { via: 'review_link' });
      } else {
        // approve：pending 版本生效，映射确认为 confirmed
        const pending = this.stmts.currentVersion.get(review.project_id);
        if (pending) {
          this._transition(pending.version_id, 'pending', 'effective', 'review_approved', this._now());
          resolvedVersionId = pending.version_id;
          // 低置信创建时未落来源，批准时补齐字段归属
          this._recordFactOwners(
            review.project_id,
            parseJson(review.proposed_facts || '{}'),
            review.org_id,
            pending.version_id,
          );
        }
        this.stmts.insertMapping.run(
          newId('map'), review.org_id, review.external_ref, review.project_id,
          this._now(), null, 1, 'confirmed', review.source_message_id, this._now());
        this._emitNotification('project_created', review.project_id, resolvedVersionId,
          { orgId: review.org_id, ref: review.external_ref, via: 'review_approved' });
      }

      const finalStatus = decision === 'reject' ? 'rejected' : 'approved';
      const info = this.stmts.resolveReview.run(
        finalStatus, decision, actor || 'reviewer', this._now(),
        resolvedVersionId, target, reviewId, review.lock_version,
      );
      if (info.changes === 0) {
        // 并发：锁版本已被其他决策推进
        throw new ReviewConflictError(this.stmts.getReview.get(reviewId));
      }
      return { reviewId, decision, projectId: target, resolvedVersionId,
        review: this.stmts.getReview.get(reviewId) };
    });
  }

  // ---------- 查询：时间线（as-of 重建） ----------

  // 按"当时已知信息"重建项目时间线。
  // asOf 给定时，只纳入 recorded_at <= asOf 的版本，并按该时刻的状态着色；
  // 不传 asOf 时返回当前完整时间线。
  getTimeline(projectId, asOf) {
    const project = this.stmts.getProject.get(projectId);
    if (!project) throw new HttpError(404, 'project_not_found', '项目不存在');
    const versions = this.db.prepare(
      `SELECT * FROM project_versions
       WHERE project_id=? ${asOf ? 'AND recorded_at <= ?' : ''}
       ORDER BY recorded_at, version_no`,
    ).all(projectId, ...(asOf ? [asOf] : []));

    const histStmt = this.db.prepare(
      `SELECT * FROM version_status_history
       WHERE version_id=? ${asOf ? 'AND changed_at <= ?' : ''}
       ORDER BY changed_at, id`,
    );

    const timeline = versions.map((v) => {
      const hist = asOf
        ? histStmt.all(v.version_id, asOf)
        : histStmt.all(v.version_id);
      // 创建时即写入首条历史；as-of 下取截止时刻前最后一次流转的目标状态
      const statusAt = hist.length ? hist[hist.length - 1].to_status : v.status;
      return {
        versionId: v.version_id,
        versionNo: v.version_no,
        changeType: v.change_type,
        eventType: v.event_type,
        facts: v.facts ? parseJson(v.facts) : null,
        sourceOrg: v.source_org,
        sourceMessageId: v.source_message_id,
        eventTime: v.event_time,
        recordedAt: v.recorded_at,
        confidence: v.confidence,
        statusAt,
        relatedProjectId: v.related_project_id,
        note: v.note,
      };
    });

    // 项目快照：优先取最后一个 effective 条目；否则若仍待审取 pending；否则为 null。
    // as-of 下只在截止时刻已知的版本中选取，保证快照形态与时间线条目一致。
    let snapshot = null;
    for (let i = timeline.length - 1; i >= 0; i--) {
      if (timeline[i].statusAt === 'effective') { snapshot = timeline[i]; break; }
    }
    if (!snapshot) {
      for (let i = timeline.length - 1; i >= 0; i--) {
        if (timeline[i].statusAt === 'pending') { snapshot = timeline[i]; break; }
      }
    }
    return { projectId, asOf: asOf || null, timeline, snapshot };
  }

  // ---------- 统计：只使用已生效版本 ----------

  // 统计口径固定为 status='effective' 的版本；pending（待审）、revoked、
  // superseded、rejected 一律不计入。可给 asOf 做历史口径回放：
  // 版本在 T 时刻"已生效"当且仅当 T 之前最后一次状态流转的目标状态是 effective。
  getStats(asOf) {
    const effective = this.db.prepare(
      `SELECT v.* FROM project_versions v
       WHERE ${asOf
        ? `(SELECT h.to_status FROM version_status_history h
             WHERE h.version_id=v.version_id AND h.changed_at<=?
             ORDER BY h.changed_at DESC, h.id DESC LIMIT 1)='effective'`
        : "v.status='effective'"}`,
    ).all(...(asOf ? [asOf] : []));

    const byEventType = {};
    const byOrg = {};
    let amountSum = 0;
    let amountCount = 0;
    for (const v of effective) {
      byEventType[v.event_type || 'unknown'] = (byEventType[v.event_type || 'unknown'] || 0) + 1;
      byOrg[v.source_org] = (byOrg[v.source_org] || 0) + 1;
      const facts = v.facts ? parseJson(v.facts) : {};
      if (typeof facts.amount === 'number') { amountSum += facts.amount; amountCount += 1; }
    }
    return {
      asOf: asOf || null,
      effectiveProjectCount: effective.length,
      byEventType,
      byOrg,
      totalAmount: amountSum,
      amountBearingCount: amountCount,
    };
  }

  // ---------- 通知：事务性发件箱 + 幂等投递 ----------

  // flush 待发通知。每条通知以 idempotency_key 去重；
  // 已 delivered 的通知任何重放都不会再次投递。
  async flushNotifications(limit = 100) {
    const pending = this.stmts.pendingNotifications.all(limit);
    const delivered = [];
    const failed = [];
    for (const n of pending) {
      try {
        await this.notify(n);
        this.stmts.markNotification.run('delivered', this._now(), null, n.notification_id);
        delivered.push(n.notification_id);
      } catch (e) {
        this.stmts.markNotification.run('pending', null, String(e.message || e), n.notification_id);
        failed.push({ notificationId: n.notification_id, error: String(e.message || e) });
      }
    }
    return { delivered, failed, examined: pending.length };
  }

  listNotifications(status) {
    if (status) {
      return this.db.prepare('SELECT * FROM notifications WHERE status=? ORDER BY created_at').all(status);
    }
    return this.db.prepare('SELECT * FROM notifications ORDER BY created_at').all();
  }

  getProject(projectId) {
    const project = this.stmts.getProject.get(projectId);
    if (!project) throw new HttpError(404, 'project_not_found', '项目不存在');
    const versions = this.db.prepare(
      'SELECT * FROM project_versions WHERE project_id=? ORDER BY version_no',
    ).all(projectId);
    const mappings = this.db.prepare(
      'SELECT * FROM identifier_mappings WHERE project_id=? ORDER BY valid_from',
    ).all(projectId);
    return { project, versions, mappings };
  }

  listMappings(orgId) {
    if (orgId) {
      return this.db.prepare('SELECT * FROM identifier_mappings WHERE org_id=? ORDER BY created_at').all(orgId);
    }
    return this.db.prepare('SELECT * FROM identifier_mappings ORDER BY created_at').all();
  }

  // ---------- 内部工具 ----------

  _runInTx(fn) {
    return this.db.transaction(fn)();
  }

  // 记录一组事实字段的来源机构（机构只能纠正自己拥有的字段）
  _recordFactOwners(projectId, facts, orgId, versionId, at = this._now()) {
    if (!facts || typeof facts !== 'object') return;
    for (const key of Object.keys(facts)) {
      this.stmts.upsertFactOwner.run(projectId, key, orgId, versionId, at);
    }
  }

  // 字段级鉴权：机构只能纠正自己提交的字段；
  // 尚未归属的新字段允许任一参与方添加，修改他人字段一律 403。
  _assertCanCorrectFacts(projectId, facts, orgId) {
    for (const key of Object.keys(facts)) {
      const row = this.stmts.getFactOwner.get(projectId, key);
      if (row && row.org_id !== orgId) {
        throw new HttpError(403, 'not_your_fact',
          `字段 "${key}" 由机构 ${row.org_id} 提交，机构 ${orgId} 只能纠正自己提交的事实`);
      }
    }
  }

  _addVersion({
    projectId, msg, changeType, eventType, clientVersion, facts, eventTime,
    status, revisionOf = null, relatedProjectId = null, confidence = 1, note = null,
  }) {
    const versionNo = this.stmts.nextVersionNo.get(projectId).n;
    const versionId = newId('ver');
    const recordedAt = this._now();
    this.stmts.insertVersion.run({
      version_id: versionId,
      project_id: projectId,
      version_no: versionNo,
      change_type: changeType,
      event_type: eventType || null,
      client_version: clientVersion,
      facts: facts == null ? null : JSON.stringify(facts),
      source_org: msg.org_id,
      source_message_id: msg.id,
      event_time: eventTime || null,
      recorded_at: recordedAt,
      effective_at: status === 'effective' ? recordedAt : null,
      status,
      revision_of_version_id: revisionOf,
      related_project_id: relatedProjectId,
      confidence,
      note,
    });
    this.stmts.insertHistory.run(versionId, null, status, recordedAt, changeType);
    return this.stmts.getVersion.get(versionId);
  }

  _transition(versionId, fromStatus, toStatus, reason, at = this._now()) {
    const res = this.db.prepare(
      'UPDATE project_versions SET status=?, effective_at=COALESCE(effective_at, CASE WHEN ?=? THEN ? ELSE effective_at END) WHERE version_id=?',
    ).run(toStatus, toStatus, 'effective', at, versionId);
    if (res.changes === 0) return;
    this.stmts.insertHistory.run(versionId, fromStatus, toStatus, at, reason);
  }

  _emitNotification(kind, projectId, version, extra) {
    const v = typeof version === 'string' ? this.stmts.getVersion.get(version) : version;
    const idempotencyKey = `${kind}:${v.version_id}`;
    // INSERT OR IGNORE：重放/恢复时同一版本的同类通知绝不重复产生
    this.db.prepare(
      `INSERT OR IGNORE INTO notifications(notification_id, idempotency_key, kind,
          project_id, version_id, payload, created_at)
       VALUES(?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      newId('ntf'), idempotencyKey, kind, projectId, v.version_id,
      JSON.stringify({
        kind, projectId, versionId: v.version_id,
        changeType: v.change_type, facts: v.facts ? parseJson(v.facts) : null,
        ...extra,
      }),
      this._now(),
    );
  }
}

module.exports = { EventService, HttpError, ReviewConflictError, CONFIDENCE_THRESHOLD, MAX_RETRIES };
