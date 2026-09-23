const express = require('express');
const { HttpError } = require('./service');

// 由 service 实例构建 Express 应用（便于测试注入内存库与通知器）。
function createApp(service) {
  const app = express();
  // /ingest 需要原始字节做 HMAC 校验，跳过 JSON 解析（由该路由的 raw 解析器处理）。
  app.use((req, res, next) => {
    if (req.method === 'POST' && req.path.startsWith('/ingest/')) return next();
    return express.json({ limit: '2mb' })(req, res, next);
  });

  // 签名必须针对原始报文字节计算，因此该路由单独使用 raw 解析，
  // 把原始字符串交给 service 校验，避免重新序列化导致签名不一致。
  app.post('/ingest/:orgId', express.raw({ type: '*/*', limit: '2mb' }), asyncHandler(async (req, res) => {
    const orgId = req.params.orgId;
    const msgId = req.header('x-msg-id');
    const signature = req.header('x-signature') || null;
    if (!msgId) throw new HttpError(400, 'missing_msg_id', '缺少 x-msg-id 请求头');
    const rawBody = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : String(req.body || '');
    if (!rawBody) throw new HttpError(400, 'empty_body', '报文为空');
    const result = await service.ingest(orgId, msgId, rawBody, signature);
    res.status(202).json({
      duplicate: result.duplicate,
      messageId: result.message.id,
      status: result.message.status,
      result: serializeIngestResult(result.result),
    });
  }));

  app.get('/health', (_req, res) => res.json({ status: 'ok' }));

  // 机构登记（管理用途）
  app.post('/orgs', asyncHandler(async (req, res) => {
    const { orgId, name, hmacSecret } = req.body || {};
    if (!orgId || !hmacSecret) throw new HttpError(400, 'bad_request', 'orgId 与 hmacSecret 必填');
    const org = service.registerOrganization(orgId, name || orgId, hmacSecret);
    res.status(201).json({ orgId: org.org_id, name: org.name });
  }));

  // 项目与标识映射
  app.get('/projects/:id', asyncHandler(async (req, res) => {
    res.json(serializeProject(service.getProject(req.params.id)));
  }));

  app.get('/projects/:id/timeline', asyncHandler(async (req, res) => {
    const asOf = req.query.asOf || null;
    res.json(service.getTimeline(req.params.id, asOf));
  }));

  app.get('/mappings', asyncHandler(async (req, res) => {
    res.json({ mappings: service.listMappings(req.query.orgId || null).map(serializeMapping) });
  }));

  // 统计：只使用已生效版本
  app.get('/stats', asyncHandler(async (req, res) => {
    res.json(service.getStats(req.query.asOf || null));
  }));

  // 人工审核
  app.get('/reviews', asyncHandler(async (req, res) => {
    if (req.query.status && req.query.status !== 'open') {
      throw new HttpError(400, 'bad_status', "仅支持 status=open 过滤；单条请用 /reviews/:id");
    }
    res.json({ reviews: service.listOpenReviews().map(serializeReview) });
  }));

  app.get('/reviews/:id', asyncHandler(async (req, res) => {
    res.json(serializeReview(service.getReview(req.params.id)));
  }));

  app.post('/reviews/:id/decide', asyncHandler(async (req, res) => {
    const { decision, targetProjectId, expectedLockVersion } = req.body || {};
    const actor = req.header('x-actor') || (req.body && req.body.actor) || 'reviewer';
    const out = service.decideReview(req.params.id, {
      decision, targetProjectId, expectedLockVersion, actor,
    });
    res.json(out);
  }));

  // 跨机构合并：对方确认 / 拒绝
  app.get('/merges/:id', asyncHandler(async (req, res) => {
    const merge = service.stmts.getMerge.get(req.params.id);
    if (!merge) throw new HttpError(404, 'merge_not_found', '合并提案不存在');
    res.json(merge);
  }));

  app.post('/merges/:id/confirm', asyncHandler(async (req, res) => {
    const orgId = requireOrg(req);
    const actor = req.header('x-actor') || orgId;
    res.json(service.confirmMerge(req.params.id, orgId, actor));
  }));

  app.post('/merges/:id/reject', asyncHandler(async (req, res) => {
    const orgId = requireOrg(req);
    const reason = (req.body && req.body.reason) || null;
    res.json(service.rejectMerge(req.params.id, orgId, reason));
  }));

  // 失败消息恢复
  app.get('/messages/failed', asyncHandler(async (req, res) => {
    res.json({ messages: service.listFailedMessages().map(serializeMessage) });
  }));

  app.post('/messages/:id/recover', asyncHandler(async (req, res) => {
    const out = await service.recoverMessage(Number(req.params.id));
    res.json({ messageId: out.message.id, status: out.message.status, result: serializeIngestResult(out.result) });
  }));

  // 通知：查看与投递（幂等）
  app.get('/notifications', asyncHandler(async (req, res) => {
    res.json({ notifications: service.listNotifications(req.query.status || null) });
  }));

  app.post('/notifications/flush', asyncHandler(async (req, res) => {
    const limit = (req.body && req.body.limit) || 100;
    res.json(await service.flushNotifications(limit));
  }));

  app.use((err, _req, res, _next) => {
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: err.code, message: err.message, details: err.details });
      return;
    }
    // 原始体不是 JSON 时的兜底
    if (err.type === 'entity.parse.failed') {
      res.status(400).json({ error: 'invalid_json', message: '请求体不是合法 JSON' });
      return;
    }
    res.status(500).json({ error: 'internal_error', message: String(err && err.message || err) });
  });

  return app;
}

function requireOrg(req) {
  const orgId = req.header('x-org-id') || (req.body && req.body.orgId);
  if (!orgId) throw new HttpError(400, 'missing_org', '缺少 x-org-id 请求头');
  return orgId;
}

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function serializeIngestResult(result) {
  if (!result) return null;
  return {
    ...result,
    versions: result.versions ? result.versions.map(serializeVersion) : result.versions,
  };
}

function serializeVersion(v) {
  if (!v) return v;
  return {
    versionId: v.version_id,
    projectId: v.project_id,
    versionNo: v.version_no,
    changeType: v.change_type,
    eventType: v.event_type,
    clientVersion: v.client_version,
    facts: v.facts ? JSON.parse(v.facts) : null,
    sourceOrg: v.source_org,
    sourceMessageId: v.source_message_id,
    eventTime: v.event_time,
    recordedAt: v.recorded_at,
    effectiveAt: v.effective_at,
    status: v.status,
    relatedProjectId: v.related_project_id,
    confidence: v.confidence,
    note: v.note,
  };
}

function serializeMapping(m) {
  return {
    mappingId: m.mapping_id,
    orgId: m.org_id,
    externalRef: m.external_ref,
    projectId: m.project_id,
    validFrom: m.valid_from,
    validTo: m.valid_to,
    confidence: m.confidence,
    status: m.status,
  };
}

function serializeMessage(m) {
  return {
    id: m.id,
    orgId: m.org_id,
    msgId: m.msg_id,
    eventType: m.event_type,
    op: m.op,
    externalRef: m.external_ref,
    signatureValid: !!m.signature_valid,
    signatureError: m.signature_error,
    receivedAt: m.received_at,
    status: m.status,
    retryCount: m.retry_count,
    lastError: m.last_error,
    processedAt: m.processed_at,
  };
}

function serializeReview(r) {
  return {
    reviewId: r.review_id,
    kind: r.kind,
    orgId: r.org_id,
    externalRef: r.external_ref,
    projectId: r.project_id,
    candidateProjectId: r.candidate_project_id,
    sourceMessageId: r.source_message_id,
    proposedFacts: r.proposed_facts ? JSON.parse(r.proposed_facts) : null,
    eventType: r.event_type,
    eventTime: r.event_time,
    confidence: r.confidence,
    status: r.status,
    decision: r.decision,
    decidedBy: r.decided_by,
    decidedAt: r.decided_at,
    resolvedVersionId: r.resolved_version_id,
    lockVersion: r.lock_version,
    createdAt: r.created_at,
  };
}

function serializeProject({ project, versions, mappings }) {
  return {
    projectId: project.project_id,
    primaryOrg: project.primary_org,
    createdAt: project.created_at,
    versions: versions.map(serializeVersion),
    mappings: mappings.map(serializeMapping),
  };
}

module.exports = { createApp };
