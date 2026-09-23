'use strict';

const express = require('express');

// 下游通知投递器：默认直接确认成功（生产环境替换为真实下游调用）。
// 即使这里“假成功”，投递事实也先落 notification_deliveries 再确认，重放绝不重复。
function defaultDeliver() {
  return Promise.resolve();
}

function createApp(service, { deliver = defaultDeliver } = {}) {
  const app = express();
  app.locals.service = service;

  // 事件接收必须使用原始报文做 HMAC 校验，因此注册在 json 中间件之前
  app.post('/v1/events/:institution', express.raw({ type: () => true, limit: '4mb' }), (req, res, next) => {
    try {
      const rawBody = typeof req.body === 'string' ? req.body : (req.body?.toString('utf8') ?? '');
      const messageId = req.header('x-message-id');
      if (!messageId) return res.status(400).json({ error: { code: 'ERR_NO_MESSAGE_ID', message: '缺少 X-Message-Id' } });
      const result = service.ingest(
        req.params.institution,
        rawBody,
        req.header('x-signature'),
        messageId,
      );
      res.status(202).json(result);
    } catch (error) { next(error); }
  });

  app.use(express.json());

  app.get('/health', (_req, res) => res.json({ status: 'ok' }));

  // 机构注册（运维侧）
  app.post('/admin/institutions', (req, res, next) => {
    try {
      const inst = service.registerInstitution(req.body ?? {});
      res.status(201).json({ code: inst.code, name: inst.name, active: !!inst.active });
    } catch (error) { next(error); }
  });

  // 标识映射：机构按有效期维护（只能维护本机构）
  app.get('/v1/mappings', (req, res, next) => {
    try {
      res.json({ mappings: service.listMappings(req.query.institution, req.query.external_ref) });
    } catch (error) { next(error); }
  });

  app.put('/v1/mappings', (req, res, next) => {
    try {
      const { institution, external_ref: externalRef, project_id: projectId, valid_from: validFrom, confidence } = req.body ?? {};
      if (!institution || !externalRef || !projectId) {
        return res.status(400).json({ error: { code: 'ERR_BAD_PAYLOAD', message: 'institution/external_ref/project_id 必填' } });
      }
      const mapping = service.upsertMapping(institution, externalRef, projectId, { validFrom, confidence });
      res.json({ mapping });
    } catch (error) { next(error); }
  });

  app.post('/v1/mappings/expire', (req, res, next) => {
    try {
      const { institution, external_ref: externalRef, valid_to: validTo } = req.body ?? {};
      const mapping = service.expireMapping(institution, externalRef, validTo);
      res.json({ mapping });
    } catch (error) { next(error); }
  });

  // 人工审核（乐观锁，并发冲突 409）
  app.get('/v1/reviews', (req, res) => res.json({ reviews: service.listReviews(req.query.status || 'open') }));
  app.post('/v1/reviews/:id/decide', (req, res, next) => {
    try {
      const result = service.decideReview(Number(req.params.id), {
        decision: req.body?.decision,
        reviewer: req.body?.reviewer ?? 'anonymous',
        note: req.body?.note ?? null,
        target: req.body?.target ?? null,
      });
      res.json(result);
    } catch (error) { next(error); }
  });

  // 跨机构合并：双方确认
  app.get('/v1/merges/:id', (req, res, next) => {
    try { res.json(service.getMerge(Number(req.params.id))); } catch (error) { next(error); }
  });
  app.post('/v1/merges/:id/confirm', (req, res, next) => {
    try {
      const institution = req.body?.institution;
      if (!institution) return res.status(400).json({ error: { code: 'ERR_BAD_PAYLOAD', message: '缺少 institution' } });
      res.json(service.confirmMerge(Number(req.params.id), institution));
    } catch (error) { next(error); }
  });
  app.post('/v1/merges/:id/reject', (req, res, next) => {
    try {
      const institution = req.body?.institution;
      const result = service.rejectMerge(Number(req.params.id), institution, req.body?.note ?? null);
      res.json(result);
    } catch (error) { next(error); }
  });

  // 时间线（可指定 as_of 重建当时已知信息）
  app.get('/v1/projects/:id/timeline', (req, res, next) => {
    try { res.json(service.timeline(req.params.id, req.query.as_of)); } catch (error) { next(error); }
  });

  // 统计（只使用已生效版本，可指定 as_of）
  app.get('/v1/stats', (req, res) => res.json(service.stats(req.query.as_of)));

  // 失败消息恢复
  app.get('/v1/recovery/failed-messages', (_req, res) => res.json({ messages: service.listFailedMessages() }));
  app.post('/v1/recovery/failed-messages', (_req, res, next) => {
    try { res.json(service.recoverFailed()); } catch (error) { next(error); }
  });

  // 通知发件箱：手动触发投递 / 失败重排队 / 查看已交付
  app.post('/v1/notifications/drain', async (_req, res, next) => {
    try { res.json(await service.drainNotifications(deliver)); } catch (error) { next(error); }
  });
  app.post('/v1/recovery/requeue-notifications', (_req, res) =>
    res.json(service.requeueStaleNotifications()));
  app.get('/v1/outbox', (req, res) => res.json({ outbox: service.listOutbox(req.query.status) }));
  app.get('/v1/deliveries', (_req, res) => res.json({ deliveries: service.listDeliveries() }));

  // 统一错误映射
  app.use((error, _req, res, _next) => {
    const status = error.status || 500;
    if (status >= 500) console.error(error);
    res.status(status).json({
      error: {
        code: error.code || 'ERR_INTERNAL',
        message: error.message || 'internal error',
        ...(error.details ? { details: error.details } : {}),
      },
    });
  });

  return app;
}

module.exports = { createApp };
