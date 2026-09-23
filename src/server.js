const { openDatabase } = require('./database');
const { EventService } = require('./service');
const { createApp } = require('./app');
const { nowIso } = require('./util');

// 默认下游投递器：记录到日志（生产可替换为消息队列/HTTP 推送）。
// 关键保证在 service 层：通知带 idempotency_key 且状态持久化，
// 因此无论本函数成功失败、进程重启或重放，都不会重复投递同一条通知。
function defaultNotifier() {
  return async (notification) => {
    // 占位：真实部署在此推送到下游。这里视为成功。
    if (process.env.NOTIFY_LOG) {
      console.log(`[notify] ${notification.kind} ${notification.notification_id}`, notification.payload);
    }
  };
}

// 自动重放失败消息的恢复器：仅在进程启动时尝试一次可重试的失败消息。
// 死信（超过重试上限或确定性拒绝）保留，等待人工通过 /messages/:id/recover 处理。
async function recoverFailedOnStartup(service) {
  const failed = service.listFailedMessages().filter((m) => m.status === 'failed');
  for (const msg of failed) {
    try {
      await service.recoverMessage(msg.id);
    } catch {
      // 仍未满足条件（如修订先于创建）则继续保留 failed，等待下次恢复
    }
  }
}

function bootstrap(db = openDatabase()) {
  const service = new EventService(db, { notify: defaultNotifier() });

  // 通过环境变量预置机构：ORGS="org-id:显示名:secret,org-b:B:secret-b"
  if (process.env.ORGS) {
    for (const item of process.env.ORGS.split(',')) {
      const [orgId, name, secret] = item.split(':');
      if (orgId && secret) service.registerOrganization(orgId.trim(), (name || orgId).trim(), secret.trim());
    }
  }

  const app = createApp(service);

  // 启动时恢复在途失败消息（不阻塞监听）
  recoverFailedOnStartup(service).catch(() => {});

  return { app, service, db };
}

if (require.main === module) {
  const { app, service, db } = bootstrap();
  const port = Number(process.env.PORT || 3000);
  const host = process.env.HOST || '0.0.0.0';
  const server = app.listen(port, host, () => {
    console.log(`trade-event-service listening on ${host}:${port} at ${nowIso()}`);
  });

  const shutdown = () => {
    server.close(() => {
      try { db.close(); } catch { /* already closed */ }
      process.exit(0);
    });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // 暴露以便测试/装配
  module.exports.service = service;
}

module.exports = { bootstrap, createApp };
