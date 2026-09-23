const { openDatabase } = require('./database');
const { createEventService } = require('./event-service');
const { createApp } = require('./app');

const db = openDatabase(process.env.DATABASE_PATH || 'data/trade.sqlite3');
const service = createEventService(db);
const app = createApp(service);

if (require.main === module) {
  const port = Number(process.env.PORT || 3000);
  const host = process.env.HOST || '0.0.0.0';
  const server = app.listen(port, host);

  // 进程内发件箱驱动：默认投递器为占位实现，部署时在 createApp 注入真实下游。
  // 周期 drain 只是兜底；重放安全由 notification_deliveries 唯一键保证。
  const drainTimer = setInterval(() => {
    service.drainNotifications(() => Promise.resolve()).catch(() => {});
  }, 5000);
  drainTimer.unref();

  const shutdown = () => {
    clearInterval(drainTimer);
    server.close(() => {
      db.close();
      process.exit(0);
    });
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

module.exports = app;
