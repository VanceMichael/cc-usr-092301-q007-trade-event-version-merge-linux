const { openDatabase } = require('../src/database');
const { EventService } = require('../src/service');
const { createApp } = require('../src/app');
const { hmac } = require('../src/util');
const http = require('node:http');
const { after } = require('node:test');

const ORGS = {
  a: { orgId: 'org-a', name: '机构A', secret: 'secret-a' },
  b: { orgId: 'org-b', name: '机构B', secret: 'secret-b' },
  c: { orgId: 'org-c', name: '机构C', secret: 'secret-c' },
};

// 即使测试断言失败也确保关闭所有内存库与监听端口，避免测试进程被句柄挂住。
const activeHarnesses = new Set();
after(async () => {
  await Promise.all([...activeHarnesses].map((h) => h._teardown().catch(() => {})));
  activeHarnesses.clear();
});

// 构造一个挂在内存库上的 service+app，通知器可记录并可按需制造失败。
async function createHarness({ orgs = ['a', 'b', 'c'], confidenceThreshold } = {}) {
  const db = openDatabase(':memory:');
  const delivered = [];
  let failTimes = 0; // 剩余强制失败次数
  const notify = async (n) => {
    if (failTimes > 0) {
      failTimes -= 1;
      throw new Error('downstream unavailable');
    }
    delivered.push(n);
  };
  const service = new EventService(db, {
    notify,
    ...(confidenceThreshold != null ? { confidenceThreshold } : {}),
  });
  for (const key of orgs) service.registerOrganization(ORGS[key].orgId, ORGS[key].name, ORGS[key].secret);
  const app = createApp(service);
  const server = app.listen(0, '127.0.0.1');
  await new Promise((r) => server.once('listening', r));
  const { port } = server.address();

  function request(method, path, { body, headers = {}, raw } = {}) {
    return new Promise((resolve, reject) => {
      const payload = raw != null ? raw : (body ? JSON.stringify(body) : undefined);
      const req = http.request({
        host: '127.0.0.1', port, path, method,
        headers: {
          ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}),
          ...headers,
        },
      }, (res) => {
        let data = '';
        res.on('data', (c) => { data += c; });
        res.on('end', () => {
          let json = null;
          try { json = data ? JSON.parse(data) : null; } catch { json = data; }
          resolve({ status: res.statusCode, body: json });
        });
      });
      req.on('error', reject);
      if (payload) req.write(payload);
      req.end();
    });
  }

  // 以某机构签名投递一条事件
  async function ingest(orgKey, msgId, event, { sign = true } = {}) {
    const org = ORGS[orgKey];
    const raw = JSON.stringify(event);
    const headers = { 'x-msg-id': msgId };
    if (sign) headers['x-signature'] = hmac(org.secret, raw);
    return request('POST', `/ingest/${org.orgId}`, { raw, headers });
  }

  const harness = {
    db, service, app, request, ingest, delivered,
    failNextNotifications: (n = 1) => { failTimes = n; },
    orgs: ORGS,
  };
  harness._teardown = async () => {
    activeHarnesses.delete(harness);
    await new Promise((r) => server.close(r));
    db.close();
  };
  harness.close = harness._teardown;
  activeHarnesses.add(harness);
  return harness;
}

module.exports = { createHarness, ORGS, hmac };
