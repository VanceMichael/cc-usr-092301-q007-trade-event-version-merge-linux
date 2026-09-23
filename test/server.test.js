'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { openDatabase } = require('../src/database');
const { createEventService } = require('../src/event-service');
const { createApp } = require('../src/app');

test('健康接口和SQLite迁移基线可用', async () => {
  const db = openDatabase(':memory:');
  const versions = db.prepare('SELECT COUNT(*) AS count FROM schema_versions').get().count;
  assert.equal(versions, 5);

  const app = createApp(createEventService(db));
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  const { port } = server.address();
  const result = await new Promise((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path: '/health' }, (response) => {
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body }));
    }).on('error', reject);
  });
  await new Promise((resolve) => server.close(resolve));
  assert.equal(result.status, 200);
  assert.deepEqual(JSON.parse(result.body), { status: 'ok' });
  db.close();
});
