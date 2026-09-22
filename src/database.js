const { DatabaseSync } = require('node:sqlite');
const { mkdirSync } = require('node:fs');
const { dirname, resolve } = require('node:path');

function openDatabase(path = process.env.DATABASE_PATH || 'data/trade.sqlite3') {
  const resolved = resolve(path);
  mkdirSync(dirname(resolved), { recursive: true });
  const db = new DatabaseSync(resolved);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;');
  db.exec('CREATE TABLE IF NOT EXISTS schema_versions(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);');
  db.prepare('INSERT OR IGNORE INTO schema_versions(version, applied_at) VALUES(1, ?)').run(new Date().toISOString());
  return db;
}

module.exports = { openDatabase };
