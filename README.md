# 跨境贸易事件版本归并

金砖国家示范中心贸易事件归并服务。各成员机构（贸易撮合、园区签约、项目履约）上报的同一合作项目在各方系统中编号不同，且补发、撤销、修订可能乱序到达。本服务保存来源报文与签名校验结果，按机构有效期维护标识映射，把重复 / 修订 / 撤销 / 拆分 / 合并表示为可追溯版本，并在低置信度时转人工审核。

## 能力

- **来源报文留存与签名校验**：HMAC-SHA256（`X-Signature: sha256=<hex>`），原始报文与校验结果原样落 `source_messages`；坏签名报文留存、事件不入库，机构按同一 `X-Message-Id` 补发即可恢复。
- **标识映射有效期**：`identifier_mappings` 按机构 + 外部号维护 `valid_from / valid_to` 区间，任意时刻同号只指向一个项目；合并时旧区间关闭、同号转指存续项目，历史时点解析仍返回旧项目。
- **可追溯版本谱系**（`event_versions` + `version_links`）：
  - 重复报文 / 重复 `event_id` 幂等，不产生新版本；
  - `revision` 旧版本 `superseded`，`cancel` 旧版本 `cancelled`，均保留 `parent_version_id` 与链接；
  - `split` 父版本 `superseded`，子项成为独立实体并以 `split_child` 链接；
  - `merge` 以 `merge_parent` 连接被吸收项目的全部生效版本。
- **乱序恢复**：撤销 / 修订 / 拆分 / 合并先于目标实体到达时进入 `pending_commands` 前向引用队列；目标事实一旦晚到补报，按业务时间自动补做。
- **人工审核**：置信度低于阈值（默认 0.8）的实体进入 `review_tasks`，审核员可裁决独立实体、驳回或并入既有项目；裁决带乐观锁，并发裁决后到者收到 409。
- **授权纠正**：机构只能修订 / 撤销 / 拆分自己提交的事实，越权返回 403；跨机构合并必须双方机构各自确认（同机构合并直接生效）。
- **双时间轴查询**：
  - 业务时间 `business_time`（版本生效区间）；
  - 入库时间 `recorded_at`（决定“当时是否已知”）。
  `GET /v1/projects/:id/timeline?as_of=` 按当时已知信息重建项目时间线；`GET /v1/stats?as_of=` 只统计当时已知且处于生效态的事实 / 修订版本。
- **幂等下游通知**：版本生效写入 `notification_outbox`（`dedupe_key` 唯一），投递成功先落 `notification_deliveries` 再标记完成；任何重放、进程重启、补发都不会重复触发已交付通知。失败退避重试，可手动重新入队。
- **数据库迁移**：v1 基线 → v2 机构与来源报文 → v3 项目/映射/事件/版本谱系/前向引用 → v4 审核与双方确认合并 → v5 通知发件箱，每个迁移在事务内应用并登记。

## HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /v1/events/:institution` | 事件接入（原始报文 + HMAC 头，需 `X-Message-Id`） |
| `PUT /v1/mappings` / `POST /v1/mappings/expire` / `GET /v1/mappings` | 机构维护映射有效期 |
| `GET /v1/reviews` / `POST /v1/reviews/:id/decide` | 人工审核（`approved`/`rejected`，可带 `target` 裁决实体关系） |
| `GET /v1/merges/:id` / `POST /v1/merges/:id/confirm` / `…/reject` | 跨机构合并双方确认 |
| `GET /v1/projects/:id/timeline?as_of=` | 按当时已知信息重建时间线 |
| `GET /v1/stats?as_of=` | 仅已生效版本的统计 |
| `GET|POST /v1/recovery/failed-messages` | 查看 / 恢复未处理报文 |
| `POST /v1/notifications/drain` / `POST /v1/recovery/requeue-notifications` | 投递发件箱 / 失败重排 |
| `GET /v1/outbox` / `GET /v1/deliveries` | 通知状态与交付事实 |
| `POST /admin/institutions` | 注册 / 更新机构密钥 |
| `GET /health` | 健康检查 |

### 事件报文

```json
{
  "event_id": "evt-001",
  "action": "fact | revision | cancel | split | merge",
  "business_time": "2026-09-01T08:00:00Z",
  "external_ref": "A-100",
  "target_ref": "A-100",
  "confidence": 0.95,
  "payload": { "category": "match", "amount": 1000 },
  "children": [{ "external_ref": "A-100-1", "payload": {} }],
  "surviving_ref": "A-1", "absorbed_ref": "B-1", "target_institution": "B"
}
```

## 本地验证

```bash
npm install
npm test     # node:test，含关键乱序/并发/重放场景
npm run build
```

数据库路径由 `DATABASE_PATH` 控制（默认 `data/trade.sqlite3`）。所有验证均在单进程 Linux 环境完成，仅依赖 Node 22 内置 `node:sqlite` 与 Express，不需要外部数据库或消息队列。
