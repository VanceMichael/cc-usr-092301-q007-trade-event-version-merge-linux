# 跨境贸易事件版本归并

金砖国家示范中心的事件归并服务。各成员机构以不同编号上报同一合作项目，补发可能晚于撤销到达；本服务在 Node.js + 内置 `node:sqlite` 上，把**重复、修订、撤销、拆分、合并**统一表示为可追溯的项目版本，并在低置信度时交由人工审核。

## 能力一览

- **来源报文留痕**：保存原始报文字节、HMAC-SHA256 签名与校验结果（`source_messages`）。签名不符拒绝入业务，但报文仍留痕为 `rejected`。
- **幂等接收**：以 `(org_id, msg_id)` 去重，任何重放都不产生新版本、不重复通知。
- **标识映射**：按机构维护外部编号 → 内部 `project_id` 的映射，带有效期（`valid_from/valid_to`），撤销后置 `deprecated`；同一机构同一标识至多一条 `confirmed`（部分唯一索引）。
- **版本化归并**：`create / revise / revoke / split_out / merge` 全部落为 `project_versions`，并通过 `version_status_history` 记录每次状态流转，可完整追溯。
- **乱序安全**：
  - 撤销可能先到：先落**撤销墓碑**（`revocation_tombstones`），晚到的被撤销补发直接登记为 `revoked`，绝不复活项目；只有自证更晚（更高版本号 / 更晚事件时间）的新事件才开启新项目。
  - 修订可能先于创建到达：标记 `failed` 可重试，创建到达后经 `/messages/:id/recover` 归并。
  - 待审事件先被撤销：审核任务自动关闭。
- **人工审核**：低置信事件（默认 `confidence < 0.85`）进入 `review_tasks`，人工决定 **approve（独立新项目）/ link（关联到已有项目）/ reject**；决策用 `lock_version` 乐观锁，并发冲突返回 **409**。
- **机构只能纠正自己提交的事实**：字段级来源表 `fact_provenance` 记录每个事实字段的归属机构，覆盖他人字段返回 **403**；跨机构修订做字段级合并而不覆盖对方字段。
- **跨机构合并双方确认**：发起方生成 `merge_proposals`，必须由被并入方确认才生效；单方无法完成，重复确认返回 409。同机构内部纠错性合并直接生效。
- **时间线 as-of 重建**：`GET /projects/:id/timeline?asOf=...` 只按截止时刻已知的版本与状态着色，还原"当时看到的项目"。
- **统计只含已生效版本**：`pending / revoked / superseded / rejected` 一律不计；支持 as-of 历史口径。
- **事务性发件箱 + exactly-once 通知**：通知与业务版本同事务写入 `notifications`，以 `idempotency_key`（`kind:version_id`）去重；下游失败保留 `pending` 重试，已投递通知任何重放都不会再次投递。
- **失败消息恢复**：业务处理失败不丢消息（`failed` / `dead_letter`），可查询、可重处理；启动时自动重放可重试失败消息。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/:orgId` | 投递事件（原始字节 body，头 `x-msg-id`、`x-signature` 为 HMAC） |
| GET | `/health` | 健康检查 |
| POST | `/orgs` | 登记机构（`orgId/name/hmacSecret`） |
| GET | `/projects/:id` | 项目版本链与标识映射 |
| GET | `/projects/:id/timeline?asOf=` | 时间线与 as-of 快照 |
| GET | `/stats?asOf=` | 统计（仅已生效版本） |
| GET | `/mappings?orgId=` | 标识映射 |
| GET/POST | `/reviews`、`/reviews/:id`、`/reviews/:id/decide` | 人工审核（决策带 `expectedLockVersion`） |
| GET/POST | `/merges/:id/confirm`、`/merges/:id/reject` | 跨机构合并确认/拒绝（头 `x-org-id`） |
| GET | `/messages/failed` · POST `/messages/:id/recover` | 失败消息恢复 |
| GET/POST | `/notifications`、`/notifications/flush` | 通知查看与幂等投递 |

### 事件报文示例

```jsonc
// 上报 / 修订
{ "op": "upsert", "ref": "A-2026-001", "eventType": "match",
  "eventTime": "2026-09-20T00:00:00Z", "version": 3,
  "confidence": 0.92, "facts": { "amount": 1000000, "partner": "..." } }

// 撤销（可带 version / upToVersion / eventTime 界定范围）
{ "op": "revoke", "ref": "A-2026-001", "reason": "重复上报" }

// 拆分
{ "op": "split", "ref": "A-2026-001",
  "parts": [ { "ref": "A-2026-001-X", "facts": {} }, { "ref": "A-2026-001-Y", "facts": {} } ] }

// 跨机构合并（发起）
{ "op": "merge", "ref": "A-2026-001", "fromProjectId": "prj_a", "intoProjectId": "prj_b" }
```

`op` 取值：`upsert`（默认）/ `create` / `revise` / `revoke` / `split` / `merge`。

## 本地运行

```bash
npm install

# 预置机构（可选）
ORGS="org-a:机构A:secret-a,org-b:机构B:secret-b" \
  PORT=3000 npm start

# 计算签名示例
node -e "const {hmac}=require('./src/util');console.log(hmac('secret-a', JSON.stringify({op:'upsert',ref:'A-1'})))"
```

## 验证

```bash
npm run build   # 语法检查全部源文件
npm test        # 自动化测试（node:test），含关键乱序场景
```

测试覆盖：迁移基线与增量升级、签名留痕、幂等重放、字段级归属与越权、撤销/补发两种乱序、修订先到失败恢复、待审先撤销、拆分、跨机构合并双方确认与拒绝、低置信审核 approve/link/reject、审核乐观锁 409、时间线 as-of、统计口径、通知 exactly-once（含下游失败重试）。

所有验证均在单个 Linux 应用环境中完成，使用内存 SQLite，不需要浏览器或独立运行的数据库、缓存与消息队列。

## 代码结构

```
src/database.js  版本化迁移（schema_versions）与 node:sqlite 事务助手
src/service.js   领域核心：接收/签名、归并、版本链、审核、合并、墓碑、时间线、统计、发件箱
src/app.js       由 service 装配的 Express 应用与序列化
src/server.js    启动入口：装配 DB/服务/通知器，启动时恢复失败消息
src/util.js      HMAC、恒定时间比较、ID 与时钟
test/            node:test 自动化测试与测试夹具
```
