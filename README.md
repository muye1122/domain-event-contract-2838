# 骑手招募 · 跨模块领域事件契约验证

可运行交付：为 7 种跨模块领域事件建立 JSON Schema 契约（`contracts/events/`），配套内存 Redis Stream 模拟、7 个业务动作生产者、3 个消费者（SOP派发 / 房间状态更新 / 付款计划生成）与死信队列。生产者先按 schema 校验再入流；v1 消费者忽略未知字段；缺少必填字段的事件进入死信队列并记录原因、不阻塞后续事件。所有数据均为**虚构合成数据**，无真实个人信息。

## 技术栈（与同系列保持一致，均为 MIT / Apache-2.0 / BSD 许可）
- Node 22 + Express 4
- 自写 JSON Schema 等价校验函数（支持 type / const / enum / required / properties / additionalProperties / minLength / pattern / format:date-time / minimum / items），**不安装新包**
- React 18 + Vite 6（契约 / 生产者 / 消费者 / 死信队列演示界面）
- Playwright 1.65，复用系统 Edge（`channel: 'msedge'`，不下载 Chromium）
- 内存模拟 Redis Stream；Dockerfile 使用 `node:22-alpine`

## 一键启动
```bash
# A. Docker（验收用）
docker compose up --build
#   演示界面 http://localhost:4173   Mock API http://localhost:3001/api/v1/health

# B. 本机开发
npm install
npm run dev:all            # mock:3001 + Vite:5173，打开 http://localhost:5173

# C. 本机生产预览（模拟容器）
npm install && npm run build && node server.js
#   http://localhost:4173，/api 代理到 3001

# D. 测试（系统 Edge，单 worker）
npm test
```

## 事件契约
公共字段（7 种事件全部必填）：

| 字段 | 类型 | 说明 |
|---|---|---|
| event_id | string | 全局唯一，`^evt_[a-f0-9]{12,}$` |
| event_type | string | 事件类型，各 schema 用 `const` 锁定 |
| version | string | 契约版本，当前 `v1` |
| occurred_at | string | ISO 8601 date-time |
| org_id | string | 组织标识（虚构） |
| site_id | string | 站点标识（虚构） |
| payload | object | 事件特有负载，各 schema 自定义必填字段 |

| 事件类型 | payload 必填字段 |
|---|---|
| contract.signed | contract_id, rider_id, signed_at, channel, amount, currency |
| lead.assigned | lead_id, assignee_id, station_id, assigned_at, source |
| lead.recycled | lead_id, reason, recycled_at, previous_assignee_id |
| quote.approved | quote_id, contract_id, amount, currency, approved_by, approved_at |
| room.status_changed | room_id, from_status, to_status, changed_at, changed_by |
| demand.published | demand_id, title, headcount, city, published_at, channel |
| certification.approved | certification_id, subject_id, cert_type, approved_by, approved_at, expires_at |

契约文件位于 `contracts/events/<event_type>.schema.json`。顶层与 payload 均设 `additionalProperties: true`：v1 消费者遇到未知字段时不报错、向前兼容。`data/samples.json` 随仓库提供 7 条合法样例，以及 `unknown_fields`、`missing_org_id` 两个边界样例。

## 模块与 API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/health` | 健康检查与内存状态计数 |
| GET | `/api/v1/meta` | 事件类型、公共字段、生产者、消费者与 demo 边界 |
| GET | `/api/v1/contracts` | 7 个契约索引（含 payload 必填字段） |
| GET | `/api/v1/contracts/:type` | 单个完整 schema |
| GET | `/api/v1/samples` | 样例事件与边界样例 |
| POST | `/api/v1/business/:action` | 触发业务动作（生产者），产出事件先校验再 XADD |
| GET | `/api/v1/stream` | 读取事件总线全部条目 |
| POST | `/api/v1/consume/all` | 把样例事件扇出给 3 个消费者 |
| POST | `/api/v1/consume/batch` | 批量投递，非法事件跳过并记录，后续事件照常处理 |
| POST | `/api/v1/consume/:consumer` | 投递给指定消费者 |
| GET | `/api/v1/consumers` | 消费者定义与处理记录（含每次处理的 kind/ref） |
| GET | `/api/v1/dlq` | 死信队列（含 reason 与原始事件） |
| GET | `/api/v1/selftest` | 自测汇总（契约数、样例校验、reason 非空检查） |
| POST | `/api/v1/_test/reset` | 验收用重置；生产模式下返回 404 |

业务动作名：`sign-contract` / `assign-lead` / `recycle-lead` / `approve-quote` / `change-room-status` / `publish-demand` / `approve-certification`。

消费者处理记录包含 `consumer`、`event_id`、`event_type` 与 `outcome`，可断言每个消费者对各事件的处理次数。

## 验收标准对照
| # | 标准 | 自动化测试 |
|---|---|---|
| 1 | 7 种事件各有 schema，生产者测试全过 | `seven schema files ...` / `all seven business-action producers ...` |
| 2 | contract.signed 被 3 个消费者各处理 1 次（断言记录） | `contract.signed is handled exactly once by each of the three consumers` |
| 3 | 带未知字段事件正常处理（v1 忽略） | `an event with unknown fields is processed normally by v1 consumers` |
| 4 | 缺 org_id 进死信队列、reason 非空、后续事件照常处理 | `missing org_id goes to the dead-letter queue ...` |

`npm test` 单命令 12/12 全绿；真实命令输出见 `验证记录.md`。测试产物写入系统临时目录，交付目录保持干净。

## Data sources
仅来自需求说明；所有 org_id / site_id / 人员标识 / 金额 / 城市均为虚构占位，无外部数据集、无真实个人信息、无境外 LLM 调用。

## 诚实边界（demo 限制）
- **Redis Stream 为内存模拟**，不是真实 Redis 服务。替换点：`mock-server/index.js` 中的 `xadd` / `xrange` 改为真实 `XADD` / `XRANGE` + 消费组；消费者确认与 DLQ 落库同样替换为持久化实现。
- 消费者处理记录与死信队列均在内存，服务重启回到种子状态；未实现真实数据库持久化。
- 自写校验器是 jsonschema 等价子集，覆盖本契约所需关键字；若接入任意 schema，可替换为标准 ajv / jsonschema 库。
- 测试钩子 `POST /api/v1/_test/reset` 仅供验收；`NODE_ENV=production`（Docker 镜像默认）下返回 404，不会改动生产数据。
- 未实现：真实 Redis、真实数据库、真实 SSO、真实支付/短信、真实 LLM 调用。
