# BucketHub API 文档

> 供机器人 / 自动化系统 / 第三方集成调用。

## 快速开始

```bash
# 1. 提交一个 URL 下载并上传到 bucket
curl -X POST https://your-host/api/submit \
  -H "X-API-Key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"urls": ["https://example.com/file.zip"], "prefix": "backups/2026"}'

# 2. 轮询任务状态
curl https://your-host/api/status/5 -H "X-API-Key: YOUR_KEY"

# 3. 暂停 / 恢复 / 取消
curl -X POST https://your-host/api/jobs/5/pause  -H "X-API-Key: YOUR_KEY"
curl -X POST https://your-host/api/jobs/5/resume -H "X-API-Key: YOUR_KEY"
curl -X POST https://your-host/api/jobs/5/cancel -H "X-API-Key: YOUR_KEY"
```

---

## 鉴权

所有接口均需认证。支持三种方式（任选其一）：

| 方式 | 示例 |
|------|------|
| Query 参数 | `?apikey=YOUR_KEY` |
| Header | `X-API-Key: YOUR_KEY` |
| Bearer Token | `Authorization: Bearer YOUR_KEY` |

密钥在 `.env` 的 `APP_API_KEY` 中配置。认证失败统一返回 `401` + JSON `{"error": "未授权"}`。

> 所有接口（含 `/upload`、`/url-upload` 等表单类路由）对机器人默认返回 JSON。
> 无需特殊 `Accept` header — 缺省 `Accept: */*` 或不发 Accept 即返回 JSON。
> 仅浏览器直接提交 HTML 表单（`Accept: text/html`）才会走 redirect。

---

## 核心概念

### Job（任务）

所有上传 / 下载操作都是异步任务。提交后立即返回 `job_id`，后台 worker 处理——上传与下载各走一条独立的**单并发 FIFO 队列**：同一条队列内严格按提交顺序逐个执行（同一时间最多一个上传、一个下载），两条队列之间互不阻塞（一个上传和一个下载可同时进行）。

**状态流转**：

```
queued → uploading → done
                   → failed
                   → cancelled
```

- `queued`：排队中（等待前一个任务完成）
- `uploading`：传输中（含 fetch 下载阶段）
- `done`：完成
- `failed`：失败（`error` 字段含原因）
- `cancelled`：已取消

**暂停状态**：`paused=true` 可叠加在 `queued` 或 `uploading` 上。暂停的排队任务会被 worker 跳过；暂停的传输任务会阻塞在当前进度，恢复后继续。

**serial 参数**：历史兼容参数，现已无行为差异（队列本身固定单并发逐个执行），可省略。

### File（文件记录）

文件库中的每一条记录，关联上传状态、MD5、本地路径、桶信息等。一个 file 可关联一个 job（当前活跃任务）。

### Kind（任务类型）

| kind | 含义 |
|------|------|
| `fetch` | 从 URL 下载 → 上传到 bucket（或下载到服务器） |
| `upload` | 本地/服务器文件 → 指定桶（`bucket_id` 区分目标） |
| `download` | 指定桶 → 服务器路径（`bucket_id` 区分来源） |
| `transfer` | 下载源（桶/链接/本地路径）→ 指定桶，流式直传不落盘（`bucket_id` 区分目标） |

> 历史任务中可能残留 `upload_beijing` / `upload_bucket2` / `download_beijing` / `download_bucket2` 等旧 kind，
> 启动时已自动迁移为 `upload` / `download` + `bucket_id`。

### Bucket（桶标识）

桶完全由数据库 `buckets` 表驱动（见 [桶管理](#桶管理)），加桶只需调 `POST /api/buckets`，无需改代码。

所有接受 `bucket` / `bucket_id` 参数的接口统一支持两种引用方式：

| 引用 | 示例 | 说明 |
|------|------|------|
| 桶 id（数字） | `3` | `GET /api/buckets` 返回的 `id`，推荐 |
| legacy 别名 | `self` / `bucket2` / `beijing` | 历史遗留的三桶专属别名（仅存量库自动迁移的行带有），老机器人脚本可继续使用 |

缺省（不传）= **默认桶**（第一个添加的桶自动成为默认桶）。

---

## 机器人典型工作流

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
│ POST /api/  │────▶│ GET /api/    │────▶│ POST /api/jobs/  │
│ submit      │     │ status/:id   │     │ :id/{pause,      │
│             │     │ (轮询)        │     │  resume,cancel}  │
└─────────────┘     └──────────────┘     └──────────────────┘
```

1. **提交**：`POST /api/submit`（批量 URL）或 `POST /upload`（本地文件）
2. **轮询**：`GET /api/status/:id`，直到 `status` 变为 `done`/`failed`/`cancelled`
3. **控制**：按需 `pause` / `resume` / `cancel`
4. **查询**：`GET /api/files`（文件库）、`GET /api/objects`（桶对象）

> 任务进度推送也可用 Socket.IO（WebSocket），避免轮询。见下方 [实时推送](#实时推送socketio)。

---

## 接口详情

### 任务提交

#### POST /api/submit

批量登记 URL 文件记录，可选登记后自动下载到服务器本地。**机器人首选入口。**

```http
POST /api/submit
Content-Type: application/json
X-API-Key: YOUR_KEY

{
  "urls": ["https://example.com/a.zip", "https://example.com/b.zip"],
  "prefix": "backups/2026",
  "download": "none"
}
```

**参数**：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `urls` | `string[]` | 是 | 1~50 个 URL，需 `http://` 或 `https://` 开头 |
| `prefix` | `string` | 否 | 对象 key 前缀（默认读 `DEFAULT_PREFIX` 环境变量；若设置了 `BUCKET_PREFIX`，最终 key 还会前置该应用级前缀） |
| `bucket` | `string` | 否 | 目标桶：桶 id 或 legacy 别名（缺省 = 默认桶，用于生成 object_key 与同名检测） |
| `download` | `string` | 否 | 登记后的动作：`none`（默认，只登记不下载）/ `now`（立即下载到服务器本地，进入下载队列排队执行）/ `serial`（与 `now` 行为一致，历史兼容；`queue` 为同义别名） |

**响应** `200`：

```json
{
  "submitted": 2,
  "jobs": [
    {"url": "https://...", "file_id": 3, "job_id": 5, "object_key": "files/backups/2026/a.zip", "filename": "a.zip", "status": "queued"},
    {"url": "https://...", "file_id": 4, "job_id": null, "object_key": "files/backups/2026/b.zip", "filename": "b.zip", "status": "registered"}
  ],
  "errors": [
    {"url": "https://bad", "error": "同名文件已存在：files/backups/2026/a.zip"}
  ]
}
```

> `download=none`（默认）时只登记记录（`status: "registered"`、`job_id: null`），
> 之后在文件列表页手动触发「下载到服务器 / 上传」。
>
> ⚠️ **行为变更**：旧版本 submit 会自动「URL 下载并上传到 bucket」；现在默认不触发任何传输，
> 需要下载请显式传 `download: "now"` 或 `"serial"`（下载到服务器本地，上传到桶仍由手动触发）。
>
> 同名拒传：目标 key 已存在时该 URL 放入 `errors`，不中断其余提交。无成功提交时返回 `400`。

---

#### POST /upload

上传本地文件（multipart/form-data）。适合机器人上传二进制文件。

```http
POST /upload
X-API-Key: YOUR_KEY
Content-Type: multipart/form-data

file=<二进制>
prefix=backups/2026
bucket=3             # 可选：桶 id 或 legacy 别名 self|bucket2|beijing（缺省=默认桶）
key=custom/path.zip  # 可选：自定义完整 object key（覆盖 prefix）
datasource_id=3      # 可选
```

> 同名拒传：目标 key 已存在时返回 `400`。机器人默认获得 JSON 响应。

**响应**：

```json
{"status": "ok", "message": "「a.zip」已加入上传队列（1.50 GiB）。", "job_id": 5, "object_key": "...", "filename": "a.zip", "size": 1610612736}
```

---

### 任务状态查询

#### GET /api/status/:job_id

查询单个任务及其关联文件。

```http
GET /api/status/5
X-API-Key: YOUR_KEY
```

**响应**：

```json
{
  "job": {
    "id": 5, "kind": "fetch", "status": "uploading",
    "filename": "photo.jpg", "object_key": "files/photo.jpg",
    "source": "https://...", "size": 10240, "progress": 5120,
    "error": null, "cancelled": false, "paused": false,
    "bucket_id": null, "bucket_name": null,
    "created_at": 1234567890, "started_at": 1234567891, "finished_at": null
  },
  "file": {
    "id": 3, "status": "pending", "md5": "abc123", "size": 10240,
    "uploaded": 0, "uploaded_bucket2": 0, "uploaded_beijing": 0,
    "uploaded_bucket_ids": [1],
    "bucket": "mybucket", "object_key": "files/photo.jpg",
    "synced_at": null, "error": null
  }
}
```

> `file` 为 `null` 表示该 job 无关联文件记录。
> `uploaded_bucket_ids` 是权威字段（已上传桶 id 数组）；`uploaded` / `uploaded_bucket2` / `uploaded_beijing` 三个旧布尔为兼容保留，从 `uploaded_bucket_ids` 派生。

#### GET /api/jobs

最近 50 条任务。

```http
GET /api/jobs
X-API-Key: YOUR_KEY
```

**响应**：`JobUpdate[]`（同 `/api/status` 中 `job` 的格式）

---

### 任务控制

#### POST /api/jobs/:job_id/cancel

取消任务（排队中或传输中均可）。

```http
POST /api/jobs/5/cancel
X-API-Key: YOUR_KEY
```

**响应**：

```json
{"status": "ok", "message": "已请求取消。", "job": {"id": 5, "status": "cancelled", ...}}
```

> 排队中的任务立即标记 `cancelled`；传输中的任务由回调中止后标记。

#### POST /api/jobs/:job_id/pause

暂停任务。排队中的任务会被 worker 跳过；传输中的任务阻塞在当前进度。

```http
POST /api/jobs/5/pause
X-API-Key: YOUR_KEY
```

**响应**：

```json
{"status": "ok", "message": "已暂停", "job": {"id": 5, "status": "uploading", "paused": true, ...}}
```

> 仅 `queued` / `uploading` 状态可暂停，其余返回 `400`。

#### POST /api/jobs/:job_id/resume

恢复暂停的任务。排队任务会重新入队；传输任务解除阻塞继续。

```http
POST /api/jobs/5/resume
X-API-Key: YOUR_KEY
```

**响应**：

```json
{"status": "ok", "message": "已恢复", "job": {"id": 5, "status": "uploading", "paused": false, ...}}
```

#### DELETE /api/jobs/:job_id

删除一条任务记录（仅限已结束的历史记录：`done` / `failed` / `cancelled`）。
只清理任务日志，不影响文件记录与桶内对象；关联文件记录的 `job_id` 会置空。

```http
DELETE /api/jobs/5
X-API-Key: YOUR_KEY
```

**响应**：

```json
{"status": "ok", "message": "任务记录已删除"}
```

> 进行中的任务（`queued` / `uploading`，含已暂停）返回 `400`，需先取消。

#### GET /api/concurrency

查询上传/下载两条队列的当前并发（兼容保留。队列已固定单并发：同一条队列内按提交顺序逐个执行）。

```http
GET /api/concurrency
X-API-Key: YOUR_KEY
```

**响应**：

```json
{"upload": 1, "download": 1, "max": 1}
```

#### POST /api/concurrency

已退役：队列固定单并发 FIFO，不再支持调整（端点保留以兼容旧客户端，调用无害但无效果）。

---

### 文件库管理

#### GET /api/files

分页查询文件库。

```http
GET /api/files?page=1&page_size=20&q=backup&status=synced
X-API-Key: YOUR_KEY
```

| 参数 | 默认 | 说明 |
|------|------|------|
| `page` | 1 | 页码 |
| `page_size` | 20 | 每页条数（max 200） |
| `q` | — | 按 `filename` / `source_url` / `object_key` 模糊匹配 |
| `status` | — | `pending` / `synced` / `failed` / `deleted` / `cancelled` |

**响应**：

```json
{
  "items": [FileItem, ...],
  "total": 123, "page": 1, "page_size": 20
}
```

#### GET /api/files/:file_id

查询单个文件记录（前端行级刷新用，返回结构与列表项一致，含 `uploaded_bucket_ids`）。

```http
GET /api/files/3
X-API-Key: YOUR_KEY
```

**响应**：`FileItem`（`404` = 记录不存在）。

#### PATCH /api/files/:file_id

编辑文件记录（所有字段可选）。

```http
PATCH /api/files/3
X-API-Key: YOUR_KEY
Content-Type: application/json

{"status": "synced", "uploaded_bucket_ids": [1, 3], "md5": "newhash"}
```

> `uploaded_bucket_ids` 为**替换语义**（整体覆盖已上传桶集合）。
> 旧布尔 `uploaded` / `uploaded_bucket2` / `uploaded_beijing` 仍可传入（映射到对应桶），等价于 `uploaded_bucket_ids` 的单元素增删。

#### DELETE /api/files/:file_id

删除文件记录（仅数据库，不影响桶中对象）。

#### POST /api/files/:file_id/upload

将服务器本地文件上传到指定桶（需先 `download-server` 或已有本地副本）。

```http
POST /api/files/3/upload
X-API-Key: YOUR_KEY
Content-Type: application/json

{"bucket_id": 3, "key": "custom/object/key"}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `bucket_id` | `number` 或 `string` | 否 | 桶 id 或 legacy 别名（缺省 = 默认桶） |
| `key` | `string` | 否 | 自定义完整 object key |
| `serial` | `boolean` | 否 | 历史兼容参数，无行为差异（队列固定逐个执行），可省略 |

> 兼容旧路由：`/upload-cloud`、`/upload-bucket2`、`/upload-beijing`（无请求体或同上，固定转发到对应桶）。

#### POST /api/files/:file_id/transfer

从文件的下载源流式直传到目标桶（**无需本地副本**，源数据边读边传不落盘，跨平台桶间也可直传）。

```http
POST /api/files/3/transfer
X-API-Key: YOUR_KEY
Content-Type: application/json

{"bucket_id": 3}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `bucket_id` | `number` 或 `string` | 否 | 目标桶 id 或 legacy 别名（缺省 = 默认桶） |
| `serial` | `boolean` | 否 | 历史兼容参数，无行为差异，可省略 |

下载源按文件级配置解析（与 `download-server` 缺省逻辑一致）：

| download_kind | 直传来源 |
|---------------|----------|
| `bucket` | 从该桶（可为其他平台）流式拉取对象 |
| `url` | 从 `source_url` 流式抓取 |
| `local` | 读 `source_url` 指定的服务器本地路径 |
| 未配置 | 默认桶已上传 → 从默认桶拉取；否则回退 `source_url` |

- 目标桶已上传 / 源与目标为同一桶 / 无可用下载源时返回 400。
- 任务 `kind=transfer`，走上传道队列，进度/取消/暂停与其他任务一致；成功后写入 `file_uploads` 标记目标桶（默认桶另置 `status=synced`）。
- 源对象经 `head_object` 校验存在并取真实大小；传输完成后 `verify_object` 复核目标对象大小，md5 边传边算并回写 `files.md5`。

#### POST /api/files/:file_id/download-server

从指定桶下载到服务器 `SERVER_FILE_ROOT`。

```http
POST /api/files/3/download-server
X-API-Key: YOUR_KEY
Content-Type: application/json

{"bucket_id": 3}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `bucket_id` | `number` 或 `string` | 否 | 桶 id 或 legacy 别名；缺省时优先默认桶，未上传过默认桶则回退 `source_url` 直接下载 |
| `serial` | `boolean` | 否 | 历史兼容参数，无行为差异，可省略 |

> 兼容旧路由：`/download-server-bucket2`、`/download-server-beijing`（固定转发到对应桶）。

#### POST /api/files/:file_id/check

重新检测文件在指定位置是否存在，不存在则自动更新记录。

```http
POST /api/files/3/check
X-API-Key: YOUR_KEY
Content-Type: application/json

{"target": "local"}
```

| target | 检测内容 | 不存在时更新 |
|--------|----------|-------------|
| `local` | 服务器本地文件（`SERVER_FILE_ROOT`） | `local_path = NULL` |
| `cloud` 或缺省 | 默认桶中的对象 | 清默认桶标记；原已上传则 `status = 'pending'` |
| 桶 id（数字） | 对应桶中的对象 | 清该桶标记（默认桶同上联动 status） |
| `bucket2` / `beijing` | 对应 legacy 桶中的对象 | 清该桶标记 |

> 检测存在时会顺带更新 `size`（本地取文件 stat，桶内取对象 ContentLength）。

**响应**：

```json
{"target": "local", "exists": true, "file": {FileItem}}
```

---

### 桶对象管理

#### GET /api/objects

列出桶内对象（分页）。

```http
GET /api/objects?bucket=3&prefix=backups/&q=2026&page=1&page_size=50
X-API-Key: YOUR_KEY
```

> `bucket` 接受桶 id 或 legacy 别名（缺省 = 默认桶）。

**响应**：

```json
{
  "prefix": "backups/",
  "bucket": "self",
  "objects": [{"key": "...", "size": 1024, "last_modified": 1234567890}],
  "total": 42, "page": 1, "page_size": 50
}
```

#### DELETE /api/objects

删除桶对象。

```json
{"key": "path/to/object.zip", "bucket": 3}
```

> `bucket` 接受桶 id 或 legacy 别名（缺省 = 默认桶）。删除成功后本地文件库对应桶的上传标记自动清除（默认桶另标记 `status=deleted`）。

#### POST /api/objects/rename

重命名 / 移动桶对象（copy + delete）。

```json
{"bucket": 3, "from_key": "old/path", "to_key": "new/path"}
```

> `bucket` 接受桶 id 或 legacy 别名（缺省 = 默认桶）。

#### GET /download

流式下载桶对象到客户端。

```http
GET /download?key=path/to/file.zip&bucket=3&apikey=YOUR_KEY
```

> 浏览器直跳用，返回二进制流。`bucket` 接受桶 id 或 legacy 别名（缺省 = 默认桶）。

---

### 服务器文件管理

需配置 `SERVER_FILE_ROOT` 环境变量。

#### GET /api/server-files

列出服务器本地文件。

```json
{"root": "/data/files", "total_size": 12345678, "files": [{"path": "a.txt", "size": 123, "absolute": "/data/files/a.txt"}]}
```

#### POST /server-upload

上传服务器本地文件到 bucket（FormData：`path` + `prefix` + 可选 `bucket`=桶 id/别名）。

#### POST /server-download

从 bucket 下载到服务器路径（FormData：`key` + 可选 `destination` + 可选 `bucket`=桶 id/别名）。

> `destination` 缺省时落到 `SERVER_FILE_ROOT/<对象文件名>`（需已配置 `SERVER_FILE_ROOT`）。

#### GET /server-file/download

浏览器下载服务器文件（`?path=relative/path`）。

#### DELETE /api/server-files

删除服务器文件（`?path=relative/path`）。

---

### 系统

#### GET /api/auth

鉴权探测 + 应用信息。机器人可用它验证 key 是否有效。

```json
{
  "app": "buckethub", "bucket": "mybucket",
  "default_prefix": "",
  "bucket2_enabled": false, "bucket2_bucket": "",
  "beijing_enabled": false, "beijing_bucket": "",
  "bucket_private": true, "bucket_private_note": "...",
  "buckets": [
    {"id": 1, "name": "自己桶1", "bucket_name": "mybucket", "legacy_key": "self", "is_default": true},
    {"id": 3, "name": "北京桶", "bucket_name": "kkk-oo-a", "legacy_key": "beijing", "is_default": false}
  ]
}
```

> 旧字段 `bucket2_enabled` / `beijing_enabled` 等从 buckets 表派生保留；新集成建议直接读 `buckets` 数组。

#### GET /api/bucket-health

检测所有已启用桶的连通性 + 元数据（延迟、region、版本控制、公开状态等）。

```json
{
  "buckets": [
    {"id": 1, "name": "自己桶1", "bucket_name": "mybucket", "legacy_key": "self", "is_default": true,
     "health": {"ok": true, "latency_ms": 42, "status_code": 200, "...": "..."}}
  ]
}
```

---

### 桶管理

桶凭证存于数据库 `buckets` 表；`application_key` 永远不会出现在响应里。

#### GET /api/buckets

桶列表（含禁用桶；按 `sort_order` → `id` 排序，顺序由桶管理拖动排序 / `POST /api/buckets/reorder` 维护）。

```json
[
  {"id": 1, "name": "自己桶1", "bucket_name": "mybucket", "application_key_id": "0051...",
   "has_application_key": true, "endpoint": null, "region": "us-west-004",
   "addressing_style": "auto", "signature_version": "s3v4", "legacy_key": "self", "is_default": true,
   "enabled": true, "sort_order": 0, "created_at": 123, "updated_at": 456}
]
```

#### POST /api/buckets

新增桶。首个桶强制为默认桶。

```http
POST /api/buckets
X-API-Key: YOUR_KEY
Content-Type: application/json

{
  "name": "北京桶",
  "bucket_name": "kkk-oo-a",
  "application_key_id": "0051xxx",
  "application_key": "K005xxx",
  "endpoint": "tos-s3-cn-beijing.volces.com",
  "region": "cn-beijing",
  "addressing_style": "virtual",
  "signature_version": "s3v4",
  "sort_order": 10,
  "is_default": false
}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `name` | 是 | 显示名 |
| `bucket_name` | 是 | S3 桶名 |
| `application_key_id` / `application_key` | 是 | 凭证 |
| `endpoint` / `region` | 否 | 二者至少一个；都不填则尝试从 keyID 解析 |
| `addressing_style` | 否 | `auto`（默认）/ `virtual` / `path` |
| `signature_version` | 否 | S3 签名版本：`s3v4`（默认，AWS/B2/TOS）/ `s3`（华为云 OBS 的 S3 兼容层只认 V2） |
| `sort_order` | 否 | 排序值（默认 0） |
| `is_default` | 否 | 设为默认桶会清掉其它桶的默认标记 |

**响应** `201`：`{"status": "ok", "message": "桶「北京桶」已添加。", "bucket_id": 4}`

#### PATCH /api/buckets/:bucket_id

编辑桶（子集更新）。`application_key` 为空/缺省 = 保留旧值；`endpoint` / `region` 传空字符串 = 清除。
凭证 / endpoint / 桶名 / 寻址风格 / 签名版本变更后缓存客户端自动失效重建。
清除当前默认桶的 `is_default` 返回 `400`（必须保留一个默认桶）。

> 只写权限桶说明：`signature_version=s3` 的华为云 OBS 交付桶 AK 无读权限，
> 上传后的大小核对（head_object 403）自动跳过、同名预检按"不存在"放行，均不算失败。

#### DELETE /api/buckets/:bucket_id

删除桶。默认桶拒绝删除（`400`）。删除时：该桶排队/传输中任务标记 `failed`（error='桶已删除'）、
`file_uploads` 关联清除、`jobs.bucket_id` 置空。

#### POST /api/buckets/:bucket_id/test

连通性测试（用临时客户端，不污染缓存）。返回 `BucketHealthEntry`（同 `/api/bucket-health` 的 `health` 项）。

---

### 数据源管理

#### GET /api/scripts

数据源列表。

#### POST /api/scripts

新增数据源（JSON：`name` 必填；`script_path`? / `description`?）。返回 `{status, message, datasource_id}`，201。

```json
{ "name": "爬虫A", "script_path": "/opt/scripts/spider_a.py", "description": "备注" }
```

#### PATCH /api/scripts/:datasource_id

编辑数据源（JSON 子集更新：`name` / `script_path` / `description`；可选字段传空串即清空）。返回 `{status, message, datasource_id}`。

#### DELETE /api/scripts/:datasource_id

删除数据源，同时把关联文件的 `datasource_id` 置空。返回 `{status, message, datasource_id}`。

#### POST /scripts（legacy）

新增数据源（FormData：`name` + `script_path`? + `description`?）。

#### POST /scripts/delete（legacy）

删除数据源（FormData：`id`）。

---

## 实时推送（Socket.IO）

避免轮询，可用 WebSocket 接收任务进度更新。

```javascript
import { io } from "socket.io-client"

const socket = io({
  transports: ["websocket", "polling"],
  query: { apikey: "YOUR_KEY" }
})

// 连接时收到全量快照
socket.on("jobs_snapshot", (jobs) => console.log(jobs))

// 每次任务变化（进度更新 ~0.5s、状态变化、暂停/恢复等）
socket.on("job_update", (job) => {
  console.log(job.id, job.status, job.progress, job.paused)
})

// 任务记录被删除（DELETE /api/jobs/:id）
socket.on("job_removed", ({ id }) => console.log("已删除", id))
```

**JobUpdate 字段**：

```typescript
interface JobUpdate {
  id: number
  kind: string           // fetch | upload | download | transfer
  status: string         // queued | uploading | done | failed | cancelled
  filename: string
  object_key: string
  progress: number       // 已传输字节
  size: number           // 总字节
  error: string | null
  source: string | null  // transfer 任务为下载源描述（URL / 本地路径 / bucket:<id>）
  bucket_id: number | null    // upload/download/transfer 任务的目标桶 id
  bucket_name: string | null  // 目标桶显示名
  created_at: number     // Unix 时间戳（秒）
  started_at: number | null
  finished_at: number | null
  cancelled: boolean
  paused: boolean
  serial: boolean          // 历史兼容字段（队列固定逐个执行，true/false 行为一致）
}
```

---

## 错误处理

### 错误响应格式

错误响应有两种 JSON 格式（机器人建议同时检查 `error` 和 `message` 字段）：

```json
// 格式 A：大多数 /api/* 端点
{"error": "错误描述"}

// 格式 B：表单提交类端点（/upload, /server-upload, /url-upload, /scripts 等）
{"status": "error", "message": "错误描述"}
```

> 所有端点（含格式 B）对机器人均返回 JSON，无需 `Accept: application/json` header。
> 认证失败统一返回格式 A：`{"error": "未授权"}`。

### 常见错误码

| HTTP | 场景 |
|------|------|
| `400` | 参数缺失 / 同名文件已存在 / 状态不允许操作 |
| `401` | apikey 缺失或无效 |
| `404` | 任务 / 文件 / 对象不存在 |
| `500` | 服务器内部错误（文件落盘失败等） |
| `502` | 桶操作失败（B2/Boto3 异常） |

---

## 并发模型

上传与下载是**两条互不冲突的单并发 FIFO 队列**：

| 队列 | 任务 kind | 并发 | 说明 |
|------|-----------|------|------|
| 上传队列 | `upload` / `transfer` | 1 | 同一时间最多一个上传任务，按提交顺序逐个执行 |
| 下载队列 | `download` / `fetch` | 1 | 同一时间最多一个下载任务，按提交顺序逐个执行 |

- **跨队列并行**：一个上传任务与一个下载任务可同时进行（总并发最多 2）。
- **同类互斥**：同类任务（如两个上传）严格排队，前一个完成后自动接续下一个。
- **serial 参数**：历史兼容参数，不改变入队行为（`serial=true` 与 `false` 进入同一条队列）。
- **暂停**：暂停的传输任务阻塞在当前进度；暂停的排队任务被 worker 跳过（恢复时重新入队）。
- **取消**：内存标志 + 回调检查，立即中止传输。排队中取消直接标记。
- **恢复**：服务重启后排队任务按原队列恢复（按提交顺序）。

---

## Python 示例

```python
import requests
import time

BASE = "https://your-host"
HEADERS = {"X-API-Key": "YOUR_KEY", "Content-Type": "application/json"}

# 提交
r = requests.post(f"{BASE}/api/submit", headers=HEADERS,
                  json={"urls": ["https://example.com/big.zip"], "prefix": "data"})
job_id = r.json()["jobs"][0]["job_id"]

# 轮询
while True:
    s = requests.get(f"{BASE}/api/status/{job_id}", headers=HEADERS).json()
    job = s["job"]
    print(f"{job['status']} {job['progress']}/{job['size']}")
    if job["status"] in ("done", "failed", "cancelled"):
        break
    time.sleep(1)

# 暂停示例
requests.post(f"{BASE}/api/jobs/{job_id}/pause", headers=HEADERS)
# ... 稍后恢复
requests.post(f"{BASE}/api/jobs/{job_id}/resume", headers=HEADERS)
```

---

## 已知限制

1. **无 webhook 回调**：任务完成不会主动通知，需轮询 `/api/status/:id` 或用 Socket.IO。
2. **无批量控制**：暂停 / 恢复 / 取消只能逐个调用，无批量端点。
3. **无速率限制**：请自行控制调用频率。
4. **gunicorn 需单 worker**：任务队列在进程内存中，gunicorn 必须保持 `--workers 1`（多进程会导致队列分裂）。
