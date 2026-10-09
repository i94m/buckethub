import type {
  AppInfo,
  BucketHealth,
  BucketHealthEntry,
  BucketInfo,
  Datasource,
  FileItem,
  FilesResponse,
  ObjectsResponse,
  ServerFilesResponse,
} from "./types"

/**
 * apikey 全程从 URL ?apikey= 取，不缓存到 localStorage。
 * 所有请求统一走 X-API-Key header（后端 apikey_ok() 也支持 query / Bearer）。
 */
export function getApiKey(): string {
  return new URLSearchParams(window.location.search).get("apikey") ?? ""
}

/** 桶引用：桶 id（number）或 legacy 别名（"self" / "bucket2" / "beijing"）。 */
export type BucketRef = number | string

function headers(json = false): Record<string, string> {
  const h: Record<string, string> = { "X-API-Key": getApiKey() }
  if (json) h["Accept"] = "application/json"
  return h
}

async function handle<T>(res: Response): Promise<T> {
  if (res.status === 401) {
    throw new Error("未授权：apikey 无效或缺失")
  }
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    throw new Error(data?.message || data?.error || `请求失败 (HTTP ${res.status})`)
  }
  return data as T
}

/** GET /api/auth — 鉴权探测 + 应用信息（bucket、安全状态、默认前缀）。 */
export async function checkAuth(): Promise<AppInfo> {
  const res = await fetch("/api/auth", { headers: headers() })
  return handle<AppInfo>(res)
}

/** GET /api/bucket-health — 检测所有已启用桶的连通性。 */
export async function getBucketHealth(): Promise<BucketHealth> {
  const res = await fetch("/api/bucket-health", { headers: headers() })
  return handle<BucketHealth>(res)
}

/** GET /api/files — 分页文件列表。 */
export async function getFiles(params: {
  page?: number
  page_size?: number
  q?: string
  status?: string
}): Promise<FilesResponse> {
  const qs = new URLSearchParams()
  if (params.page) qs.set("page", String(params.page))
  if (params.page_size) qs.set("page_size", String(params.page_size))
  if (params.q) qs.set("q", params.q)
  if (params.status) qs.set("status", params.status)
  const res = await fetch(`/api/files?${qs}`, { headers: headers() })
  return handle<FilesResponse>(res)
}

/** GET /api/files/:id — 单个文件记录（行级刷新，结构与列表项一致）。 */
export async function getFile(fileId: number): Promise<FileItem> {
  const res = await fetch(`/api/files/${fileId}`, { headers: headers() })
  return handle<FileItem>(res)
}

/** GET /api/scripts — 数据源列表（id→名称映射）。 */
export async function getScripts(): Promise<Datasource[]> {
  const res = await fetch("/api/scripts", { headers: headers() })
  return handle<Datasource[]>(res)
}

/** POST /api/scripts — 新增数据源（name 必填；script_path / description 可选）。 */
export async function createScript(data: {
  name: string
  script_path?: string
  description?: string
}): Promise<FormResult & { datasource_id: number }> {
  const res = await fetch("/api/scripts", {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  return handle(res)
}

/** PATCH /api/scripts/:id — 编辑数据源（子集更新；可选字段空串即清空）。 */
export async function updateScript(
  datasourceId: number,
  data: {
    name?: string
    script_path?: string
    description?: string
  },
): Promise<FormResult & { datasource_id: number }> {
  const res = await fetch(`/api/scripts/${datasourceId}`, {
    method: "PATCH",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  return handle(res)
}

/** DELETE /api/scripts/:id — 删除数据源（关联文件的 datasource_id 置空）。 */
export async function deleteScript(datasourceId: number): Promise<FormResult & { datasource_id: number }> {
  const res = await fetch(`/api/scripts/${datasourceId}`, {
    method: "DELETE",
    headers: headers(true),
  })
  return handle(res)
}

// ---------------------------------------------------------------------------
// 桶管理 CRUD
// ---------------------------------------------------------------------------

/** GET /api/buckets 列表项（不含 application_key 明文）。 */
export interface Bucket extends BucketInfo {
  application_key_id: string
  has_application_key: boolean
  endpoint: string | null
  region: string | null
  addressing_style: string
  signature_version: string
  enabled: boolean
  sort_order: number
  created_at: number
  updated_at: number
}

/** POST /api/buckets 的请求体。 */
export interface BucketCreateData {
  name: string
  bucket_name: string
  application_key_id: string
  application_key: string
  endpoint?: string
  region?: string
  addressing_style?: string
  signature_version?: string
  sort_order?: number
  is_default?: boolean
}

/** PATCH /api/buckets/:id 的请求体（子集更新，application_key 空=保留）。 */
export interface BucketUpdateData extends Partial<Omit<BucketCreateData, "application_key_id" | "application_key">> {
  application_key_id?: string
  application_key?: string
  enabled?: boolean
}

/** GET /api/buckets — 桶列表（含禁用桶）。 */
export async function getBuckets(): Promise<Bucket[]> {
  const res = await fetch("/api/buckets", { headers: headers() })
  return handle<Bucket[]>(res)
}

/** POST /api/buckets — 新增桶（首桶强制为默认桶）。 */
export async function createBucket(data: BucketCreateData): Promise<FormResult & { bucket_id: number }> {
  const res = await fetch("/api/buckets", {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  return handle(res)
}

/** PATCH /api/buckets/:id — 编辑桶。 */
export async function updateBucket(
  bucketId: number,
  data: BucketUpdateData,
): Promise<FormResult & { bucket_id: number }> {
  const res = await fetch(`/api/buckets/${bucketId}`, {
    method: "PATCH",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  return handle(res)
}

/** DELETE /api/buckets/:id — 删除桶（默认桶拒绝）。 */
export async function deleteBucket(bucketId: number): Promise<FormResult & { bucket_id: number }> {
  const res = await fetch(`/api/buckets/${bucketId}`, {
    method: "DELETE",
    headers: headers(true),
  })
  return handle(res)
}

/** POST /api/buckets/reorder — 拖动排序（按传入顺序整体重写 sort_order）。 */
export async function reorderBuckets(bucketIds: number[]): Promise<FormResult> {
  const res = await fetch("/api/buckets/reorder", {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({ bucket_ids: bucketIds }),
  })
  return handle(res)
}

/** POST /api/buckets/:id/test — 连通性测试（临时 client，不入注册表）。 */
export async function testBucket(bucketId: number): Promise<BucketHealthEntry> {
  const res = await fetch(`/api/buckets/${bucketId}/test`, {
    method: "POST",
    headers: headers(true),
  })
  return handle<BucketHealthEntry>(res)
}

/** 表单提交类响应（与后端 respond() 对齐）。 */
export interface FormResult {
  status: "ok" | "error"
  message: string
  job_id?: number
  file_id?: number
  object_key?: string
  filename?: string
  size?: number
}

/** POST /upload — 本地文件上传到指定桶（FormData）。 */
export async function uploadFile(
  file: File,
  opts: { prefix?: string; key?: string; bucket?: BucketRef; datasourceId?: number } = {},
): Promise<FormResult> {
  const form = new FormData()
  form.append("file", file)
  if (opts.prefix) form.append("prefix", opts.prefix)
  if (opts.key) form.append("key", opts.key)
  if (opts.bucket !== undefined) form.append("bucket", String(opts.bucket))
  if (opts.datasourceId) form.append("datasource_id", String(opts.datasourceId))
  const res = await fetch("/upload", { method: "POST", headers: headers(true), body: form })
  return handle<FormResult>(res)
}

/** POST /url-upload — 录入链接（可自动下载 + 逐桶上传；key=完整 ObjectKey）。 */
export async function urlUpload(
  url: string,
  opts: {
    prefix?: string
    key?: string
    datasourceId?: number
    downloadKind?: "url" | "local" | "bucket"
    downloadBucketId?: number
    /** 勾选的自动上传桶 id：录入即下载，落地后自动逐桶上传。 */
    autoUploadBuckets?: number[]
    /** 历史兼容参数（队列固定逐个执行），可省略。 */
    serial?: boolean
  } = {},
): Promise<FormResult> {
  const form = new FormData()
  form.append("url", url)
  if (opts.key) form.append("key", opts.key)
  if (opts.prefix) form.append("prefix", opts.prefix)
  if (opts.datasourceId) form.append("datasource_id", String(opts.datasourceId))
  if (opts.downloadKind) form.append("download_kind", opts.downloadKind)
  if (opts.downloadBucketId) form.append("download_bucket_id", String(opts.downloadBucketId))
  if (opts.autoUploadBuckets?.length) {
    form.append("auto_upload_buckets", opts.autoUploadBuckets.join(","))
  }
  if (opts.serial) form.append("serial", "true")
  const res = await fetch("/url-upload", { method: "POST", headers: headers(true), body: form })
  return handle<FormResult>(res)
}

/** POST /api/files/:id/upload — 上传服务器本地文件到指定桶（可指定自定义 key；进上传队列逐个执行）。 */
export async function uploadFileToBucket(
  fileId: number,
  bucket: BucketRef,
  key?: string,
  opts: { serial?: boolean } = {},
): Promise<FormResult> {
  const res = await fetch(`/api/files/${fileId}/upload`, {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({
      bucket_id: bucket,
      ...(key ? { key } : {}),
      ...(opts.serial ? { serial: true } : {}),
    }),
  })
  return handle<FormResult>(res)
}

/** POST /api/files/:id/transfer — 从文件的下载源（桶/链接/本地路径）流式直传到目标桶，无需本地副本。 */
export async function transferToBucket(
  fileId: number,
  bucket: BucketRef,
  opts: { serial?: boolean } = {},
): Promise<FormResult> {
  const res = await fetch(`/api/files/${fileId}/transfer`, {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({
      bucket_id: bucket,
      ...(opts.serial ? { serial: true } : {}),
    }),
  })
  return handle<FormResult>(res)
}

/** POST /api/jobs/:id/cancel — 请求取消一个进行中的任务。 */
export async function cancelJob(
  jobId: number,
): Promise<{ status: string; message: string; job?: unknown }> {
  const res = await fetch(`/api/jobs/${jobId}/cancel`, {
    method: "POST",
    headers: headers(true),
  })
  return handle(res)
}

/** POST /api/jobs/:id/pause — 暂停一个排队中或上传中的任务。 */
export async function pauseJob(
  jobId: number,
): Promise<FormResult & { job?: unknown }> {
  const res = await fetch(`/api/jobs/${jobId}/pause`, {
    method: "POST",
    headers: headers(true),
  })
  return handle(res)
}

/** POST /api/jobs/:id/resume — 恢复一个已暂停的任务。 */
export async function resumeJob(
  jobId: number,
): Promise<FormResult & { job?: unknown }> {
  const res = await fetch(`/api/jobs/${jobId}/resume`, {
    method: "POST",
    headers: headers(true),
  })
  return handle(res)
}

/** DELETE /api/jobs/:id — 删除一条任务记录（仅限已结束的历史记录，不影响文件与桶内对象）。 */
export async function deleteJob(jobId: number): Promise<FormResult> {
  const res = await fetch(`/api/jobs/${jobId}`, {
    method: "DELETE",
    headers: headers(true),
  })
  return handle(res)
}

/** POST /api/files/:id/download-server — 下载对象到服务器（指定桶；缺省=默认桶/URL 兜底；进下载队列逐个执行）。 */
export async function downloadServerFromBucket(
  fileId: number,
  bucket?: BucketRef,
  opts: { serial?: boolean } = {},
): Promise<FormResult> {
  const res = await fetch(`/api/files/${fileId}/download-server`, {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({
      ...(bucket !== undefined ? { bucket_id: bucket } : {}),
      ...(opts.serial ? { serial: true } : {}),
    }),
  })
  return handle<FormResult>(res)
}

/** POST /api/files/:id/check — 重新检测文件在指定位置是否存在（target=桶 id / local / 别名）。 */
export async function checkFileExists(
  fileId: number,
  target: "local" | BucketRef,
): Promise<{ target: string; exists: boolean; file?: FileItem }> {
  const res = await fetch(`/api/files/${fileId}/check`, {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({ target }),
  })
  return handle(res)
}

/** POST /server-download — 从指定桶下载对象到服务器目录（destination 缺省 = SERVER_FILE_ROOT/<文件名>）。 */
export async function serverDownload(
  key: string,
  destination?: string,
  bucket: BucketRef = "self",
): Promise<FormResult> {
  const form = new FormData()
  form.append("key", key)
  if (destination) form.append("destination", destination)
  form.append("bucket", String(bucket))
  const res = await fetch("/server-download", { method: "POST", headers: headers(true), body: form })
  return handle<FormResult>(res)
}

/** PATCH /api/files/:id 的请求体（所有字段可选）。 */
export interface FileUpdateData {
  filename?: string | null
  object_key?: string
  md5?: string | null
  size?: number
  bucket?: string
  source_url?: string | null
  local_path?: string | null
  /** 已上传桶集合（替换语义）。 */
  uploaded_bucket_ids?: number[]
  status?: string
  datasource_id?: number | null
  /** 文件级下载源（'none' 表示清除）。 */
  download_kind?: "none" | "url" | "local" | "bucket"
  download_bucket_id?: number
  error?: string | null
}

/** PATCH /api/files/:id — 编辑文件记录（支持所有可编辑字段）。 */
export async function updateFile(
  fileId: number,
  data: FileUpdateData,
): Promise<{ status: string; file_id: number; file: FileItem }> {
  const res = await fetch(`/api/files/${fileId}`, {
    method: "PATCH",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  return handle(res)
}

/** DELETE /api/files/:id — 删除文件记录（仅删除数据库记录，不影响桶中对象）。 */
export async function deleteFile(fileId: number): Promise<{ deleted: boolean; file_id: number }> {
  const res = await fetch(`/api/files/${fileId}`, {
    method: "DELETE",
    headers: headers(true),
  })
  return handle(res)
}

/** DELETE /api/objects — 删除桶对象。 */
export async function deleteObject(
  key: string,
  bucket: BucketRef = "self",
): Promise<{ deleted: boolean; key: string }> {
  const res = await fetch("/api/objects", {
    method: "DELETE",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({ key, bucket }),
  })
  return handle(res)
}

/** POST /api/objects/rename — 重命名/移动桶内对象（copy + delete）。 */
export async function renameObject(
  fromKey: string,
  toKey: string,
  bucket: BucketRef = "self",
): Promise<{ ok: boolean; from_key: string; to_key: string }> {
  const res = await fetch("/api/objects/rename", {
    method: "POST",
    headers: { ...headers(), "Content-Type": "application/json" },
    body: JSON.stringify({ bucket, from_key: fromKey, to_key: toKey }),
  })
  return handle(res)
}

/** GET /api/objects — 列出桶内对象（按前缀 + 文件名筛选）。 */
export async function getObjects(
  prefix: string,
  bucket: BucketRef = "self",
  page = 1,
  pageSize = 50,
  q?: string,
): Promise<ObjectsResponse> {
  const qs = new URLSearchParams()
  if (prefix) qs.set("prefix", prefix)
  if (q) qs.set("q", q)
  qs.set("bucket", String(bucket))
  qs.set("page", String(page))
  qs.set("page_size", String(pageSize))
  const res = await fetch(`/api/objects?${qs}`, { headers: headers() })
  return handle<ObjectsResponse>(res)
}

/** GET /api/server-files — 本地文件列表（SERVER_FILE_ROOT）。 */
export async function getServerFiles(): Promise<ServerFilesResponse> {
  const res = await fetch("/api/server-files", { headers: headers() })
  return handle<ServerFilesResponse>(res)
}

/** DELETE /api/server-files — 删除本地文件。 */
export async function deleteServerFile(
  path: string,
): Promise<{ deleted: boolean; path: string }> {
  const res = await fetch(`/api/server-files?path=${encodeURIComponent(path)}`, {
    method: "DELETE",
    headers: headers(),
  })
  return handle(res)
}

/** 本地文件下载链接（服务器 → 浏览器）。 */
export function serverFileDownloadUrl(path: string): string {
  return `/server-file/download?path=${encodeURIComponent(path)}&apikey=${encodeURIComponent(getApiKey())}`
}
