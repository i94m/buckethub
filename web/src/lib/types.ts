/** 桶基础信息（/api/buckets 列表项 / /api/auth 的 buckets 数组）。 */
export interface BucketInfo {
  id: number
  name: string
  bucket_name: string
  /** 历史遗留别名 self / bucket2 / beijing（仅存量库迁移行带有，其余为 null）。 */
  legacy_key: string | null
  is_default: boolean
}

/** 后端 files 表的一行（GET /api/files 返回的 item）。 */
export interface FileItem {
  id: number
  job_id: number | null
  object_key: string
  filename: string | null
  md5: string | null
  size: number
  bucket: string
  source_url: string | null
  /** 已上传到的桶 id 集合（file_uploads 表派生，替代旧 uploaded_* 三列）。 */
  uploaded_bucket_ids: number[]
  status: string // pending | synced | failed | deleted
  datasource_id: number | null
  /** 文件级下载源：url=下载链接 / local=服务器本地路径 / bucket=指定桶 / null=未配置。 */
  download_kind: "url" | "local" | "bucket" | null
  /** kind=bucket 时指向的桶 id。 */
  download_bucket_id: number | null
  local_path: string | null // 已下载到服务器 SERVER_FILE_ROOT 的相对路径
  /** 最终交付内容的会话条数（B 类流水线交付的记录数；未交付为 null）。 */
  delivered_records: number | null
  created_at: number
  updated_at: number
  synced_at: number | null
  error: string | null
}

/** GET /api/files 的响应。 */
export interface FilesResponse {
  items: FileItem[]
  total: number
  page: number
  page_size: number
}

/** 桶对象列表里的一项（GET /api/objects 返回）。 */
export interface BucketObject {
  key: string
  size: number
  last_modified: number
}

/** GET /api/objects 的响应。 */
export interface ObjectsResponse {
  prefix: string
  bucket: string
  objects: BucketObject[]
  total: number
  page: number
  page_size: number
}

/** 本地文件列表里的一项（SERVER_FILE_ROOT 目录）。 */
export interface ServerFile {
  path: string
  size: number
  absolute: string
}

/** GET /api/server-files 的响应。 */
export interface ServerFilesResponse {
  root: string | null
  total_size: number
  files: ServerFile[]
}

/** 数据源（原脚本）：name + script_path（允许空，仅记录不执行）。 */
export interface Datasource {
  id: number
  name: string
  script_path: string | null
  description: string | null
  created_at: number
  updated_at: number
}

/** 上传/下载两条并行道的并发上限（GET/POST /api/concurrency）。 */
export interface ConcurrencyInfo {
  upload: number
  download: number
  /** 可调最大值（MAX_CONCURRENCY）。 */
  max: number
}

/** GET / 根路由的引导信息。 */
export interface AppInfo {
  app: string
  /** 当前 key 的角色：admin=管理（可写）/ public=首页浏览（只读）。 */
  role?: "admin" | "public"
  bucket: string
  default_prefix: string
  bucket_private: boolean | null
  bucket_private_note: string
  /** 并行道并发上限（顶部下拉框数据源）。 */
  concurrency: ConcurrencyInfo
  /** 旧字段从 buckets 表派生（后端保留，机器人兼容）。 */
  beijing_enabled: boolean
  beijing_bucket: string
  bucket2_enabled: boolean
  bucket2_bucket: string
  buckets: BucketInfo[]
}

/** GET /api/bucket-health 的单项结果。 */
export interface BucketHealthEntry {
  ok: boolean
  error: string | null
  /** head_bucket 耗时（毫秒）；连不通时可能为 null。 */
  latency_ms: number | null
  /** head_bucket 的 HTTP 状态码。 */
  status_code: number | null
  /** 实际使用的 S3 endpoint URL。 */
  endpoint: string | null
  /** 寻址风格：auto / virtual / path。 */
  addressing_style: string | null
  /** 桶所在 region。 */
  region: string | null
  /** 版本控制状态：Enabled / Suspended / Disabled。 */
  versioning: string | null
  /** 是否公开读（ACL 含 AllUsers）；null = 无法检测。 */
  public: boolean | null
  /** 冗余模式（single-az / multi-az），部分服务才返回。 */
  redundancy: string | null
  /** 默认存储类，部分服务才返回。 */
  storage_class: string | null
}

/** GET /api/bucket-health 的响应（按桶动态返回）。 */
export interface BucketHealth {
  buckets: Array<BucketInfo & { health: BucketHealthEntry }>
}

/** Socket.IO job_update 事件（与后端 job_payload 对齐）。 */
export interface JobUpdate {
  id: number
  kind: string
  status: string
  filename: string
  object_key: string
  progress: number
  size: number
  error: string | null
  source: string | null
  created_at: number
  started_at: number | null
  finished_at: number | null
  cancelled: boolean
  paused: boolean
  /** 历史兼容字段（队列固定逐个执行，true/false 行为一致）。 */
  serial: boolean
  bucket_id: number | null
  bucket_name: string | null
}

/** 单个 job 的实时进度统计（由前端基于 job_update 序列推算）。 */
export interface JobStats {
  /** 0~100 的百分比。 */
  percent: number
  /** 瞬时速率（字节/秒）。 */
  rate: number
  /** 剩余字节数。 */
  remaining: number
  /** 预计剩余秒数（无速率时为 null）。 */
  etaSec: number | null
  /** 已用秒数（从 started_at 起，无则 null）。 */
  elapsedSec: number | null
}
