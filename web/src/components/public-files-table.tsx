import * as React from "react"
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  useReactTable,
} from "@tanstack/react-table"
import { type Bucket } from "@/lib/api"
import { type Datasource, type FileItem, type JobUpdate } from "@/lib/types"
import { cn, formatBytes, formatTime } from "@/lib/utils"
import { useJobs } from "@/lib/use-jobs"
import { JobProgressBadge } from "@/components/progress-cell"
import { StatusText } from "@/components/status-text"
import { FileStatusBadge, SortableHead } from "@/components/sortable-head"

/** 活跃（排队中/传输中）任务按 object_key 匹配出的行级状态。 */
export interface ActiveFileStatus {
  job: JobUpdate
  /** 本地列（fetch/download）或某桶列（upload）。 */
  target: "local" | number
}
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Skeleton } from "@/components/ui/skeleton"
import { NumberPagination } from "@/components/number-pagination"
import { CopyButton } from "@/components/ui/copy-button"

/**
 * 只读同步状态单元（公开页矩阵核心）：
 * 排队→黄字 / 传输中→进度徽章 / 已同步→绿字 / 否则→灰字（失败 hover 错误）。
 * active 由父级按 object_key 从活跃任务解析（file.job_id 快照滞后时的兜底）。
 */
function ReadOnlySyncCell({
  file,
  target,
  active,
}: {
  file: FileItem
  target: "local" | number
  active?: ActiveFileStatus
}) {
  const { jobs } = useJobs()
  const job = active?.job ?? (file.job_id ? jobs[file.job_id] : undefined)
  const isMyJob = job && (target === "local"
    ? job.kind === "fetch" || job.kind === "download"
    : (job.kind === "upload" || job.kind === "transfer") && job.bucket_id === target)

  if (isMyJob && job.status === "queued") {
    return <StatusText tone="queued">排队中</StatusText>
  }
  if (isMyJob && job.status === "uploading") {
    return <JobProgressBadge file={file} job={job} />
  }

  const synced = target === "local"
    ? !!file.local_path
    : file.uploaded_bucket_ids.includes(target)
  return (
    <StatusText
      tone={synced ? "active" : "muted"}
      error={file.status === "failed" ? file.error : null}
    >
      {synced
        ? target === "local" ? "已存在" : "已同步"
        : target === "local" ? "不存在" : "未同步"}
    </StatusText>
  )
}

/** 只读来源单元格（与管理页文件来源列同口径，去掉复制/操作）。 */
function DownloadSourceLabel({ file }: { file: FileItem }) {
  const label =
    file.download_kind === "url"
      ? "网络链接"
      : file.download_kind === "local"
        ? "本地"
        : file.download_kind === "bucket"
          ? `桶 ${file.download_bucket_id ?? "?"}`
          : "未配置"
  return <span className="text-xs text-muted-foreground">{label}</span>
}

/** 骨架行各列的宽度类（按 columns 顺序对齐）。 */
function skeletonWidths(bucketCount: number): string[] {
  const widths = [
    "w-16",   // 数据源
    "w-16",   // 文件来源
    "w-48",   // 文件名称
    "w-20",   // 大小
    "w-20",   // 交付条数
    "w-16",   // 处理状态
    "w-16",   // 本地
  ]
  for (let i = 0; i < bucketCount; i++) widths.push("w-16") // 各桶列
  widths.push("w-24") // 创建时间
  return widths
}

interface PublicFilesTableProps {
  data: FileItem[]
  /** 数据源列表（数据源 id → 名称映射，与管理页一致）。 */
  scripts: Datasource[]
  /** 全部桶（仅启用桶生成列，与管理页一致）。 */
  buckets: Bucket[]
  /** object_key → 活跃任务状态（正在上传/下载的文件，可能尚未登记为文件记录）。 */
  activeMap?: Map<string, ActiveFileStatus>
  total: number
  page: number
  pageSize: number
  loading: boolean
  onPageChange: (page: number) => void
  /** 服务端排序状态（文件名称/大小/交付条数/创建时间）。 */
  sortBy?: string
  sortDir?: "asc" | "desc"
  onSortChange?: (col: string) => void
}

/** 公开页只读表格：文件 × 桶同步状态矩阵（无选择/操作列）。 */
export function PublicFilesTable({
  data,
  scripts,
  buckets,
  activeMap,
  total,
  page,
  pageSize,
  loading,
  onPageChange,
  sortBy,
  sortDir,
  onSortChange,
}: PublicFilesTableProps) {
  const enabledBuckets = React.useMemo(
    () => buckets.filter((b) => b.enabled),
    [buckets],
  )

  /** 数据源 id → 名称（与管理页同口径，供「数据源」列）。 */
  const datasourceName = React.useMemo(() => {
    const map = new Map<number, string>()
    scripts.forEach((s) => map.set(s.id, s.name))
    return map
  }, [scripts])

  // 展示行 = 当前页文件记录 ∪ 活跃任务对应的文件（去重：同一 object_key 只保留一条，
  // 文件记录优先——其大小/时间等元数据完整，活跃状态经 activeMap 叠加到单元格）。
  const rows = React.useMemo(() => {
    if (!activeMap || activeMap.size === 0) return data
    const seen = new Set(data.map((f) => f.object_key))
    const extras: FileItem[] = []
    for (const status of activeMap.values()) {
      if (seen.has(status.job.object_key)) continue
      seen.add(status.job.object_key)
      extras.push({
        id: -status.job.id, // 负 job id，避免与真实文件 id 冲突
        job_id: status.job.id,
        object_key: status.job.object_key,
        filename: status.job.filename,
        md5: null,
        size: status.job.size,
        bucket: "",
        source_url: status.job.source,
        uploaded_bucket_ids: [],
        status: "pending",
        datasource_id: null,
        download_kind: null,
        download_bucket_id: null,
        local_path: null,
        delivered_records: null,
        created_at: status.job.created_at,
        updated_at: status.job.created_at,
        synced_at: null,
        error: null,
      })
    }
    return extras.length ? [...extras, ...data] : data
  }, [data, activeMap])

  const columns = React.useMemo<ColumnDef<FileItem>[]>(
    () => {
      const cols: ColumnDef<FileItem>[] = [
        {
          accessorKey: "datasource_id",
          header: "数据源",
          cell: ({ row }) => {
            const name = row.original.datasource_id
              ? datasourceName.get(row.original.datasource_id)
              : null
            if (!name) return <span className="text-muted-foreground">—</span>
            return <span className="text-xs">{name}</span>
          },
        },
        {
          id: "download_source",
          header: "文件来源",
          cell: ({ row }) => <DownloadSourceLabel file={row.original} />,
        },
        {
          accessorKey: "filename",
          header: () => (
            <SortableHead label="文件名称" col="filename" sortBy={sortBy} sortDir={sortDir} onSort={onSortChange} />
          ),
          cell: ({ row }) => {
            const f = row.original
            if (!f.filename) {
              return <span className="text-muted-foreground">—</span>
            }
            return (
              <div className="flex items-center gap-1">
                <span className="block max-w-[11rem] truncate sm:max-w-[24rem]">{f.filename}</span>
                <CopyButton
                  value={f.filename}
                  title="复制文件名"
                  className="size-5 p-0.5 hover:bg-muted"
                />
              </div>
            )
          },
        },
        {
          accessorKey: "size",
          header: () => (
            <div className="min-w-[5rem] text-center">
              <SortableHead label="大小" col="size" sortBy={sortBy} sortDir={sortDir} onSort={onSortChange} />
            </div>
          ),
          cell: ({ row }) => (
            <div className="text-center tabular-nums text-muted-foreground">
              {formatBytes(row.original.size)}
            </div>
          ),
        },
        {
          accessorKey: "delivered_records",
          header: () => (
            <div className="min-w-[4.5rem] text-center">
              <SortableHead label="交付条数" col="delivered_records" sortBy={sortBy} sortDir={sortDir} onSort={onSortChange} />
            </div>
          ),
          cell: ({ row }) => (
            <div className="text-center tabular-nums text-muted-foreground">
              {row.original.delivered_records == null ? "—" : row.original.delivered_records.toLocaleString()}
            </div>
          ),
        },
        {
          id: "file_status",
          header: () => <div className="min-w-[4rem] text-center">处理状态</div>,
          cell: ({ row }) => (
            <div className="flex justify-center">
              <FileStatusBadge file={row.original} />
            </div>
          ),
        },
        {
          id: "local",
          header: () => <div className="min-w-[4rem] text-center">本地</div>,
          cell: ({ row }) => (
            <div className="flex justify-center">
              <ReadOnlySyncCell
                file={row.original}
                target="local"
                active={activeMap?.get(row.original.object_key)}
              />
            </div>
          ),
        },
      ]
      enabledBuckets.forEach((bucket) => {
        cols.push({
          id: `bucket-${bucket.id}`,
          header: () => <div className="min-w-[4rem] text-center">{bucket.name}</div>,
          cell: ({ row }) => (
            <div className="flex justify-center">
              <ReadOnlySyncCell
                file={row.original}
                target={bucket.id}
                active={activeMap?.get(row.original.object_key)}
              />
            </div>
          ),
        })
      })
      cols.push({
        accessorKey: "created_at",
        header: () => (
          <SortableHead label="创建时间" col="created_at" sortBy={sortBy} sortDir={sortDir} onSort={onSortChange} />
        ),
        cell: ({ row }) => (
          <span className="whitespace-nowrap text-muted-foreground">
            {formatTime(row.original.created_at)}
          </span>
        ),
      })
      return cols
    },
    [enabledBuckets, activeMap, datasourceName, sortBy, sortDir, onSortChange],
  )

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    manualPagination: true,
    pageCount: Math.max(1, Math.ceil(total / pageSize)),
    getRowId: (row) => String(row.id),
  })

  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const skelWidths = skeletonWidths(enabledBuckets.length)

  return (
    <div className="space-y-3">
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id}>
                {hg.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: 8 }).map((_, i) => (
                <TableRow key={`skeleton-${i}`} className="hover:bg-transparent">
                  {columns.map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton
                        className={cn(
                          "h-4",
                          skelWidths[j] ?? "w-20",
                          j === 3 && "mx-auto", // 大小列居中
                        )}
                      />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id} className="group">
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-32 text-center text-muted-foreground">
                  暂无文件记录，可通过「新增文件」登记。
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {/* 分页 */}
      <NumberPagination
        page={page}
        pageCount={pageCount}
        total={total}
        onPageChange={onPageChange}
      />
    </div>
  )
}
