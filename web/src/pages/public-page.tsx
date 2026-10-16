import * as React from "react"
import { RefreshCw, Search } from "lucide-react"

import { type Datasource, type FileItem } from "@/lib/types"
import { formatBytes } from "@/lib/utils"
import { useLiveStats } from "@/lib/use-live-stats"
import { getFiles, getScripts } from "@/lib/api"
import { useTerminalFileRefresh } from "@/lib/use-terminal-file-refresh"
import { useBuckets } from "@/lib/use-buckets"
import { useJobs } from "@/lib/use-jobs"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { FetchUrlDialog } from "@/components/action-dialogs"
import { ConnectionStatus } from "@/components/job-status-bar"
import { PublicFilesTable, type ActiveFileStatus } from "@/components/public-files-table"
import { ThemeToggle } from "@/components/theme-toggle"

/** 统计卡：大数字一行（"/" 分隔多指标）+ 下方标签；可选 hint 悬停提示。 */
function StatCard({ label, value, hint }: { label: string; value: React.ReactNode | undefined; hint?: string }) {
  const card = (
    <div className="rounded-xl border bg-card px-4 py-3 whitespace-nowrap">
      <div className="text-2xl font-bold tabular-nums">{value ?? "—"}</div>
      <div className="text-xs text-muted-foreground">{label}</div>
    </div>
  )
  if (!hint) return card
  return (
    <Tooltip>
      <TooltipTrigger asChild>{card}</TooltipTrigger>
      <TooltipContent className="max-w-sm">{hint}</TooltipContent>
    </Tooltip>
  )
}

/**
 * 对外展示页（/dashboard）：只读的文件 × 桶同步状态矩阵 + 链接录入。
 * 无管理操作（任务控制/桶管理/编辑删除均无）；同步状态经 Socket.IO 实时推送。
 */
function PublicPage() {
  const { buckets } = useBuckets()

  const [files, setFiles] = React.useState<FileItem[]>([])

  /** 单条文件记录更新（任务终态后行级 patch，不触发全表重载）。 */
  const patchFile = React.useCallback((updated: FileItem) => {
    setFiles((prev) => prev.map((f) => (f.id === updated.id ? updated : f)))
  }, [])
  const [scripts, setScripts] = React.useState<Datasource[]>([])
  const [total, setTotal] = React.useState(0)
  // 统计卡片：挂载拉一次 + 定时轮询，页面隐藏时暂停（只更新数字节点，不整页重建）
  const { stats, reload: reloadStats } = useLiveStats(10_000)
  const [page, setPage] = React.useState(1)
  const [pageSize] = React.useState(20)
  const [loading, setLoading] = React.useState(true)

  const [q, setQ] = React.useState("")
  const [debouncedQ, setDebouncedQ] = React.useState("")
  const [status, setStatus] = React.useState("all")
  const [sortBy, setSortBy] = React.useState("")
  const [sortDir, setSortDir] = React.useState<"asc" | "desc">("asc")
  const [refreshing, setRefreshing] = React.useState(false)

  /** 表头排序：同列翻转方向，新列默认升序；翻页回到第 1 页。 */
  const handleSort = React.useCallback((col: string) => {
    setPage(1)
    if (sortBy === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortBy(col)
      setSortDir("asc")
    }
  }, [sortBy])

  // 搜索防抖：q 变化后 400ms 才更新 debouncedQ（触发列表请求）
  React.useEffect(() => {
    const t = setTimeout(() => setDebouncedQ(q), 400)
    return () => clearTimeout(t)
  }, [q])

  // 数据源列表：仅供「新增文件」弹窗下拉
  React.useEffect(() => {
    getScripts()
      .then(setScripts)
      .catch((e) => console.error(e))
  }, [])

  // 文件列表：分页 / 状态筛选 / 排序 / 防抖后的搜索词 变化时加载
  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    getFiles({
      page,
      page_size: pageSize,
      q: debouncedQ || undefined,
      status: status === "all" ? undefined : status,
      sort_by: (sortBy || undefined) as "filename" | "size" | "delivered_records" | "created_at" | undefined,
      sort_order: sortBy ? sortDir : undefined,
    })
      .then((data) => {
        if (cancelled) return
        setFiles(data.items)
        setTotal(data.total)
      })
      .catch((e) => console.error(e))
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [page, pageSize, debouncedQ, status, sortBy, sortDir])

  // 强制重新拉取当前页（供刷新按钮、录入完成回调使用）
  const loadFiles = React.useCallback(async () => {
    setLoading(true)
    try {
      const data = await getFiles({
        page,
        page_size: pageSize,
        q: debouncedQ || undefined,
        status: status === "all" ? undefined : status,
        sort_by: (sortBy || undefined) as "filename" | "size" | "delivered_records" | "created_at" | undefined,
        sort_order: sortBy ? sortDir : undefined,
      })
      setFiles(data.items)
      setTotal(data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, pageSize, debouncedQ, status, sortBy, sortDir])

  const refresh = async () => {
    setRefreshing(true)
    await loadFiles()
    reloadStats()
    setRefreshing(false)
  }

  // 任务终态后行级刷新（上传/下载完成后对应行的桶列自动变绿）
  useTerminalFileRefresh(files, patchFile)

  // 活跃任务（排队中/上传中）→ object_key 映射：正在传输的文件即使不在当前页
  // （或尚未登记为文件记录）也在列表展示实时状态。同一 object_key 多个任务时
  // 取最新（id 大者），与 files.job_id 的「最近一次任务」语义一致。
  const { jobs } = useJobs()
  const activeMap = React.useMemo(() => {
    const map = new Map<string, ActiveFileStatus>()
    for (const job of Object.values(jobs)) {
      if (job.status !== "queued" && job.status !== "uploading") continue
      const target: ActiveFileStatus["target"] =
        (job.kind === "upload" || job.kind === "transfer") && job.bucket_id != null ? job.bucket_id : "local"
      const prev = map.get(job.object_key)
      if (!prev || prev.job.id < job.id) {
        map.set(job.object_key, { job, target })
      }
    }
    return map
  }, [jobs])

  return (
    <div className="min-h-screen bg-background">
      <main className="mx-auto max-w-[1600px] px-4 py-8 sm:px-6">
        {/* Header */}
        <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold tracking-tight sm:text-3xl">BucketHub 文件同步</h1>
            <ConnectionStatus />
          </div>
          <ThemeToggle />
        </header>

        {/* 统计卡：总文件 / 预交付 / 已交付（大数字，"/" 分隔 文件/大小/会话） */}
        <div className="mb-6 flex flex-wrap gap-3">
          <StatCard
            label="总文件（文件 / 大小）"
            value={stats ? `${stats.total} / ${formatBytes(stats.total_bytes)}` : undefined}
          />
          <StatCard
            label="待处理（文件 / 大小）"
            value={stats ? `${stats.pending} / ${formatBytes(stats.pending_bytes)}` : undefined}
          />
          <StatCard
            label="处理中（文件 / 大小）"
            value={stats ? `${stats.processing} / ${formatBytes(stats.processing_bytes)}` : undefined}
          />
          <StatCard
            label="预交付（文件 / 大小 / 会话）"
            value={stats ? `${stats.pre_delivered_files} / ${formatBytes(stats.pre_delivered_bytes)} / ${stats.pre_delivered_sessions.toLocaleString()}` : undefined}
            hint={stats ? `大小为交付产物字节（已打包暂存、尚未上传 OBS）；源 zip 合计 ${formatBytes(stats.pre_delivered_source_bytes)}` : undefined}
          />
          <StatCard
            label="已交付（文件 / 大小 / 会话）"
            value={stats ? `${stats.delivered_files} / ${formatBytes(stats.delivered_bytes)} / ${stats.synced.toLocaleString()}` : undefined}
            hint={stats ? `大小为交付产物字节（已上传 OBS 的分片合计）；源 zip 合计 ${formatBytes(stats.delivered_source_bytes)}` : undefined}
          />
        </div>

        {/* 工具栏：链接录入 + 搜索 + 状态筛选 + 刷新 */}
        <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
          <FetchUrlDialog defaultPrefix="" scripts={scripts} onDone={refresh} />
          <div className="flex min-w-0 flex-1 flex-wrap items-center justify-end gap-2 sm:flex-none">
            <div className="relative min-w-0 flex-1 sm:flex-none">
              <Search className="absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                className="w-full pl-8 sm:w-64 lg:w-[28rem]"
                placeholder="搜索，多关键词空格分隔..."
                value={q}
                onChange={(e) => setQ(e.target.value)}
              />
            </div>
            <Select value={status} onValueChange={(v) => { setStatus(v); setPage(1) }}>
              <SelectTrigger className="w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部状态</SelectItem>
                <SelectItem value="pending">待处理</SelectItem>
                <SelectItem value="processing">处理中</SelectItem>
                <SelectItem value="processed">处理完成</SelectItem>
                <SelectItem value="synced">已交付</SelectItem>
                <SelectItem value="failed">失败</SelectItem>
                <SelectItem value="deleted">已删除</SelectItem>
              </SelectContent>
            </Select>
            <Button variant="ghost" size="icon" onClick={refresh} title="刷新" disabled={refreshing}>
              <RefreshCw className={refreshing ? "size-4 animate-spin" : "size-4"} />
            </Button>
          </div>
        </div>

        {/* 文件 × 桶同步状态矩阵（只读，含正在上传/下载的活跃任务行） */}
        <PublicFilesTable
          data={files}
          scripts={scripts}
          buckets={buckets}
          activeMap={activeMap}
          total={total}
          page={page}
          pageSize={pageSize}
          loading={loading}
          onPageChange={setPage}
          sortBy={sortBy}
          sortDir={sortDir}
          onSortChange={handleSort}
        />
      </main>
    </div>
  )
}

export default PublicPage
