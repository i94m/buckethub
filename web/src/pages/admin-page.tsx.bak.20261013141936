import * as React from "react"
import { ArrowUpDown, Cloud, FolderOpen, HardDrive, RefreshCw, Search } from "lucide-react"

import { type BucketHealthEntry, type Datasource, type FileItem } from "@/lib/types"
import { getBucketHealth, getFiles, getScripts } from "@/lib/api"
import { useTerminalFileRefresh } from "@/lib/use-terminal-file-refresh"
import { useAppInfo } from "@/components/auth-guard"
import { useBuckets } from "@/lib/use-buckets"
import { useJobs } from "@/lib/use-jobs"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Tabs, TabsContent } from "@/components/ui/tabs"
import {
  NavigationMenu,
  NavigationMenuContent,
  NavigationMenuItem,
  NavigationMenuLink,
  NavigationMenuList,
  NavigationMenuTrigger,
  navigationMenuTriggerStyle,
} from "@/components/ui/navigation-menu"
import { FilesDataTable } from "@/components/files-data-table"
import { FetchUrlDialog } from "@/components/action-dialogs"
import { ConnectionStatus, JobsTable } from "@/components/job-status-bar"
import { ServerFilesSection } from "@/components/server-files-section"
import { BucketBrowserSection } from "@/components/bucket-browser-section"
import { BucketManager } from "@/components/bucket-manager"
import { DatasourceManager } from "@/components/datasource-manager"
import { HealthDot } from "@/components/bucket-health"
import { ThemeToggle } from "@/components/theme-toggle"

/** 管理后台（/admin）：文件库 / 上传管理 / 本地文件 / 各桶浏览全功能页。 */
function AdminPage() {
  const appInfo = useAppInfo()
  const { buckets } = useBuckets()
  const defaultPrefix = appInfo?.default_prefix ?? ""
  const bucketPrivateNote = appInfo?.bucket_private_note ?? ""
  const bucketPrivate = appInfo?.bucket_private ?? null

  // 桶连通性：导航菜单内状态点 + hover 详情共用
  const [healthMap, setHealthMap] = React.useState<Record<number, BucketHealthEntry>>({})
  const checkHealth = React.useCallback(async () => {
    try {
      const data = await getBucketHealth()
      const map: Record<number, BucketHealthEntry> = {}
      data.buckets.forEach((b) => {
        map[b.id] = b.health
      })
      setHealthMap(map)
    } catch {
      /* 检测失败保持原状态 */
    }
  }, [])
  React.useEffect(() => {
    checkHealth()
  }, [checkHealth])

  const [files, setFiles] = React.useState<FileItem[]>([])

  /** 单条文件记录更新（检测存在性等轻量操作，不触发全表重载）。 */
  const patchFile = React.useCallback((updated: FileItem) => {
    setFiles((prev) => prev.map((f) => (f.id === updated.id ? updated : f)))
  }, [])
  const [scripts, setScripts] = React.useState<Datasource[]>([])
  const [total, setTotal] = React.useState(0)
  const [page, setPage] = React.useState(1)
  const [pageSize] = React.useState(20)
  const [loading, setLoading] = React.useState(true)

  const [q, setQ] = React.useState("")
  const [debouncedQ, setDebouncedQ] = React.useState("")
  const [status, setStatus] = React.useState("all")
  const [refreshing, setRefreshing] = React.useState(false)
  /** 顶层模块切换：files=源文件管理 / transfers=上传管理 / local=本地文件 / bucket:<id>=各桶（动态）。 */
  const [section, setSection] = React.useState<string>("files")

  // 当前选中的桶被停用/删除时，回退到源文件管理
  React.useEffect(() => {
    if (section.startsWith("bucket:")) {
      const id = Number(section.slice("bucket:".length))
      const stillEnabled = buckets.some((b) => b.id === id && b.enabled)
      if (!stillEnabled) setSection("files")
    }
  }, [section, buckets])

  // 搜索防抖：q 变化后 400ms 才更新 debouncedQ（触发列表请求）
  React.useEffect(() => {
    const t = setTimeout(() => setDebouncedQ(q), 400)
    return () => clearTimeout(t)
  }, [q])

  // 数据源列表：初始加载 + 数据源管理变更后刷新
  const loadScripts = React.useCallback(() => {
    getScripts()
      .then(setScripts)
      .catch((e) => console.error(e))
  }, [])
  React.useEffect(() => {
    loadScripts()
  }, [loadScripts])

  // 文件列表：分页 / 状态 / 防抖后的搜索词 变化时加载
  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    getFiles({
      page,
      page_size: pageSize,
      q: debouncedQ || undefined,
      status: status === "all" ? undefined : status,
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
  }, [page, pageSize, debouncedQ, status])

  // 强制重新拉取当前页（供刷新按钮、删除/录入完成回调使用）
  const loadFiles = React.useCallback(async () => {
    setLoading(true)
    try {
      const data = await getFiles({
        page,
        page_size: pageSize,
        q: debouncedQ || undefined,
        status: status === "all" ? undefined : status,
      })
      setFiles(data.items)
      setTotal(data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, pageSize, debouncedQ, status])

  const refresh = async () => {
    setRefreshing(true)
    await loadFiles()
    setRefreshing(false)
  }

  // 任务结束时只更新受影响的行（按 job_id 匹配当前页），不整表刷新
  const { jobs } = useJobs()
  /** 进行中的任务数（上传管理菜单角标）。 */
  const activeJobs = Object.values(jobs).filter(
    (j) => j.status === "queued" || j.status === "uploading",
  ).length
  /** 启用中的桶（存储桶下拉菜单项）。 */
  const enabledBuckets = buckets.filter((b) => b.enabled)
  useTerminalFileRefresh(files, patchFile)

  return (
    <div className="min-h-screen bg-background">
      <main className="mx-auto max-w-[1600px] px-4 py-8 sm:px-6">
        {/* Header */}
        <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
          <div className="flex flex-col gap-1">
            <h1 className="text-xl font-bold tracking-tight sm:text-3xl">BucketHub</h1>
            <ConnectionStatus />
          </div>
          <div className="flex items-center gap-2">
            <BucketManager />
            <DatasourceManager scripts={scripts} onChanged={loadScripts} />
            <ThemeToggle />
          </div>
        </header>

        {/* Bucket 公开告警 */}
        {bucketPrivate === false && (
          <div className="mb-4 flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            ⚠️ {bucketPrivateNote}
          </div>
        )}

        {/* 顶部模块菜单（NavigationMenu）：源文件管理 / 上传管理 / 本地文件 / 桶（下拉）；小屏单行横向滑动 */}
        <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
          <div className="-mx-4 overflow-x-auto px-4 pb-1 [scrollbar-width:none] sm:mx-0 sm:overflow-visible sm:px-0 sm:pb-0 [&::-webkit-scrollbar]:hidden">
            <NavigationMenu viewport={false} className="max-w-none flex-none justify-start">
              <NavigationMenuList className="max-w-none flex-nowrap justify-start gap-1">
              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "files"}
                  onClick={() => setSection("files")}
                >
                  <FolderOpen className="size-4" /> 源文件管理
                </button>
              </NavigationMenuItem>
              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "transfers"}
                  onClick={() => setSection("transfers")}
                >
                  <ArrowUpDown className="size-4" /> 上传管理
                  {activeJobs > 0 && (
                    <span className="text-[10px] tabular-nums text-muted-foreground">
                      {activeJobs}
                    </span>
                  )}
                </button>
              </NavigationMenuItem>
              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "local"}
                  onClick={() => setSection("local")}
                >
                  <HardDrive className="size-4" /> 本地文件
                </button>
              </NavigationMenuItem>
              <NavigationMenuItem>
                <NavigationMenuTrigger
                  className="gap-1.5 px-3"
                  data-active={section.startsWith("bucket:")}
                >
                  <Cloud className="size-4" /> 存储桶
                </NavigationMenuTrigger>
                <NavigationMenuContent className="w-[11rem] p-1">
                  <ul className="grid gap-1">
                    {enabledBuckets.length === 0 && (
                      <li className="px-2 py-1.5 text-xs text-muted-foreground">
                        暂无启用的桶
                      </li>
                    )}
                    {enabledBuckets.map((b) => (
                      <li key={b.id}>
                        <NavigationMenuLink
                          asChild
                          data-active={section === `bucket:${b.id}`}
                          onClick={() => setSection(`bucket:${b.id}`)}
                          className="cursor-pointer flex-row items-center gap-2"
                        >
                          <button type="button" className="flex flex-1 items-center gap-2 text-left">
                            <HealthDot entry={healthMap[b.id]} />
                            <span className="min-w-0 truncate">{b.name}</span>
                          </button>
                        </NavigationMenuLink>
                      </li>
                    ))}
                  </ul>
                </NavigationMenuContent>
              </NavigationMenuItem>
            </NavigationMenuList>
          </NavigationMenu>
          </div>

          {/* 搜索 / 状态筛选只在源文件管理模块显示 */}
          {section === "files" && (
            <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto">
              <FetchUrlDialog
                defaultPrefix={defaultPrefix}
                scripts={scripts}
                onDone={refresh}
              />
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
                  <SelectItem value="synced">已上传</SelectItem>
                  <SelectItem value="pending">待上传</SelectItem>
                  <SelectItem value="failed">失败</SelectItem>
                  <SelectItem value="deleted">已删除</SelectItem>
                </SelectContent>
              </Select>
              <Button variant="ghost" size="icon" onClick={refresh} title="刷新" disabled={refreshing}>
                <RefreshCw className={refreshing ? "size-4 animate-spin" : "size-4"} />
              </Button>
            </div>
          )}
        </div>

        {/* 模块内容（section 驱动，Tabs 仅作受控容器复用挂载/卸载行为） */}
        <Tabs value={section} onValueChange={setSection} className="mb-4">

          {/* 源文件管理：文件库表格 */}
          <TabsContent value="files">
            <FilesDataTable
              data={files}
              scripts={scripts}
              buckets={buckets}
              total={total}
              page={page}
              pageSize={pageSize}
              loading={loading}
              onPageChange={setPage}
              onDeleted={loadFiles}
              onFileUpdated={patchFile}
            />
          </TabsContent>

          {/* 上传管理：传输列表（上传/下载任务全宽） */}
          <TabsContent value="transfers">
            <JobsTable />
          </TabsContent>

          {/* 本地文件：服务器文件目录 */}
          <TabsContent value="local">
            <ServerFilesSection
              defaultPrefix={defaultPrefix}
              scripts={scripts}
              onUploaded={refresh}
            />
          </TabsContent>

          {/* 各桶：动态顶层 Tab（桶顺序沿用桶管理拖动排序） */}
          {buckets.map((b) => (
            <TabsContent key={b.id} value={`bucket:${b.id}`}>
              <BucketBrowserSection
                title={`${b.name} · ${b.bucket_name}`}
                bucketId={b.id}
                defaultPrefix={defaultPrefix}
                bucketName={b.name}
                bucketKey={b.bucket_name}
                health={healthMap[b.id]}
              />
            </TabsContent>
          ))}
        </Tabs>
      </main>
    </div>
  )
}

export default AdminPage
