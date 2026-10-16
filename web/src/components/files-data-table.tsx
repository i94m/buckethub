import * as React from "react"
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  useReactTable,
} from "@tanstack/react-table"
import { Check, CloudUpload, Download, HardDriveDownload, Info, ArrowRightLeft, Loader2, MoreVertical, Pause, Pencil, Play, RefreshCw, Trash2, X } from "lucide-react"
import { toast } from "sonner"

import { type Datasource, type FileItem } from "@/lib/types"
import { cn, formatBytes, formatTime } from "@/lib/utils"
import {
  type Bucket,
  cancelJob,
  checkFileExists,
  deleteFile,
  deleteObject,
  deleteServerFile,
  downloadServerFromBucket,
  getFile,
  pauseJob,
  renameObject,
  resumeJob,
  transferToBucket,
  updateFile,
  uploadFileToBucket,
} from "@/lib/api"
import { useJobs } from "@/lib/use-jobs"
import { useBuckets } from "@/lib/use-buckets"
import { useConfirm } from "@/lib/use-confirm"
import { JobProgressBadge } from "@/components/progress-cell"
import { StatusText } from "@/components/status-text"
import { FileStatusBadge, SortableHead } from "@/components/sortable-head"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
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
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { CopyButton } from "@/components/ui/copy-button"
import { ProcessCell, type ProcessJobLite } from "@/components/process-cell"

/** 骨架行各列的宽度类（按 columns 顺序对齐，模拟真实数据宽度）。 */
function skeletonWidths(bucketCount: number): string[] {
  const widths = [
    "w-4",    // select
    "w-16",   // 数据源
    "w-28",   // 文件来源
    "w-28",   // 文件名称
    "w-32",   // ObjectKey
    "w-20",   // 大小（居中）
    "w-16",   // 本地
    "w-16",   // 清洗
  ]
  for (let i = 0; i < bucketCount; i++) widths.push("w-16") // 各桶列
  widths.push("w-24", "w-8") // 创建时间、操作
  return widths
}

interface FilesDataTableProps {
  data: FileItem[]
  scripts: Datasource[]
  /** 全部桶（含禁用；顺序：sort_order → id，桶管理拖动排序）；仅启用桶生成列。 */
  buckets: Bucket[]
  total: number
  page: number
  pageSize: number
  loading: boolean
  onPageChange: (page: number) => void
  onDeleted: () => void
  onFileUpdated?: (file: FileItem) => void
  /** 数据处理任务列表（清洗列状态来源）。 */
  processJobs?: ProcessJobLite[]
  /** 服务端排序状态（文件名称/大小/交付条数/创建时间）。 */
  sortBy?: string
  sortDir?: "asc" | "desc"
  onSortChange?: (col: string) => void
}

/** 截断长文本 + tooltip 显示完整值。 */
function Truncate({ value, className }: { value: string | null; className?: string }) {
  if (!value) return <span className="text-muted-foreground">—</span>
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn("block max-w-[12rem] truncate cursor-default sm:max-w-[24rem]", className)}>{value}</span>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm break-all">{value}</TooltipContent>
    </Tooltip>
  )
}

/** 检测中状态：重新检测在 ⋮ 菜单里触发，菜单随即关闭，需在单元格内展示 loading。 */
function CheckingText() {
  return (
    <span className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground">
      <Loader2 className="size-3 animate-spin" />
      检测中…
    </span>
  )
}

/** 已存在状态：绿色圆形填充对号图标，hover 显示说明。 */
function ExistsBadge() {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className="flex size-3 shrink-0 items-center justify-center rounded-full bg-green-500 text-white"
          aria-label="已存在"
        >
          <Check className="size-2" strokeWidth={3.5} />
        </span>
      </TooltipTrigger>
      <TooltipContent>已存在</TooltipContent>
    </Tooltip>
  )
}

/** 不存在状态：小号灰色实心圆点（与绿色实心对勾形成对照），失败时 hover 展示错误。 */
function EmptyBadge({ label, error }: { label: string; error?: string | null }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="flex items-center" aria-label={label}>
          <span className="size-2 shrink-0 rounded-full bg-muted-foreground/50" />
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs break-all">{error ?? label}</TooltipContent>
    </Tooltip>
  )
}

/** 单元格内「更多操作」⋮ 菜单（一格有多个操作时收敛于此）。 */
function CellMenu({
  items,
}: {
  items: Array<{
    icon: React.ComponentType<{ className?: string }>
    label: string
    onClick: () => void
    busy?: boolean
    destructive?: boolean
  }>
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          title="更多操作"
          className="inline-flex size-5 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          <MoreVertical className="size-3.5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        {items.map((it) => (
          <DropdownMenuItem
            key={it.label}
            onClick={it.onClick}
            disabled={it.busy}
            variant={it.destructive ? "destructive" : "default"}
            className="px-2 py-1 text-xs"
          >
            {it.busy ? <Loader2 className="size-3 animate-spin" /> : <it.icon className="size-3" />}
            {it.label}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

/** 小型任务操作按钮组：暂停/恢复 + 取消（排队 / 上传中均可点击）。 */
function CancelJobBtn({ file }: { file: FileItem }) {
  const { jobs } = useJobs()
  const [busy, setBusy] = React.useState(false)
  const [confirm, confirmDialog] = useConfirm()
  const job = file.job_id ? jobs[file.job_id] : undefined
  if (!job) return null

  const handlePauseResume = async () => {
    setBusy(true)
    try {
      const r = job.paused ? await resumeJob(job.id) : await pauseJob(job.id)
      toast(r.message)
    } catch (e) {
      toast.error("操作失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const handleCancel = async () => {
    if (!await confirm({
      title: "取消任务",
      description: `确认取消任务「${job.filename}」？`,
      confirmText: "取消任务",
      destructive: true,
    })) return
    setBusy(true)
    try {
      const r = await cancelJob(job.id)
      toast(r.message)
    } catch (e) {
      toast.error("取消失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const btnClass = "inline-flex size-5 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:opacity-50"

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button type="button" className={btnClass} title="任务操作">
            <MoreVertical className="size-3.5" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuItem onClick={handlePauseResume} disabled={busy} className="px-2 py-1 text-xs">
            {busy ? (
              <Loader2 className="size-3 animate-spin" />
            ) : job.paused ? (
              <Play className="size-3" />
            ) : (
              <Pause className="size-3" />
            )}
            {job.paused ? "继续" : "暂停"}
          </DropdownMenuItem>
          <DropdownMenuItem variant="destructive" onClick={handleCancel} disabled={busy} className="px-2 py-1 text-xs">
            <X className="size-3" />
            取消任务
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      {confirmDialog}
    </>
  )
}

/** 重新检测存在性的逻辑（独立按钮与「更多操作」菜单项共用）。 */
function useCheckExist(
  fileId: number,
  target: "local" | number,
  onFileUpdated?: (file: FileItem) => void,
) {
  const [busy, setBusy] = React.useState(false)
  const run = async () => {
    setBusy(true)
    try {
      const r = await checkFileExists(fileId, target)
      if (r.exists) {
        toast.success("文件存在")
      } else {
        toast("文件不存在，已更新状态", { description: "可重新下载或上传" })
      }
      if (r.file && onFileUpdated) {
        onFileUpdated(r.file)
      }
    } catch (e) {
      toast.error("检测失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }
  return { busy, run }
}

/** 排队中黄字 + 取消按钮。 */
function QueuedLabel({ file }: { file: FileItem }) {
  return (
    <div className="flex items-center gap-0.5">
      <StatusText tone="queued">排队中</StatusText>
      <CancelJobBtn file={file} />
    </div>
  )
}

/** 校验完整 ObjectKey：非空、无 '..' 路径穿越、不以 / 开头结尾。 */
function validateKeyStrict(value: string): string | null {
  const trimmed = value.trim()
  if (!trimmed) return "ObjectKey 不能为空"
  if (/^\/|\/$/.test(trimmed)) return "不能以 / 开头或结尾"
  const parts = trimmed.replace(/\\/g, "/").split("/")
  if (parts.some((p) => p === "..")) return "路径不能包含 '..'"
  if (parts.some((p) => p === "")) return "路径不能包含连续的 /"
  return null
}

/** 重命名桶内对象 Dialog：修改完整 ObjectKey（copy + delete，后端同步 files 记录）。 */
function RenameKeyDialog({
  file,
  bucketId,
  bucketLabel,
  onClose,
  onDone,
}: {
  file: FileItem
  bucketId: number
  bucketLabel: string
  onClose: () => void
  onDone: () => void
}) {
  const [newKey, setNewKey] = React.useState(file.object_key)
  const [busy, setBusy] = React.useState(false)
  /** 提交后（attempted）才展示校验结果，避免打开弹窗即满屏红色。 */
  const [attempted, setAttempted] = React.useState(false)

  const keyErrorRaw = validateKeyStrict(newKey)
  const sameErrorRaw =
    newKey.trim() && newKey.trim() === file.object_key ? "新 ObjectKey 与原始相同" : null
  const hasError = !!keyErrorRaw || !!sameErrorRaw
  const keyError = attempted ? keyErrorRaw : null
  const sameError = attempted ? sameErrorRaw : null

  const submit = async () => {
    if (hasError) {
      setAttempted(true)
      return
    }
    const to = newKey.trim().replace(/^\/+|\/+$/g, "")
    setBusy(true)
    try {
      await renameObject(file.object_key, to, bucketId)
      toast.success("已重命名", { description: `${file.object_key} → ${to}` })
      onDone()
    } catch (e) {
      toast.error("重命名失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open onOpenChange={(v) => { if (!v && !busy) onClose() }}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>重命名（{bucketLabel}）</DialogTitle>
        </DialogHeader>
        <div className="space-y-2 py-2">
          <Label htmlFor="rename-object-key">新 ObjectKey（目录/文件名）</Label>
          <Input
            id="rename-object-key"
            value={newKey}
            onChange={(e) => setNewKey(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !busy && submit()}
            placeholder="如 opus5_delivery_20260812/a.zip"
            className={cn(
              "font-mono text-sm",
              (keyError || sameError) && "border-destructive focus-visible:ring-destructive",
            )}
            autoFocus
          />
          {keyError || sameError ? (
            <p className="text-xs text-destructive">{keyError ?? sameError}</p>
          ) : (
            <p className="text-xs text-muted-foreground">
              通过「复制 + 删除」实现，大文件可能耗时较长；文件记录的 ObjectKey 会同步更新。
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button onClick={submit} disabled={busy}>
            {busy ? "重命名中…" : "确认"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** 上传到桶 Dialog：输入完整 ObjectKey（目录/文件名），支持编辑。 */
function UploadKeyDialog({
  file,
  bucketId,
  bucketLabel,
  onClose,
  onDone,
}: {
  file: FileItem
  bucketId: number
  bucketLabel: string
  onClose: () => void
  onDone: () => void
}) {
  /** 初始 key = 文件记录的 object_key（严格模式，可编辑）。 */
  const [objectKey, setObjectKey] = React.useState(file.object_key)
  const [busy, setBusy] = React.useState(false)
  /** 提交后（attempted）才展示校验结果，避免打开弹窗即满屏红色。 */
  const [attempted, setAttempted] = React.useState(false)

  const keyErrorRaw = validateKeyStrict(objectKey)
  const hasError = !!keyErrorRaw
  const keyError = attempted ? keyErrorRaw : null

  const submit = async () => {
    if (hasError) {
      setAttempted(true)
      return
    }
    const key = objectKey.trim().replace(/^\/+|\/+$/g, "")
    setBusy(true)
    try {
      const r = await uploadFileToBucket(file.id, bucketId, key)
      toast.success(r.message)
      onDone()
    } catch (e) {
      toast.error("上传失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const onEnter = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !busy) submit()
  }

  return (
    <Dialog open onOpenChange={(v) => { if (!v && !busy) onClose() }}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>上传到{bucketLabel}</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="upload-key">ObjectKey（目录/文件名）</Label>
            <Input
              id="upload-key"
              value={objectKey}
              onChange={(e) => setObjectKey(e.target.value)}
              onKeyDown={onEnter}
              placeholder="如 opus5_delivery_20260812/a.zip"
              className={cn("font-mono text-sm", keyError && "border-destructive focus-visible:ring-destructive")}
              autoFocus
            />
            {keyError && (
              <p className="text-xs text-destructive">{keyError}</p>
            )}
          </div>
          <p className="text-xs text-muted-foreground">任务进入上传队列，与其它上传任务按顺序逐个执行。</p>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button onClick={submit} disabled={busy}>
            {busy ? "提交中…" : "上传"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** 行级刷新：只拉取本行最新数据并 patch，不刷新整表（无 onFileUpdated 时回退整表刷新）。 */
function refreshFileRow(
  fileId: number,
  onUpdated: () => void,
  onFileUpdated?: (file: FileItem) => void,
) {
  if (onFileUpdated) {
    getFile(fileId).then(onFileUpdated).catch((e) => console.error(e))
  } else {
    onUpdated()
  }
}

/** 本地列：排队→黄字 / 传输中→进度 / 已存在→绿勾+菜单 / 否则→不存在+菜单。 */
function LocalCell({ file, onUpdated, onFileUpdated }: { file: FileItem; onUpdated: () => void; onFileUpdated?: (file: FileItem) => void }) {
  const { jobs } = useJobs()
  const [busy, setBusy] = React.useState(false)
  const [confirm, confirmDialog] = useConfirm()
  const check = useCheckExist(file.id, "local", onFileUpdated)
  const job = file.job_id ? jobs[file.job_id] : undefined
  const isMyJob = job && (job.kind === "fetch" || job.kind === "download")

  if (isMyJob && job!.status === "queued") return <QueuedLabel file={file} />
  if (isMyJob && job!.status === "uploading") return (
    <div className="flex items-center gap-0.5">
      <JobProgressBadge file={file} />
      <CancelJobBtn file={file} />
    </div>
  )

  /** 已存在 → 标记为不存在（仅清 local_path 记录，不删服务器文件）。 */
  const handleUnmark = async () => {
    setBusy(true)
    try {
      const r = await updateFile(file.id, { local_path: null })
      toast.success("已标记为不存在")
      if (r.file && onFileUpdated) onFileUpdated(r.file)
      else onUpdated()
    } catch (e) {
      toast.error("操作失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  /** 已存在 → 删除服务器上的本地文件（确认后删除，成功后记录自动回到不存在）。 */
  const handleDeleteLocal = async () => {
    if (!await confirm({
      title: "删除本地文件",
      description: `确认删除服务器上的「${file.local_path}」？该操作不可恢复（仅删本地文件，不影响桶内对象）。`,
      confirmText: "删除",
      destructive: true,
    })) return
    setBusy(true)
    try {
      await deleteServerFile(file.local_path!)
      const r = await updateFile(file.id, { local_path: null })
      toast.success("本地文件已删除")
      if (r.file && onFileUpdated) onFileUpdated(r.file)
      else onUpdated()
    } catch (e) {
      toast.error("删除失败", { description: (e as Error).message })
      // 磁盘文件可能已被删但记录未清，刷新行以对齐真实状态
      check.run()
    } finally {
      setBusy(false)
    }
  }

  if (file.local_path) return (
    <div className="flex items-center gap-0.5">
      {check.busy ? <CheckingText /> : <ExistsBadge />}
      <CellMenu
        items={[
          { icon: RefreshCw, label: "重新检测", onClick: check.run, busy: check.busy },
          { icon: HardDriveDownload, label: "标记为不存在", onClick: handleUnmark, busy },
          { icon: Trash2, label: "删除", onClick: handleDeleteLocal, busy, destructive: true },
        ]}
      />
      {confirmDialog}
    </div>
  )

  const handleDownload = async () => {
    setBusy(true)
    try {
      const r = await downloadServerFromBucket(file.id, undefined)
      toast.success(r.message)
      refreshFileRow(file.id, onUpdated, onFileUpdated)
    } catch (e) {
      toast.error("下载失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const failed = file.status === "failed"
  return (
    <div className="flex items-center gap-0.5">
      {check.busy ? (
        <CheckingText />
      ) : (
        <EmptyBadge label="不存在" error={failed ? file.error : undefined} />
      )}
      <CellMenu
        items={[
          { icon: HardDriveDownload, label: "下载", onClick: handleDownload, busy },
          { icon: RefreshCw, label: "重新检测", onClick: check.run, busy: check.busy },
        ]}
      />
    </div>
  )
}

/** 桶列（通用）：排队→黄字 / 上传中→进度 / 已存在→绿勾+菜单 / 否则→未上传+菜单（上传/从源直传）。 */
function BucketCell({ file, bucket, onUpdated, onFileUpdated }: { file: FileItem; bucket: Bucket; onUpdated: () => void; onFileUpdated?: (file: FileItem) => void }) {
  const { jobs } = useJobs()
  const [dialogOpen, setDialogOpen] = React.useState(false)
  const [renameOpen, setRenameOpen] = React.useState(false)
  const [downloading, setDownloading] = React.useState(false)
  const [transferring, setTransferring] = React.useState(false)
  const [deleting, setDeleting] = React.useState(false)
  const [confirm, confirmDialog] = useConfirm()
  const check = useCheckExist(file.id, bucket.id, onFileUpdated)
  const job = file.job_id ? jobs[file.job_id] : undefined
  const uploaded = file.uploaded_bucket_ids.includes(bucket.id)
  const isMyJob = job && (job.kind === "upload" || job.kind === "transfer") && job.bucket_id === bucket.id

  if (isMyJob && job!.status === "queued") return <QueuedLabel file={file} />
  if (isMyJob && job!.status === "uploading") return (
    <div className="flex items-center gap-0.5">
      <JobProgressBadge file={file} />
      <CancelJobBtn file={file} />
    </div>
  )

  const failed = file.status === "failed" && !!file.local_path && !uploaded

  const handleDownload = async () => {
    setDownloading(true)
    try {
      const r = await downloadServerFromBucket(file.id, bucket.id)
      toast.success(r.message)
      refreshFileRow(file.id, onUpdated, onFileUpdated)
    } catch (e) {
      toast.error("下载失败", { description: (e as Error).message })
    } finally {
      setDownloading(false)
    }
  }

  /** 已上传 → 从桶中删除对象（按 object_key，删后状态回退未上传）。 */
  const handleDeleteObject = async () => {
    const ok = await confirm({
      title: "从桶中删除",
      description: `删除后不可恢复，本地副本保留。`,
      confirmText: "删除",
      destructive: true,
    })
    if (!ok) return
    setDeleting(true)
    try {
      await deleteObject(file.object_key, bucket.id)
      toast.success(`已从${bucket.name}删除`)
      refreshFileRow(file.id, onUpdated, onFileUpdated)
    } catch (e) {
      toast.error("删除失败", { description: (e as Error).message })
    } finally {
      setDeleting(false)
    }
  }

  /** 无本地副本时：从文件的下载源（桶/链接/本地路径）流式直传到本桶。 */
  const handleTransfer = async () => {
    setTransferring(true)
    try {
      const r = await transferToBucket(file.id, bucket.id)
      toast.success(r.message)
      refreshFileRow(file.id, onUpdated, onFileUpdated)
    } catch (e) {
      toast.error("直传失败", { description: (e as Error).message })
    } finally {
      setTransferring(false)
    }
  }

  /** 已上传 → 重命名（改完整 ObjectKey，copy + delete，同步 files 记录）。 */
  const handleRenameDone = () => {
    setRenameOpen(false)
    refreshFileRow(file.id, onUpdated, onFileUpdated)
  }

  if (uploaded) {
    return (
      <>
        <div className="flex items-center gap-0.5">
          {check.busy ? <CheckingText /> : <ExistsBadge />}
          <CellMenu
            items={[
              { icon: Download, label: "下载", onClick: handleDownload, busy: downloading },
              { icon: Pencil, label: "重命名", onClick: () => setRenameOpen(true) },
              { icon: RefreshCw, label: "重新检测", onClick: check.run, busy: check.busy },
              { icon: Trash2, label: "删除", onClick: handleDeleteObject, busy: deleting, destructive: true },
            ]}
          />
        </div>
        {renameOpen && (
          <RenameKeyDialog
            file={file}
            bucketId={bucket.id}
            bucketLabel={bucket.name}
            onClose={() => setRenameOpen(false)}
            onDone={handleRenameDone}
          />
        )}
        {confirmDialog}
      </>
    )
  }

  if (file.local_path) {
    return (
      <>
        <div className="flex items-center gap-0.5">
          {check.busy ? (
            <CheckingText />
          ) : (
            <EmptyBadge label="未上传" error={failed ? file.error : undefined} />
          )}
          <CellMenu
            items={[
              { icon: CloudUpload, label: "上传", onClick: () => setDialogOpen(true) },
              { icon: ArrowRightLeft, label: "从源直传", onClick: handleTransfer, busy: transferring },
              { icon: RefreshCw, label: "重新检测", onClick: check.run, busy: check.busy },
            ]}
          />
        </div>
        {dialogOpen && (
          <UploadKeyDialog
            file={file}
            bucketId={bucket.id}
            bucketLabel={bucket.name}
            onClose={() => setDialogOpen(false)}
            onDone={() => {
              setDialogOpen(false)
              refreshFileRow(file.id, onUpdated, onFileUpdated)
            }}
          />
        )}
      </>
    )
  }

  return (
    <div className="flex items-center gap-0.5">
      {check.busy ? (
        <CheckingText />
      ) : (
        <EmptyBadge label="待上传" error={file.status === "failed" && !uploaded ? file.error : undefined} />
      )}
      <CellMenu
        items={[
          { icon: ArrowRightLeft, label: "从源直传", onClick: handleTransfer, busy: transferring },
          { icon: RefreshCw, label: "重新检测", onClick: check.run, busy: check.busy },
        ]}
      />
    </div>
  )
}

/** 文件来源列：纯文本展示（hover tooltip 显示对应地址），编辑入口在「编辑文件」弹窗。 */
function DownloadSourceCell({ file }: { file: FileItem }) {
  const bucketMissing = file.download_kind === "bucket" && file.download_bucket_id == null

  /** 显示文本：网络链接 / 服务器路径 / 桶名 / 未配置。 */
  const label =
    file.download_kind === "url"
      ? "网络链接"
      : file.download_kind === "local"
        ? "本地"
        : file.download_kind === "bucket"
          ? `桶 ${file.download_bucket_id ?? "?"}`
          : "未配置"

  const copyValue = file.source_url ?? (file.download_kind === "bucket" ? file.object_key : null)

  /** hover 提示：来源对应的地址（链接 / 服务器路径 / 桶内 ObjectKey）。 */
  const sourceTip = file.source_url
    ? file.source_url
    : file.download_kind === "bucket"
      ? `桶内 ObjectKey: ${file.object_key}`
      : "未配置来源"

  return (
    <div className="flex items-center gap-1">
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className={cn(
              "block max-w-[6rem] truncate text-xs",
              file.download_kind ? "text-foreground" : "text-muted-foreground",
              bucketMissing && "text-destructive",
            )}
          >
            {label}
          </span>
        </TooltipTrigger>
        <TooltipContent className="max-w-sm break-all">{sourceTip}</TooltipContent>
      </Tooltip>
      {copyValue && (
        <CopyButton
          value={copyValue}
          title="复制链接/标识"
          className="size-5 p-0.5 hover:bg-muted"
        />
      )}
    </div>
  )
}

export function FilesDataTable({
  data,
  scripts,
  buckets,
  total,
  page,
  pageSize,
  loading,
  onPageChange,
  onDeleted,
  onFileUpdated,
  processJobs = [],
  sortBy,
  sortDir,
  onSortChange,
}: FilesDataTableProps & { processJobs?: ProcessJobLite[] }) {
  const datasourceName = React.useMemo(() => {
    const map = new Map<number, string>()
    scripts.forEach((s) => map.set(s.id, s.name))
    return map
  }, [scripts])

  /** 只展示启用桶的列（停用桶不参与业务）。 */
  const enabledBuckets = React.useMemo(
    () => buckets.filter((b) => b.enabled),
    [buckets],
  )

  const [rowSelection, setRowSelection] = React.useState<Record<string, boolean>>({})
  const [editingFile, setEditingFile] = React.useState<FileItem | null>(null)
  const [detailsFile, setDetailsFile] = React.useState<FileItem | null>(null)

  const columns = React.useMemo<ColumnDef<FileItem>[]>(
    () => {
      const cols: ColumnDef<FileItem>[] = [
        {
          id: "select",
          header: ({ table }) => (
            <Checkbox
              checked={table.getIsAllPageRowsSelected()}
              indeterminate={table.getIsSomePageRowsSelected()}
              onCheckedChange={(v) => table.toggleAllPageRowsSelected(v)}
              aria-label="全选"
            />
          ),
          cell: ({ row }) => (
            <Checkbox
              checked={row.getIsSelected()}
              onCheckedChange={(v) => row.toggleSelected(v)}
              aria-label="选择"
            />
          ),
        },
        {
          accessorKey: "datasource_id",
          header: "数据源",
          cell: ({ row }) => {
            const name = row.original.datasource_id
              ? datasourceName.get(row.original.datasource_id)
              : null
            return <Truncate value={name ?? null} className="text-xs" />
          },
        },
        {
          id: "download_source",
          header: "文件来源",
          cell: ({ row }) => <DownloadSourceCell file={row.original} />,
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
                <span className="block max-w-[11rem] truncate sm:max-w-[26rem]">{f.filename}</span>
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
          cell: ({ row }) => <div className="flex justify-center"><LocalCell file={row.original} onUpdated={onDeleted} onFileUpdated={onFileUpdated} /></div>,
        },
        {
          id: "process",
          header: () => <div className="min-w-[4rem] text-center">清洗</div>,
          cell: ({ row }) => (
            <div className="flex justify-center">
              <ProcessCell
                filePath={row.original.local_path ?? ""}
                jobs={processJobs}
              />
            </div>
          ),
        },
      ]
      enabledBuckets.forEach((bucket) => {
        cols.push({
          id: `bucket-${bucket.id}`,
          header: () => <div className="min-w-[4rem] text-center">{bucket.name}</div>,
          cell: ({ row }) => <div className="flex justify-center"><BucketCell file={row.original} bucket={bucket} onUpdated={onDeleted} onFileUpdated={onFileUpdated} /></div>,
        })
      })
      cols.push(
        {
          accessorKey: "created_at",
          header: () => (
            <SortableHead label="创建时间" col="created_at" sortBy={sortBy} sortDir={sortDir} onSort={onSortChange} />
          ),
          cell: ({ row }) => (
            <span className="whitespace-nowrap text-muted-foreground">
              {formatTime(row.original.created_at)}
            </span>
          ),
        },
        {
          id: "actions",
          header: () => <div className="text-center">操作</div>,
          cell: ({ row }) => (
            <RowActions
              file={row.original}
              onDeleted={onDeleted}
              onEdit={() => setEditingFile(row.original)}
              onDetails={() => setDetailsFile(row.original)}
            />
          ),
        },
      )
      return cols
    },
    [datasourceName, onDeleted, onFileUpdated, enabledBuckets, sortBy, sortDir, onSortChange],
  )

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    manualPagination: true,
    pageCount: Math.max(1, Math.ceil(total / pageSize)),
    getRowId: (row) => String(row.id),
    enableRowSelection: true,
    onRowSelectionChange: setRowSelection,
    state: { pagination: { pageIndex: page - 1, pageSize }, rowSelection },
  })

  const pageCount = Math.max(1, Math.ceil(total / pageSize))

  const selectedFiles = table.getSelectedRowModel().rows.map((r) => r.original)
  const downloadable = selectedFiles.filter((f) => !f.local_path)
  /** 每个桶可批量上传的文件（有本地副本且尚未上传到该桶；仅启用桶）。 */
  const uploadableByBucket = new Map<number, FileItem[]>()
  enabledBuckets.forEach((b) => {
    uploadableByBucket.set(
      b.id,
      selectedFiles.filter((f) => f.local_path && !f.uploaded_bucket_ids.includes(b.id)),
    )
  })

  const [batchBusy, setBatchBusy] = React.useState(false)

  const batchDownload = async () => {
    if (!downloadable.length) {
      toast("选中的文件都已在服务器上")
      return
    }
    setBatchBusy(true)
    const results = await Promise.allSettled(
      downloadable.map((f) => downloadServerFromBucket(f.id, undefined)),
    )
    const ok = results.filter((r) => r.status === "fulfilled").length
    toast.success(`已入队 ${ok} 个下载任务（按顺序逐个执行）`)
    setRowSelection({})
    setBatchBusy(false)
    // 行级更新：只刷新受影响的行
    downloadable.forEach((f) => refreshFileRow(f.id, onDeleted, onFileUpdated))
  }

  const batchUploadTo = async (bucket: Bucket) => {
    const uploadable = uploadableByBucket.get(bucket.id) ?? []
    if (!uploadable.length) {
      toast("没有可上传的文件（需先下载到服务器）")
      return
    }
    setBatchBusy(true)
    const results = await Promise.allSettled(
      uploadable.map((f) => uploadFileToBucket(f.id, bucket.id)),
    )
    const ok = results.filter((r) => r.status === "fulfilled").length
    toast.success(`已入队 ${ok} 个上传任务（按顺序逐个执行）`)
    setRowSelection({})
    setBatchBusy(false)
    // 行级更新：只刷新受影响的行
    uploadable.forEach((f) => refreshFileRow(f.id, onDeleted, onFileUpdated))
  }

  const skelWidths = skeletonWidths(enabledBuckets.length)

  return (
    <div className="space-y-3">
      {/* 批量操作栏（队列固定逐个执行，无需排队选项） */}
      {selectedFiles.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/50 px-4 py-2">
          <span className="text-sm font-medium">已选 {selectedFiles.length} 项</span>
          <Button size="sm" variant="outline" onClick={batchDownload} disabled={batchBusy}>
            <HardDriveDownload className="size-3.5" />
            批量下载到服务器{downloadable.length > 0 && `（${downloadable.length}）`}
          </Button>
          {enabledBuckets.map((bucket) => {
            const uploadable = uploadableByBucket.get(bucket.id) ?? []
            return (
              <Button
                key={bucket.id}
                size="sm"
                variant="outline"
                onClick={() => batchUploadTo(bucket)}
                disabled={batchBusy || uploadable.length === 0}
                title={uploadable.length === 0 ? "需先下载到服务器" : undefined}
              >
                <CloudUpload className="size-3.5" />
                批量上传到{bucket.name}{uploadable.length > 0 && `（${uploadable.length}）`}
              </Button>
            )
          })}
          <Button size="sm" variant="ghost" onClick={() => setRowSelection({})} disabled={batchBusy}>
            取消选择
          </Button>
        </div>
      )}
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id}>
                {hg.headers.map((header) => (
                  <TableHead
                    key={header.id}
                    className={
                      header.column.id === "actions"
                        ? "sticky right-0 z-10 bg-background shadow-[-1px_0_0_0_var(--border)]"
                        : undefined
                    }
                  >
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
                          j === 5 && "mx-auto", // 大小列居中
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
                    <TableCell
                      key={cell.id}
                      className={
                        cell.column.id === "actions"
                          ? "sticky right-0 z-10 bg-background group-hover:bg-muted/50 shadow-[-1px_0_0_0_var(--border)]"
                          : undefined
                      }
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-32 text-center text-muted-foreground">
                  文件库还没有文件，新增文件或上传后自动登记。
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {/* 分页：统计与翻页按钮同行 */}
      <NumberPagination
        page={page}
        pageCount={pageCount}
        total={total}
        onPageChange={onPageChange}
      />

      {detailsFile && (
        <FileDetailsDialog
          file={detailsFile}
          scripts={scripts}
          buckets={buckets}
          onClose={() => setDetailsFile(null)}
        />
      )}

      {editingFile && (
        <EditFileDialog
          file={editingFile}
          scripts={scripts}
          onClose={() => setEditingFile(null)}
          onSaved={() => {
            const edited = editingFile
            setEditingFile(null)
            refreshFileRow(edited.id, onDeleted, onFileUpdated)
          }}
        />
      )}
    </div>
  )
}

function RowActions({
  file,
  onDeleted,
  onEdit,
  onDetails,
}: {
  file: FileItem
  onDeleted: () => void
  onEdit: () => void
  onDetails: () => void
}) {
  const [busy, setBusy] = React.useState(false)
  const [deleteOpen, setDeleteOpen] = React.useState(false)

  const handleDelete = async () => {
    setBusy(true)
    try {
      await deleteFile(file.id)
      toast.success("已删除")
      setDeleteOpen(false)
      onDeleted()
    } catch (e) {
      toast.error("删除失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="flex items-center justify-center">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            title="更多操作"
            className="inline-flex size-5 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <MoreVertical className="size-3.5" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          <DropdownMenuItem onClick={onDetails}>
            <Info className="size-3.5" /> 详情
          </DropdownMenuItem>
          <DropdownMenuItem onClick={onEdit}>
            <Pencil className="size-3.5" /> 编辑
          </DropdownMenuItem>
          <DropdownMenuItem variant="destructive" onClick={() => setDeleteOpen(true)}>
            <Trash2 className="size-3.5" /> 删除
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除？</AlertDialogTitle>
            <AlertDialogDescription>
              确认删除「{file.filename || file.object_key}」？此操作不可撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busy}>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              disabled={busy}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {busy ? "删除中…" : "删除"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

/** 状态枚举 → 中文标签（提交值仍为后端枚举）。 */
const STATUS_OPTIONS: Array<{ value: string; label: string }> = [
  { value: "pending", label: "待处理" },
  { value: "processed", label: "处理完成" },
  { value: "synced", label: "已同步" },
  { value: "failed", label: "失败" },
  { value: "deleted", label: "已删除" },
  { value: "cancelled", label: "已取消" },
]

/** 编辑文件记录 Modal（基础信息 + 文件级来源；其余字段由列表内操作维护）。 */
function EditFileDialog({
  file,
  scripts,
  onClose,
  onSaved,
}: {
  file: FileItem
  scripts: Datasource[]
  onClose: () => void
  onSaved: () => void
}) {
  const { buckets } = useBuckets()
  const [form, setForm] = React.useState({
    filename: file.filename ?? "",
    size: String(file.size),
    status: file.status,
    datasource_id: file.datasource_id ? String(file.datasource_id) : "",
  })
  /** 文件级来源："none" 对应 NULL（未配置）。 */
  const [downloadKind, setDownloadKind] = React.useState<"none" | "url" | "local" | "bucket">(
    file.download_kind === "url" || file.download_kind === "local" || file.download_kind === "bucket"
      ? file.download_kind
      : "none",
  )
  const [downloadBucketId, setDownloadBucketId] = React.useState(
    file.download_kind === "bucket" && file.download_bucket_id != null
      ? String(file.download_bucket_id)
      : "",
  )
  const [sourceUrl, setSourceUrl] = React.useState(file.source_url ?? "")
  /** 完整对象键（目录/文件名），直接编辑。 */
  const [objectKey, setObjectKey] = React.useState(file.object_key)
  const [busy, setBusy] = React.useState(false)

  /** 桶选项值为 bucket:<id>；当前指向的桶已删除时追加占位项（保存会报桶不存在）。 */
  const bucketDeleted =
    downloadKind === "bucket" &&
    !!downloadBucketId &&
    !buckets.some((b) => String(b.id) === downloadBucketId)
  const downloadValue =
    downloadKind === "bucket" && downloadBucketId ? `bucket:${downloadBucketId}` : downloadKind
  const handleDownloadChange = (v: string) => {
    if (v.startsWith("bucket:")) {
      setDownloadKind("bucket")
      setDownloadBucketId(v.slice("bucket:".length))
    } else {
      setDownloadKind(v as "none" | "url" | "local")
      setDownloadBucketId("")
    }
  }
  const bucketMissing = downloadKind === "bucket" && !downloadBucketId
  const keyErrorRaw = validateKeyStrict(objectKey)
  const hasError = bucketMissing || !!keyErrorRaw
  /** 提交后（attempted）才展示校验结果，避免打开弹窗即满屏红色。 */
  const [attempted, setAttempted] = React.useState(false)
  const keyError = attempted ? keyErrorRaw : null

  const set = (key: keyof typeof form, value: string) =>
    setForm((f) => ({ ...f, [key]: value }))

  const submit = async () => {
    if (hasError) {
      setAttempted(true)
      return
    }
    setBusy(true)
    try {
      await updateFile(file.id, {
        object_key: objectKey.trim().replace(/^\/+|\/+$/g, ""),
        filename: form.filename || null,
        size: Number(form.size) || 0,
        status: form.status,
        datasource_id: form.datasource_id ? Number(form.datasource_id) : null,
        download_kind: downloadKind,
        ...(downloadKind === "bucket" ? { download_bucket_id: Number(downloadBucketId) } : {}),
        ...(downloadKind === "url" || downloadKind === "local"
          ? { source_url: sourceUrl.trim() || null }
          : {}),
      })
      toast.success("已保存")
      onSaved()
    } catch (e) {
      toast.error("保存失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open onOpenChange={(v) => { if (!v && !busy) onClose() }}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>编辑文件记录</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="edit-filename">文件名</Label>
              <Input
                id="edit-filename"
                value={form.filename}
                onChange={(e) => set("filename", e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="edit-size">大小（字节）</Label>
              <Input
                id="edit-size"
                type="number"
                value={form.size}
                onChange={(e) => set("size", e.target.value)}
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="edit-key">ObjectKey（目录/文件名）</Label>
            <Input
              id="edit-key"
              value={objectKey}
              onChange={(e) => setObjectKey(e.target.value)}
              placeholder="如 opus5_delivery_20260812/a.zip"
              className={cn(
                "font-mono text-sm",
                keyError && "border-destructive focus-visible:ring-destructive",
              )}
            />
            {keyError ? (
              <p className="text-xs text-destructive">{keyError}</p>
            ) : (
              <p className="text-xs text-muted-foreground">
                完整对象键；桶中按该键匹配存在性，修改后已上传标记不会自动重算，可对各桶「重新检测」。
              </p>
            )}
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label>状态</Label>
              <Select value={form.status} onValueChange={(v) => set("status", v)}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUS_OPTIONS.map((s) => (
                    <SelectItem key={s.value} value={s.value}>
                      {s.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>数据源</Label>
              <Select value={form.datasource_id} onValueChange={(v) => set("datasource_id", v)}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="不关联" />
                </SelectTrigger>
                <SelectContent>
                  {scripts.map((s) => (
                    <SelectItem key={s.id} value={String(s.id)}>
                      {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>文件来源</Label>
              <Select value={downloadValue} onValueChange={handleDownloadChange}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="未配置（默认）" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">未配置（默认）</SelectItem>
                  <SelectItem value="url">网络链接（URL）</SelectItem>
                  <SelectItem value="local">服务器路径</SelectItem>
                  {buckets.map((b) => (
                    <SelectItem key={b.id} value={`bucket:${b.id}`}>
                      {b.name}
                      {!b.enabled && "（已禁用）"}
                    </SelectItem>
                  ))}
                  {bucketDeleted && (
                    <SelectItem value={`bucket:${downloadBucketId}`}>
                      桶 #{downloadBucketId}（已删除）
                    </SelectItem>
                  )}
                </SelectContent>
              </Select>
              {attempted && bucketMissing && (
                <p className="text-xs text-destructive">请选择来源桶</p>
              )}
            </div>
          </div>
          {(downloadKind === "url" || downloadKind === "local") && (
            <div className="space-y-2">
              <Label htmlFor="edit-source">
                {downloadKind === "url" ? "网络链接（URL）" : "服务器文件路径"}
              </Label>
              <Input
                id="edit-source"
                value={sourceUrl}
                onChange={(e) => setSourceUrl(e.target.value)}
                placeholder={
                  downloadKind === "url"
                    ? "https://example.com/file.zip"
                    : "/data/spider/out/file.zip"
                }
                className="font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">
                {downloadKind === "url"
                  ? "「下载到服务器」时直接抓取该链接。"
                  : "「下载到服务器」时从该路径复制/硬链接到服务器文件目录。"}
              </p>
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button onClick={submit} disabled={busy}>
            {busy ? "保存中…" : "保存"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** 详情行：标签 + 值（可选复制按钮）。 */
function DetailRow({
  label,
  children,
  copyValue,
}: {
  label: string
  children: React.ReactNode
  copyValue?: string | null
}) {
  return (
    <div className="grid grid-cols-[7rem_1fr] items-start gap-2 border-b py-1.5 last:border-b-0">
      <span className="pt-0.5 text-xs text-muted-foreground">{label}</span>
      <div className="flex min-w-0 items-start gap-1">
        <div className="min-w-0 flex-1 break-all text-sm">{children}</div>
        {copyValue && (
          <CopyButton
            value={copyValue}
            title={`复制${label}`}
            className="size-5 p-0.5 hover:bg-muted"
          />
        )}
      </div>
    </div>
  )
}

/** 详情里的空值占位。 */
function Dash() {
  return <span className="text-muted-foreground">—</span>
}

/** 文件详情 Modal：展示该文件记录的全部字段。 */
function FileDetailsDialog({
  file,
  scripts,
  buckets,
  onClose,
}: {
  file: FileItem
  scripts: Datasource[]
  buckets: Bucket[]
  onClose: () => void
}) {
  const datasource = file.datasource_id
    ? scripts.find((s) => s.id === file.datasource_id)
    : undefined
  const downloadBucket =
    file.download_kind === "bucket" && file.download_bucket_id != null
      ? buckets.find((b) => b.id === file.download_bucket_id)
      : undefined
  const uploadedNames = file.uploaded_bucket_ids
    .map((id) => buckets.find((b) => b.id === id)?.name ?? `#${id}`)
    .join("、")

  return (
    <Dialog open onOpenChange={(v) => { if (!v) onClose() }}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>文件详情</DialogTitle>
        </DialogHeader>
        <div className="max-h-[65vh] overflow-y-auto py-2">
          <DetailRow label="ID">{file.id}</DetailRow>
          <DetailRow label="任务 ID">{file.job_id ?? <Dash />}</DetailRow>
          <DetailRow label="ObjectKey" copyValue={file.object_key}>
            <span className="font-mono text-xs">{file.object_key}</span>
          </DetailRow>
          <DetailRow label="文件名" copyValue={file.filename}>
            {file.filename ?? <Dash />}
          </DetailRow>
          <DetailRow label="MD5" copyValue={file.md5}>
            <span className="font-mono text-xs">{file.md5 ?? <Dash />}</span>
          </DetailRow>
          <DetailRow label="大小">
            {formatBytes(file.size)}
            <span className="text-muted-foreground">（{file.size} 字节）</span>
          </DetailRow>
          <DetailRow label="数据源">
            {datasource ? datasource.name : <Dash />}
          </DetailRow>
          <DetailRow label="文件来源">
            {file.download_kind === "url" ? (
              "网络链接"
            ) : file.download_kind === "local" ? (
              "服务器路径"
            ) : file.download_kind === "bucket" ? (
              downloadBucket ? (
                `桶 · ${downloadBucket.name}`
              ) : (
                <span className="text-destructive">
                  桶 #{file.download_bucket_id ?? "?"}（已删除）
                </span>
              )
            ) : (
              <span className="text-muted-foreground">未配置</span>
            )}
          </DetailRow>
          <DetailRow label="链接/标识" copyValue={file.source_url}>
            <span className="font-mono text-xs break-all">
              {file.source_url ?? <Dash />}
            </span>
          </DetailRow>
          <DetailRow label="本地路径" copyValue={file.local_path}>
            <span className="font-mono text-xs break-all">
              {file.local_path ?? <Dash />}
            </span>
          </DetailRow>
          <DetailRow label="已上传桶">{uploadedNames || <Dash />}</DetailRow>
          <DetailRow label="状态">{file.status}</DetailRow>
          {file.error && (
            <DetailRow label="错误信息">
              <span className="text-destructive">{file.error}</span>
            </DetailRow>
          )}
          <DetailRow label="创建时间">{formatTime(file.created_at)}</DetailRow>
          <DetailRow label="更新时间">{formatTime(file.updated_at)}</DetailRow>
          <DetailRow label="同步时间">
            {file.synced_at ? formatTime(file.synced_at) : <Dash />}
          </DetailRow>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
