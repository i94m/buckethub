import * as React from "react"
import { io } from "socket.io-client"
import { Loader2, Play, Square, X } from "lucide-react"

import { getApiKey } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"

interface ProcessJob {
  id: number
  label: string
  source_path: string
  status: string
  stage: string
  progress: number
  stages: string[]
  stats?: { records?: number; kept_sessions?: number }
  error?: string
  created_at: number
}

function headers() {
  return { "X-API-Key": getApiKey(), "Content-Type": "application/json" }
}

const STATUS_LABEL: Record<string, string> = {
  queued: "排队中",
  running: "处理中",
  completed: "已完成",
  failed: "失败",
  cancelled: "已取消",
  cancelling: "取消中",
}

function statusBadge(status: string) {
  const map: Record<string, string> = {
    completed: "bg-green-600",
    failed: "bg-red-600",
    running: "bg-blue-600",
    queued: "bg-gray-400",
    cancelled: "bg-gray-500",
    cancelling: "bg-yellow-500",
  }
  return (
    <span
      className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium text-white ${map[status] || "bg-gray-400"}`}
    >
      {STATUS_LABEL[status] || status}
    </span>
  )
}

export function ProcessSection() {
  const [connected, setConnected] = React.useState(false)
  const [jobs, setJobs] = React.useState<ProcessJob[]>([])
  const [open, setOpen] = React.useState(false)
  const [sourcePath, setSourcePath] = React.useState("")
  const [label, setLabel] = React.useState("")
  const [creating, setCreating] = React.useState(false)
  const [error, setError] = React.useState("")

  const refresh = React.useCallback(async () => {
    try {
      const res = await fetch("/api/process/jobs", { headers: headers() })
      if (res.ok) setJobs(await res.json())
    } catch {
      /* 轮询失败忽略 */
    }
  }, [])

  React.useEffect(() => {
    refresh()
    const t = setInterval(refresh, 3000)
    const socket = io({ auth: { apikey: getApiKey() } })
    socket.on("connect", () => setConnected(true))
    socket.on("disconnect", () => setConnected(false))
    socket.on("process_snapshot", (rows: ProcessJob[]) => setJobs(rows))
    socket.on("process_update", (row: ProcessJob) =>
      setJobs((prev) => {
        const idx = prev.findIndex((j) => j.id === row.id)
        if (idx === -1) return [row, ...prev]
        const next = [...prev]
        next[idx] = row
        return next
      })
    )
    return () => {
      clearInterval(t)
      socket.close()
    }
  }, [refresh])

  async function createJob() {
    setCreating(true)
    setError("")
    try {
      const res = await fetch("/api/process/jobs", {
        method: "POST",
        headers: headers(),
        body: JSON.stringify({ source_path: sourcePath, label }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data?.error || "创建失败")
      setOpen(false)
      setSourcePath("")
      setLabel("")
      refresh()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setCreating(false)
    }
  }

  async function cancelJob(id: number) {
    await fetch(`/api/process/jobs/${id}/cancel`, {
      method: "POST",
      headers: headers(),
    })
    refresh()
  }

  const activeJobs = jobs.filter((j) => ["queued", "running"].includes(j.status)).length

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <span
            className={`inline-block size-2 rounded-full ${connected ? "bg-green-500" : "bg-gray-400"}`}
          />
          {connected ? "实时连接" : "未连接（3 秒轮询）"}
        </div>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button size="sm">
              <Play className="mr-1 size-4" /> 新建处理任务
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>新建数据处理任务</DialogTitle>
            </DialogHeader>
            <div className="space-y-3">
              <div>
                <label className="text-sm font-medium">原始数据路径（服务器路径，.jsonl 文件或目录）</label>
                <Input
                  value={sourcePath}
                  onChange={(e) => setSourcePath(e.target.value)}
                  placeholder="/root/data/xxx/raw_sessions.jsonl"
                  className="mt-1"
                />
              </div>
              <div>
                <label className="text-sm font-medium">任务名称（可选）</label>
                <Input
                  value={label}
                  onChange={(e) => setLabel(e.target.value)}
                  placeholder="例如：xxx 批次 B 类处理"
                  className="mt-1"
                />
              </div>
              <p className="text-xs text-muted-foreground">
                流水线：转换 → 质检 → 同会话选最长 → 脱敏 → 打包。产物在任务目录 output/ 下。
              </p>
              {error && <p className="text-sm text-red-600">{error}</p>}
              <Button className="w-full" onClick={createJob} disabled={creating || !sourcePath}>
                {creating ? <Loader2 className="mr-1 size-4 animate-spin" /> : <Play className="mr-1 size-4" />}
                创建并开始
              </Button>
            </div>
          </DialogContent>
        </Dialog>
      </div>

      <div className="space-y-3">
        {jobs.length === 0 && (
          <p className="py-6 text-center text-sm text-muted-foreground">暂无处理任务</p>
        )}
        {jobs.map((job) => (
          <div key={job.id} className="rounded-lg border p-4">
            <div className="flex items-center justify-between gap-2">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  {statusBadge(job.status)}
                  <span className="truncate font-medium">#{job.id} {job.label}</span>
                </div>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">{job.source_path}</p>
              </div>
              {["queued", "running"].includes(job.status) && (
                <Button variant="outline" size="sm" onClick={() => cancelJob(job.id)}>
                  <Square className="mr-1 size-3.5" /> 取消
                </Button>
              )}
            </div>
            <div className="mt-3">
              <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
                <span>{job.stage || STATUS_LABEL[job.status]}</span>
                <span className="tabular-nums">{job.progress?.toFixed(1) ?? 0}%</span>
              </div>
              <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
                <div
                  className="h-full rounded-full bg-blue-600 transition-all"
                  style={{ width: `${Math.min(100, job.progress ?? 0)}%` }}
                />
              </div>
              {job.stages && job.stages.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1">
                  {job.stages.map((s, i) => (
                    <span
                      key={i}
                      className={`rounded px-1.5 py-0.5 text-[10px] ${
                        s === "done"
                          ? "bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-400"
                          : s === "running"
                            ? "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-400"
                            : "bg-muted text-muted-foreground"
                      }`}
                    >
                      {["转换", "质检", "选最长", "脱敏", "打包"][i] || s}
                    </span>
                  ))}
                </div>
              )}
              {job.error && (
                <p className="mt-2 text-xs text-red-600">{job.error}</p>
              )}
              {job.stats?.kept_sessions !== undefined && (
                <p className="mt-1 text-xs text-muted-foreground">
                  会话 {job.stats.kept_sessions}
                </p>
              )}
            </div>
          </div>
        ))}
      </div>
      {activeJobs > 0 && (
        <p className="text-xs text-muted-foreground">{activeJobs} 个任务处理中</p>
      )}
      <div className="flex items-center gap-1 text-xs text-muted-foreground">
        <X className="size-3" /> 取消仅在阶段边界生效（大文件阶段内最长 2 分钟生效）
      </div>
    </div>
  )
}
