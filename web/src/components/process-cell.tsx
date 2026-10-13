import * as React from "react"

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"

export interface ProcessJobLite {
  id: number
  label?: string
  source_path: string
  status: string
  stage: string
  progress: number
  stats_json?: string | null
  error?: string | null
  created_at?: number
  finished_at?: number | null
}

const STAGE_NAMES = ["转换", "质检", "选最长", "脱敏", "打包"]

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
      className={`inline-block whitespace-nowrap rounded-full px-1.5 py-0.5 text-[10px] font-medium text-white ${map[status] || "bg-gray-400"}`}
    >
      {STATUS_LABEL[status] || status}
    </span>
  )
}

/** 匹配规则：source_path 与文件路径完全一致，或以 "/" + 文件相对路径 结尾。 */
function matchJob(jobs: ProcessJobLite[], filePath: string): ProcessJobLite | undefined {
  if (!filePath) return undefined
  const rel = filePath.replace(/^\/+/, "")
  return jobs
    .filter((j) => j.source_path === filePath || j.source_path.endsWith("/" + rel))
    .sort((a, b) => b.id - a.id)[0]
}

export function ProcessCell({
  filePath,
  jobs,
}: {
  filePath: string
  jobs: ProcessJobLite[]
}) {
  const job = matchJob(jobs, filePath)
  const [open, setOpen] = React.useState(false)

  if (!job) {
    return <span className="text-xs text-muted-foreground">—</span>
  }

  let stages: { name: string; status: string }[] = []
  try {
    stages = job.stats_json ? (JSON.parse(job.stats_json).stages ?? []) : []
  } catch {
    stages = []
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <button
          type="button"
          className="mx-auto flex max-w-[7rem] cursor-pointer items-center justify-center gap-1"
          title="点击查看清洗五阶段详情"
        >
          {statusBadge(job.status)}
          {job.status === "running" && (
            <span className="tabular-nums text-[10px] text-muted-foreground">
              {(job.progress ?? 0).toFixed(0)}%
            </span>
          )}
        </button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="text-left">
            #{job.id} {job.label || "处理任务"} · {statusBadge(job.status)}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          <p className="break-all text-xs text-muted-foreground">{job.source_path}</p>
          <div>
            <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
              <span>{job.stage || STATUS_LABEL[job.status]}</span>
              <span className="tabular-nums">{(job.progress ?? 0).toFixed(1)}%</span>
            </div>
            <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-blue-600 transition-all"
                style={{ width: `${Math.min(100, job.progress ?? 0)}%` }}
              />
            </div>
          </div>
          <div className="grid gap-1">
            {(stages.length ? stages : STAGE_NAMES.map((name) => ({ name, status: "pending" }))).map(
              (s, i) => (
                <div key={i} className="flex items-center gap-2 text-xs">
                  <span
                    className={`inline-block size-1.5 rounded-full ${
                      s.status === "done"
                        ? "bg-green-500"
                        : s.status === "running"
                          ? "bg-blue-500 animate-pulse"
                          : s.status === "failed" || s.status === "cancelled"
                            ? "bg-red-500"
                            : "bg-gray-300 dark:bg-gray-600"
                    }`}
                  />
                  <span className="w-12">{STAGE_NAMES[i] || s.name}</span>
                  <span className="text-muted-foreground">{s.status}</span>
                </div>
              )
            )}
          </div>
          {job.error && <p className="text-xs text-red-600">{job.error}</p>}
          <p className="text-xs text-muted-foreground">
            完整审计（去重/脱敏/打包日志）见「数据处理」Tab 与服务器 audit/ 目录。
          </p>
        </div>
      </DialogContent>
    </Dialog>
  )
}
