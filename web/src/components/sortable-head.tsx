import * as React from "react"

import { cn } from "@/lib/utils"
import { type FileItem } from "@/lib/types"
import { StatusText } from "@/components/status-text"

/** 可排序表头：点击切换排序（同列翻转方向，新列升序），激活时显示方向箭头。 */
export function SortableHead({
  label,
  col,
  sortBy,
  sortDir,
  onSort,
  className,
}: {
  label: React.ReactNode
  col: string
  sortBy?: string
  sortDir?: "asc" | "desc"
  onSort?: (col: string) => void
  className?: string
}) {
  const active = sortBy === col
  return (
    <button
      type="button"
      title="点击排序"
      className={cn(
        "inline-flex cursor-pointer select-none items-center gap-0.5 text-inherit hover:text-foreground",
        active && "font-semibold",
        className,
      )}
      onClick={() => onSort?.(col)}
    >
      {label}
      <span className="text-[10px] leading-none text-muted-foreground">
        {active ? (sortDir === "asc" ? "▲" : "▼") : "⇅"}
      </span>
    </button>
  )
}

const STATUS_LABELS: Record<string, { label: string; tone: "active" | "muted" | "queued" }> = {
  pending: { label: "待处理", tone: "muted" },
  processing: { label: "处理中", tone: "queued" },
  processed: { label: "处理完成", tone: "muted" },
  synced: { label: "已交付", tone: "active" },
  failed: { label: "失败", tone: "muted" },
  deleted: { label: "已删除", tone: "muted" },
}

/** 处理状态单元：pending/processing/processed/synced/failed/deleted 中文标签。 */
export function FileStatusBadge({ file }: { file: FileItem }) {
  const s = STATUS_LABELS[file.status] ?? { label: file.status, tone: "muted" as const }
  return (
    <StatusText tone={s.tone} error={file.status === "failed" ? file.error : null}>
      {s.label}
    </StatusText>
  )
}
