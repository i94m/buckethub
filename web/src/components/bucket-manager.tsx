import * as React from "react"
import { CircleCheck, CircleSlash, Database, GripVertical, Loader2, MoreVertical, Pencil, Plus, Trash2, Zap } from "lucide-react"
import { toast } from "sonner"

import {
  createBucket,
  deleteBucket,
  reorderBuckets,
  testBucket,
  updateBucket,
  type Bucket,
} from "@/lib/api"
import type { BucketHealthEntry } from "@/lib/types"
import { useBuckets } from "@/lib/use-buckets"
import { useConfirm } from "@/lib/use-confirm"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
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

const ADDRESSING_OPTIONS = [
  { value: "auto", label: "auto（自动）" },
  { value: "virtual", label: "virtual（虚拟主机）" },
  { value: "path", label: "path（路径）" },
]

const SIGNATURE_OPTIONS = [
  { value: "s3v4", label: "s3v4（AWS V4，默认）" },
  { value: "s3", label: "s3（V2，华为云 OBS）" },
]

/** 连通性一栏的展示。 */
function TestState({ entry, testing }: { entry: BucketHealthEntry | undefined; testing: boolean }) {
  if (testing) return <span className="text-xs text-muted-foreground">测试中…</span>
  if (!entry) return <span className="text-xs text-muted-foreground">未测试</span>
  if (entry.ok)
    return (
      <span className="text-xs text-emerald-600">
        ✓ {entry.latency_ms != null ? `${entry.latency_ms}ms` : "可访问"}
      </span>
    )
  return (
    <span className="cursor-help text-xs text-destructive" title={entry.error ?? undefined}>
      ✗ 失败
    </span>
  )
}

/** 桶管理入口：Dialog 内表格（列表/编辑切换）+ 拖动排序 + 连通性测试 + 删除。 */
export function BucketManager() {
  const { buckets, refresh } = useBuckets()
  const [open, setOpen] = React.useState(false)
  /** null = 列表视图；{id: null} = 新增；{id: n} = 编辑。 */
  const [formTarget, setFormTarget] = React.useState<{ id: number | null } | null>(null)
  const [testingId, setTestingId] = React.useState<number | null>(null)
  const [testResults, setTestResults] = React.useState<Record<number, BucketHealthEntry>>({})
  const [confirm, confirmDialog] = useConfirm()

  // ── 拖动排序（HTML5 原生 DnD，拖完一次性持久化 sort_order）──
  const [order, setOrder] = React.useState<Bucket[] | null>(null)
  const dragId = React.useRef<number | null>(null)
  const [dragOverId, setDragOverId] = React.useState<number | null>(null)
  /** 展示顺序：拖动中的本地顺序优先，否则用服务端顺序。 */
  const list = order ?? buckets

  React.useEffect(() => {
    // 弹窗打开/桶列表变更时，丢弃本地拖动顺序，回到服务端顺序
    setOrder(null)
  }, [open, buckets])

  const persistOrder = async (next: Bucket[]) => {
    setOrder(next)
    try {
      await reorderBuckets(next.map((b) => b.id))
      toast.success("桶顺序已保存")
      await refresh()
      setOrder(null)
    } catch (e) {
      toast.error("排序保存失败", { description: (e as Error).message })
      setOrder(null)
    }
  }

  const handleDrop = (targetId: number) => {
    const fromId = dragId.current
    dragId.current = null
    setDragOverId(null)
    if (fromId == null || fromId === targetId) return
    const current = [...list]
    const from = current.findIndex((b) => b.id === fromId)
    const to = current.findIndex((b) => b.id === targetId)
    if (from < 0 || to < 0) return
    const [moved] = current.splice(from, 1)
    current.splice(to, 0, moved)
    persistOrder(current)
  }

  const nextSortOrder = buckets.reduce((m, b) => Math.max(m, b.sort_order), -1) + 1

  const handleTest = async (b: Bucket) => {
    setTestingId(b.id)
    try {
      const entry = await testBucket(b.id)
      setTestResults((prev) => ({ ...prev, [b.id]: entry }))
      if (entry.ok) {
        toast.success(`「${b.name}」可访问`, {
          description: entry.latency_ms != null ? `${entry.latency_ms} ms` : undefined,
        })
      } else {
        toast.error(`「${b.name}」不可访问`, { description: entry.error ?? undefined })
      }
    } catch (e) {
      toast.error("测试失败", { description: (e as Error).message })
    } finally {
      setTestingId(null)
    }
  }

  const handleDelete = async (b: Bucket) => {
    if (
      !(await confirm({
        title: "删除桶",
        description: `删除桶「${b.name}」？不可撤销，相关任务将标记失败。`,
        confirmText: "删除",
        destructive: true,
      }))
    )
      return
    try {
      const r = await deleteBucket(b.id)
      toast.success(r.message)
      refresh()
    } catch (e) {
      toast.error("删除失败", { description: (e as Error).message })
    }
  }

  /** 启用/停用切换：停用的桶不参与业务（导航不显示、不可上传/下载）。 */
  const [togglingId, setTogglingId] = React.useState<number | null>(null)
  const handleToggleEnabled = async (b: Bucket) => {
    // 默认桶不允许停用（大量缺省请求依赖它）
    if (b.enabled && b.is_default) {
      toast.error("默认桶不能停用，请先把其它桶设为默认")
      return
    }
    setTogglingId(b.id)
    try {
      const r = await updateBucket(b.id, { enabled: !b.enabled })
      toast.success(r.message)
      refresh()
    } catch (e) {
      toast.error("操作失败", { description: (e as Error).message })
    } finally {
      setTogglingId(null)
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(v) => {
        setOpen(v)
        if (!v) setFormTarget(null)
      }}
    >
      <DialogTrigger asChild>
        <Button variant="outline">
          <Database className="size-4" /> 桶管理
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>桶管理</DialogTitle>
          <DialogDescription>
            桶配置存于数据库，新增/修改后立即生效；拖动左侧把手可排序（页面各处桶列/列表按此顺序展示）；旧别名 self / bucket2 / beijing 仍可供机器人使用。
          </DialogDescription>
        </DialogHeader>
        {formTarget ? (
          <BucketForm
            initial={
              formTarget.id === null ? null : buckets.find((b) => b.id === formTarget.id) ?? null
            }
            defaultSortOrder={nextSortOrder}
            onClose={() => setFormTarget(null)}
            onSaved={() => {
              setFormTarget(null)
              refresh()
            }}
          />
        ) : (
          <div className="space-y-3">
            <div className="max-h-[55vh] overflow-y-auto rounded-lg border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-10"></TableHead>
                    <TableHead>名称</TableHead>
                    <TableHead>桶名</TableHead>
                    <TableHead className="w-16 text-center">默认</TableHead>
                    <TableHead className="w-16 text-center">启用</TableHead>
                    <TableHead className="w-24">连通性</TableHead>
                    <TableHead className="w-12 text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {list.map((b) => (
                    <TableRow
                      key={b.id}
                      onDragOver={(e) => {
                        e.preventDefault()
                        setDragOverId(b.id)
                      }}
                      onDrop={() => handleDrop(b.id)}
                      className={dragOverId === b.id ? "bg-accent" : undefined}
                    >
                      <TableCell className="w-10">
                        <span
                          draggable
                          onDragStart={() => (dragId.current = b.id)}
                          onDragEnd={() => {
                            dragId.current = null
                            setDragOverId(null)
                          }}
                          title="拖动排序"
                          className="inline-flex cursor-grab items-center text-muted-foreground hover:text-foreground active:cursor-grabbing"
                        >
                          <GripVertical className="size-4" />
                        </span>
                      </TableCell>
                      <TableCell className="font-medium">{b.name}</TableCell>
                      <TableCell className="font-mono text-xs">{b.bucket_name}</TableCell>
                      <TableCell className="text-center">{b.is_default ? "★" : ""}</TableCell>
                      <TableCell className="text-center">
                        {b.enabled ? (
                          "✓"
                        ) : (
                          <span className="text-xs text-muted-foreground">停用</span>
                        )}
                      </TableCell>
                      <TableCell>
                        <TestState entry={testResults[b.id]} testing={testingId === b.id} />
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end">
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
                              <DropdownMenuItem onClick={() => setFormTarget({ id: b.id })}>
                                <Pencil className="size-3.5" /> 编辑
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                onClick={() => handleToggleEnabled(b)}
                                disabled={togglingId !== null || (b.enabled && b.is_default)}
                              >
                                {togglingId === b.id ? (
                                  <Loader2 className="size-3.5 animate-spin" />
                                ) : b.enabled ? (
                                  <CircleSlash className="size-3.5" />
                                ) : (
                                  <CircleCheck className="size-3.5" />
                                )}
                                {b.enabled
                                  ? b.is_default
                                    ? "停用（默认桶不可停用）"
                                    : "停用"
                                  : "启用"}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                onClick={() => handleTest(b)}
                                disabled={testingId !== null}
                              >
                                {testingId === b.id ? (
                                  <Loader2 className="size-3.5 animate-spin" />
                                ) : (
                                  <Zap className="size-3.5" />
                                )}
                                测试连通性
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                variant="destructive"
                                onClick={() => handleDelete(b)}
                                disabled={b.is_default}
                              >
                                <Trash2 className="size-3.5" />
                                {b.is_default ? "删除（默认桶不可删）" : "删除"}
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            <Button size="sm" onClick={() => setFormTarget({ id: null })}>
              <Plus className="size-3.5" /> 新增桶
            </Button>
          </div>
        )}
      </DialogContent>
      {confirmDialog}
    </Dialog>
  )
}

/** 新增/编辑桶表单。 */
function BucketForm({
  initial,
  defaultSortOrder,
  onClose,
  onSaved,
}: {
  initial: Bucket | null
  defaultSortOrder: number
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = !!initial
  const [form, setForm] = React.useState({
    name: initial?.name ?? "",
    bucket_name: initial?.bucket_name ?? "",
    application_key_id: initial?.application_key_id ?? "",
    application_key: "",
    endpoint: initial?.endpoint ?? "",
    region: initial?.region ?? "",
    addressing_style: initial?.addressing_style ?? "auto",
    signature_version: initial?.signature_version ?? "s3v4",
    sort_order: String(initial?.sort_order ?? defaultSortOrder),
    is_default: initial?.is_default ?? false,
    enabled: initial?.enabled ?? true,
  })
  const [busy, setBusy] = React.useState(false)
  /** 提交后（attempted）才展示校验结果，避免打开弹窗即满屏红色。 */
  const [attempted, setAttempted] = React.useState(false)

  const set = (key: keyof typeof form, value: string | boolean) =>
    setForm((f) => ({ ...f, [key]: value }))

  const nameErrorRaw = form.name.trim() ? null : "名称必填"
  const bucketNameErrorRaw = form.bucket_name.trim() ? null : "桶名必填"
  const keyIdErrorRaw = form.application_key_id.trim() ? null : "keyID 必填"
  const keyErrorRaw =
    !isEdit && !form.application_key.trim() ? "新增桶必须填写 applicationKey" : null
  const hasError = !!(nameErrorRaw || bucketNameErrorRaw || keyIdErrorRaw || keyErrorRaw)
  const nameError = attempted ? nameErrorRaw : null
  const bucketNameError = attempted ? bucketNameErrorRaw : null
  const keyIdError = attempted ? keyIdErrorRaw : null
  const keyError = attempted ? keyErrorRaw : null

  const inputCls = (err: string | null) =>
    cn(err && "border-destructive focus-visible:ring-destructive")

  const submit = async () => {
    if (hasError) {
      setAttempted(true)
      return
    }
    setBusy(true)
    try {
      if (isEdit) {
        const r = await updateBucket(initial.id, {
          name: form.name.trim(),
          bucket_name: form.bucket_name.trim(),
          application_key_id: form.application_key_id.trim(),
          ...(form.application_key.trim()
            ? { application_key: form.application_key.trim() }
            : {}),
          endpoint: form.endpoint.trim(),
          region: form.region.trim(),
          addressing_style: form.addressing_style,
          signature_version: form.signature_version,
          sort_order: Number(form.sort_order) || 0,
          is_default: form.is_default,
          enabled: form.enabled,
        })
        toast.success(r.message)
      } else {
        const r = await createBucket({
          name: form.name.trim(),
          bucket_name: form.bucket_name.trim(),
          application_key_id: form.application_key_id.trim(),
          application_key: form.application_key.trim(),
          endpoint: form.endpoint.trim() || undefined,
          region: form.region.trim() || undefined,
          addressing_style: form.addressing_style,
          signature_version: form.signature_version,
          sort_order: Number(form.sort_order) || 0,
          is_default: form.is_default,
        })
        toast.success(r.message)
      }
      onSaved()
    } catch (e) {
      toast.error("保存失败", { description: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="max-h-[55vh] space-y-4 overflow-y-auto py-1">
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>显示名称</Label>
            <Input
              value={form.name}
              onChange={(e) => set("name", e.target.value)}
              placeholder="如 北京桶"
              className={inputCls(nameError)}
              autoFocus
            />
            {nameError && <p className="text-xs text-destructive">{nameError}</p>}
          </div>
          <div className="space-y-2">
            <Label>桶名（bucket_name）</Label>
            <Input
              value={form.bucket_name}
              onChange={(e) => set("bucket_name", e.target.value)}
              placeholder="my-b2-bucket"
              className={cn("font-mono text-sm", inputCls(bucketNameError))}
            />
            {bucketNameError && (
              <p className="text-xs text-destructive">{bucketNameError}</p>
            )}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>keyID</Label>
            <Input
              value={form.application_key_id}
              onChange={(e) => set("application_key_id", e.target.value)}
              className={cn("font-mono text-sm", inputCls(keyIdError))}
            />
            {keyIdError && <p className="text-xs text-destructive">{keyIdError}</p>}
          </div>
          <div className="space-y-2">
            <Label>applicationKey{isEdit ? "（留空 = 不变）" : ""}</Label>
            <Input
              type="password"
              value={form.application_key}
              onChange={(e) => set("application_key", e.target.value)}
              placeholder={isEdit ? "••••（保留旧值）" : ""}
              className={cn("font-mono text-sm", inputCls(keyError))}
            />
            {keyError && <p className="text-xs text-destructive">{keyError}</p>}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>Endpoint（可选，默认自动解析）</Label>
            <Input
              value={form.endpoint}
              onChange={(e) => set("endpoint", e.target.value)}
              placeholder="https://s3.<region>.amazonaws.com"
              className="font-mono text-sm"
            />
          </div>
          <div className="space-y-2">
            <Label>Region（可选）</Label>
            <Input
              value={form.region}
              onChange={(e) => set("region", e.target.value)}
              placeholder="us-west-004"
              className="font-mono text-sm"
            />
          </div>
        </div>
        <div className="grid grid-cols-3 gap-4">
          <div className="space-y-2">
            <Label>寻址风格</Label>
            <Select
              value={form.addressing_style}
              onValueChange={(v) => set("addressing_style", v)}
            >
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {ADDRESSING_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>签名版本</Label>
            <Select
              value={form.signature_version}
              onValueChange={(v) => set("signature_version", v)}
            >
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {SIGNATURE_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>排序值</Label>
            <Input
              type="number"
              value={form.sort_order}
              onChange={(e) => set("sort_order", e.target.value)}
            />
          </div>
        </div>
        <div className="flex items-center gap-6">
          <label className="flex items-center gap-2 text-sm font-medium">
            <Checkbox
              checked={form.is_default}
              onCheckedChange={(v) => set("is_default", v === true)}
            />
            默认桶（无桶参数请求的目标）
          </label>
          <label className="flex items-center gap-2 text-sm font-medium">
            <Checkbox
              checked={form.enabled}
              onCheckedChange={(v) => set("enabled", v === true)}
            />
            启用
          </label>
        </div>
      </div>
      <div className="flex justify-end gap-2">
        <Button variant="outline" onClick={onClose} disabled={busy}>
          取消
        </Button>
        <Button onClick={submit} disabled={busy}>
          {busy ? "保存中…" : "保存"}
        </Button>
      </div>
    </div>
  )
}
