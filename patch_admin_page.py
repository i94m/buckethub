#!/usr/bin/env python3
"""Apply the process-section patch to web/src/pages/admin-page.tsx."""
from pathlib import Path

PAGE = Path("/opt/b2-file-manager/web/src/pages/admin-page.tsx")
src = PAGE.read_text(encoding="utf-8")

NAV_ANCHOR = '''              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "local"}'''
NAV_NEW = '''              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "process"}
                  onClick={() => setSection("process")}
                >
                  <Workflow className="size-4" /> 数据处理
                  {activeProcessJobs > 0 && (
                    <span className="text-[10px] tabular-nums text-muted-foreground">
                      {activeProcessJobs}
                    </span>
                  )}
                </button>
              </NavigationMenuItem>
              <NavigationMenuItem>
                <button
                  type="button"
                  className={navigationMenuTriggerStyle({ className: "gap-1.5 px-3" })}
                  data-active={section === "local"}'''

IMPORT_ANCHOR = 'import { ServerFilesSection } from "@/components/server-files-section"'
IMPORT_NEW = IMPORT_ANCHOR + '\nimport { ProcessSection } from "@/components/process-section"'

ICON_ANCHOR = 'import { ArrowUpDown, Cloud, FolderOpen, HardDrive, RefreshCw, Search } from "lucide-react"'
ICON_NEW = 'import { ArrowUpDown, Cloud, FolderOpen, HardDrive, RefreshCw, Search, Workflow } from "lucide-react"'

TABS_ANCHOR = '''          {/* 上传管理：传输列表（上传/下载任务全宽） */}
          <TabsContent value="transfers">
            <JobsTable />
          </TabsContent>'''
TABS_NEW = TABS_ANCHOR + '''

          {/* 数据处理：B 类流水线（转换→质检→选最长→脱敏→打包） */}
          <TabsContent value="process">
            <ProcessSection />
          </TabsContent>'''

HOOK_ANCHOR = '''  // 桶连通性：导航菜单内状态点 + hover 详情共用'''
HOOK_NEW = '''  // 数据处理任务数（导航徽标用）
  const [processJobs, setProcessJobs] = React.useState<{ id: number; status: string }[]>([])
  const activeProcessJobs = processJobs.filter((j) =>
    ["queued", "running", "cancelling"].includes(j.status)
  ).length
  React.useEffect(() => {
    let stop = false
    async function load() {
      try {
        const res = await fetch("/api/process/jobs", {
          headers: { "X-API-Key": new URLSearchParams(window.location.search).get("apikey") ?? "" },
        })
        if (res.ok && !stop) setProcessJobs(await res.json())
      } catch {
        /* ignore */
      }
    }
    load()
    const t = setInterval(load, 5000)
    return () => {
      stop = true
      clearInterval(t)
    }
  }, [])

  // 桶连通性：导航菜单内状态点 + hover 详情共用'''

for anchor, label in ((NAV_ANCHOR, "nav"), (IMPORT_ANCHOR, "import"), (ICON_ANCHOR, "icon"),
                      (TABS_ANCHOR, "tabs"), (HOOK_ANCHOR, "hook")):
    assert anchor in src, f"anchor missing: {label}"
assert "ProcessSection" not in src, "already patched"

src = src.replace(NAV_ANCHOR, NAV_NEW, 1)
src = src.replace(IMPORT_ANCHOR, IMPORT_NEW, 1)
src = src.replace(ICON_ANCHOR, ICON_NEW, 1)
src = src.replace(TABS_ANCHOR, TABS_NEW, 1)
src = src.replace(HOOK_ANCHOR, HOOK_NEW, 1)
PAGE.write_text(src, encoding="utf-8")
print("admin-page.tsx patched OK")
