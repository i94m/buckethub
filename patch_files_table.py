#!/usr/bin/env python3
"""Add 清洗 column to files-data-table + pass processJobs prop."""
from pathlib import Path

TABLE = Path("/opt/b2-file-manager/web/src/components/files-data-table.tsx")
PAGE = Path("/opt/b2-file-manager/web/src/pages/admin-page.tsx")

tsrc = TABLE.read_text(encoding="utf-8")

# 1. import ProcessCell + type
IMPORT_ANCHOR = 'import { CopyButton } from "@/components/ui/copy-button"'
IMPORT_NEW = IMPORT_ANCHOR + '''
import { ProcessCell, type ProcessJobLite } from "@/components/process-cell"'''
assert IMPORT_ANCHOR in tsrc
tsrc = tsrc.replace(IMPORT_ANCHOR, IMPORT_NEW, 1)

# 2. props interface
PROPS_ANCHOR = "  onFileUpdated?: (file: FileItem) => void\n}"
PROPS_NEW = """  onFileUpdated?: (file: FileItem) => void
  /** 数据处理任务列表（清洗列状态来源）。 */
  processJobs?: ProcessJobLite[]
}"""
assert PROPS_ANCHOR in tsrc
tsrc = tsrc.replace(PROPS_ANCHOR, PROPS_NEW, 1)

# 3. skeleton widths
SKEL_ANCHOR = '    "w-16",   // 本地'
SKEL_NEW = SKEL_ANCHOR + '\n    "w-16",   // 清洗'
assert SKEL_ANCHOR in tsrc
tsrc = tsrc.replace(SKEL_ANCHOR, SKEL_NEW, 1)

# 4. destructure prop
DESTRUCT_OLD = """  onDeleted,
  onFileUpdated,
}: FilesDataTableProps"""
DESTRUCT_NEW = """  onDeleted,
  onFileUpdated,
  processJobs = [],
}: FilesDataTableProps & { processJobs?: ProcessJobLite[] }"""
assert DESTRUCT_OLD in tsrc, "destructure anchor missing"
tsrc = tsrc.replace(DESTRUCT_OLD, DESTRUCT_NEW, 1)

# 5. column after 本地列定义块
COL_ANCHOR = '''        {
          id: "local",
          header: () => <div className="min-w-[4rem] text-center">本地</div>,
          cell: ({ row }) => <div className="flex justify-center"><LocalCell file={row.original} onUpdated={onDeleted} onFileUpdated={onFileUpdated} /></div>,
        },'''
COL_NEW = COL_ANCHOR + '''
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
        },'''
assert COL_ANCHOR in tsrc, "local column anchor missing"
tsrc = tsrc.replace(COL_ANCHOR, COL_NEW, 1)
TABLE.write_text(tsrc, encoding="utf-8")
print("files-data-table.tsx patched OK")

# ---- admin-page.tsx: widen type + pass prop ----
psrc = PAGE.read_text(encoding="utf-8")

TYPE_ANCHOR = "  const [processJobs, setProcessJobs] = React.useState<{ id: number; status: string }[]>([])"
TYPE_NEW = "  const [processJobs, setProcessJobs] = React.useState<ProcessJobLite[]>([])"
assert TYPE_ANCHOR in psrc, "processJobs state anchor missing"
psrc = psrc.replace(TYPE_ANCHOR, TYPE_NEW, 1)

PASS_ANCHOR = """            <FilesDataTable
              data={files}
              scripts={scripts}
              buckets={buckets}"""
PASS_NEW = """            <FilesDataTable
              data={files}
              scripts={scripts}
              buckets={buckets}
              processJobs={processJobs}"""
assert PASS_ANCHOR in psrc, "FilesDataTable anchor missing"
psrc = psrc.replace(PASS_ANCHOR, PASS_NEW, 1)

IMP_ANCHOR = 'import { ProcessSection } from "@/components/process-section"'
IMP_NEW = IMP_ANCHOR + '\nimport { type ProcessJobLite } from "@/components/process-cell"'
assert IMP_ANCHOR in psrc, "process-section import anchor missing"
psrc = psrc.replace(IMP_ANCHOR, IMP_NEW, 1)
PAGE.write_text(psrc, encoding="utf-8")
print("admin-page.tsx patched OK")
