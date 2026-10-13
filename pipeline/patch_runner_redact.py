#!/usr/bin/env python3
"""Switch pipeline_runner.py to import redaction from redact.py (single source)."""
from pathlib import Path

RUNNER = Path("/opt/b2-file-manager/pipeline/pipeline_runner.py")
src = RUNNER.read_text(encoding="utf-8")

# 1. 删除内嵌 REDACTIONS 列表（从 "REDACTIONS = [" 到 "]\n" 的第一处完整块）
start = src.find("REDACTIONS = [")
assert start != -1, "REDACTIONS not found"
end = src.find("\n]\n", start)
assert end != -1
src = src[:start] + "REDACTIONS = None  # 规则单一源见 redact.py\n" + src[end + 3:]

# 2. 删除内嵌 redact_string / redact_walk 定义（pipeline_runner 版本）
import re
m1 = re.search(r"def redact_string\(text\):.*?\n    return out\n", src, re.S)
assert m1, "redact_string not found"
src = src[:m1.start()] + src[m1.end():]
m2 = re.search(r"def redact_walk\(node, log_ctx\):.*?\n                redact_walk\(val, log_ctx\)\n", src, re.S)
assert m2, "redact_walk not found"
src = src[:m2.start()] + src[m2.end():]

# 3. 加 import（置于 PIPELINE_DIR 之前）
anchor = 'STAGES = ["convert", "validate", "keep_longest", "redact", "package"]'
assert anchor in src
src = src.replace(anchor, "from redact import redact_walk, sensitive_hits  # 单一规则源\n\n" + anchor, 1)

# 4. Stage 4 中残留统计改用 sensitive_hits（原引用已随删除移除，补齐）
old4 = '            redact_walk(rec, "record")'
assert old4 in src
src = src.replace(old4, '            redact_walk(rec, rec.get("run_id", "?"))', 1)

RUNNER.write_text(src, encoding="utf-8")
print("pipeline_runner.py switched to redact.py import OK")
