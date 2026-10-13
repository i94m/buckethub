#!/usr/bin/env python3
"""脱敏规则与执行（单一规则源，供 pipeline_runner 引用；可独立执行）。

用法：python3 redact.py <in.jsonl> <out.jsonl>
规则 13 类：私钥块/AWS AK/sk-rk-pk/GitHub/Google AIza/JWT/键值密钥/邮箱/
手机号/身份证号/POSIX 绝对路径/Windows 盘符路径/内网 IP。
命中替换为 [[REDACTED:<类型>]]，残留目标为 0。
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

REDACTIONS: list[tuple[str, re.Pattern, int | None]] = [
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"), None),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b"), None),
    ("api_key_sk", re.compile(r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{16,}"), None),
    ("github_token", re.compile(r"\b(?:ghp|github_pat|xox[baprs])_[A-Za-z0-9_-]{12,}"), None),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_\-]{35}"), None),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), None),
    ("kv_secret", re.compile(r'(?i)\b(?:api[_-]?key|secret|password|passwd|cookie|authorization)\s*[:=]\s*["\']?([A-Za-z0-9_./+\-=]{8,})'), 1),
    ("email", re.compile(r"(?<![\w.+-])[\w.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), None),
    ("cn_mobile", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), None),
    ("cn_id_card", re.compile(r"(?<!\d)\d{17}[0-9Xx](?!\d)"), None),
    ("posix_abs_path", re.compile(r"(?<!\w)/(?:Users|home|var|private)/[^\s\"'<>]+"), None),
    ("win_drive_path", re.compile(r"(?i)\b[A-Z]:\\[^\s\"'<>]+"), None),
    ("private_ip", re.compile(r"(?<!\d)(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})(?!\d)"), None),
]


def redact_string(text: str):
    """返回 (脱敏后文本, 命中计数 Counter)。"""
    from collections import Counter

    counts = Counter()
    out = text
    for tag, pattern, group in REDACTIONS:
        def _sub(m, _tag=tag, _group=group):
            counts[_tag] += 1
            if _group is None:
                return f"[[REDACTED:{_tag}]]"
            return m.group(0).replace(m.group(_group), f"[[REDACTED:{_tag}]]")
        out = pattern.sub(_sub, out)
    return out, counts


def redact_walk(node, sid: str):
    """递归脱敏 dict/list 结构（原地修改），返回命中明细列表。"""
    from collections import Counter

    log = []
    counts = Counter()

    def _walk(n, path):
        if isinstance(n, list):
            for i, item in enumerate(n):
                if isinstance(item, str):
                    new, c = redact_string(item)
                    if c:
                        log.append({"session_id": sid, "path": f"{path}[{i}]", "redactions": dict(c)})
                        counts.update(c)
                    n[i] = new
                else:
                    _walk(item, f"{path}[{i}]")
        elif isinstance(n, dict):
            for k in list(n.keys()):
                v = n[k]
                if isinstance(v, str):
                    new, c = redact_string(v)
                    if c:
                        log.append({"session_id": sid, "path": f"{path}.{k}", "redactions": dict(c)})
                        counts.update(c)
                    n[k] = new
                else:
                    _walk(v, f"{path}.{k}")

    _walk(node, "record")
    return log, counts


def sensitive_hits(node) -> Counter:
    """残留检测（跳过已含 REDACTED 标记的命中），返回 Counter。"""
    counts = Counter()

    def visit(n):
        if isinstance(n, str):
            for tag, pattern, _ in REDACTIONS:
                if any("REDACTED" not in m.group(0) for m in pattern.finditer(n)):
                    counts[tag] += 1
        elif isinstance(n, list):
            for item in n:
                visit(item)
        elif isinstance(n, dict):
            for val in n.values():
                visit(val)

    visit(node)
    return counts


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    rows = hits = 0
    t0 = time.time()
    with src.open(encoding="utf-8") as fi, dst.open("w", encoding="utf-8") as fo:
        for line in fi:
            if not line.strip():
                continue
            rec = json.loads(line)
            _, counts = redact_walk(rec, rec.get("run_id", "?"))
            rows += 1
            hits += sum(counts.values())
            fo.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(json.dumps({"rows": rows, "redaction_hits": hits,
                      "elapsed_min": round((time.time() - t0) / 60, 1)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
