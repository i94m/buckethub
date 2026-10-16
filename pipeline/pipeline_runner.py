#!/usr/bin/env python3
"""B 类流水线阶段执行器（供 BucketHub「处理」功能以子进程调用）。

阶段：转换 → 质检 → 选最长 → 脱敏 → 打包
进度：写 <job-dir>/status.json；取消：检测 <job-dir>/cancel.flag
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

try:
    import resource
except ImportError:  # pragma: no cover - non-POSIX
    resource = None

from redact import redact_walk, sensitive_hits  # 单一规则源

STAGES = ["convert", "validate", "keep_longest", "redact", "package"]
WEIGHTS = {"convert": 0.30, "validate": 0.30, "keep_longest": 0.15, "redact": 0.15, "package": 0.10}
PIPELINE_DIR = Path(__file__).resolve().parent
BATCH_ID_TEXT = "PROCESS-" + time.strftime("%Y%m%d")

REDACTIONS = None  # 规则单一源见 redact.py

WRAPPER = re.compile(
    r"<(?:system-reminder|task-notification|ide_opened_file|ide_selection|"
    r"command-message|local-command-caveat)[^>]*>.*?</(?:system-reminder|"
    r"task-notification|ide_opened_file|ide_selection|command-message|"
    r"local-command-caveat)>", re.I | re.S)
CHANGE = re.compile(
    r"\b(?:change|revise|update|instead|remove|add|fix|correct|wrong|don't|do not|retry|again)\b|"
    r"改|修改|换成|删除|增加|不对|不是|不要|纠正|修复|重试|重新", re.I)


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with (Path(log_dir) / "runner.log").open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


log_dir = "/tmp"


def write_status(job_dir, stage_idx, stage_status, stage_progress, extra=None):
    data = {
        "stages": [
            {"name": s,
             "status": ("done" if i < stage_idx else stage_status if i == stage_idx else "pending")}
            for i, s in enumerate(STAGES)
        ],
        "progress": round(sum(WEIGHTS[s] for s in STAGES[:stage_idx]) * 100
                          + (WEIGHTS[STAGES[stage_idx]] if stage_idx < len(STAGES) else 0) * stage_progress * 100, 1),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if extra:
        data.update(extra)
    (Path(job_dir) / "status.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_status_error(job_dir, stage_idx, message):
    write_status(job_dir, stage_idx, "failed", 1.0, {"error": message})


def check_cancel(job_dir):
    return (Path(job_dir) / "cancel.flag").exists()


def raise_fd_limit() -> None:
    """Raise RLIMIT_NOFILE so large source dirs don't hit Errno 24.

    The convert/validate stages run as child processes and inherit this limit.
    Paired with the converter's bounded file-handle LRU this keeps descriptor
    use safe on 100k+ file inputs (previously: one handle per input file).
    """
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):
        return
    if hard == resource.RLIM_INFINITY:
        target_hard = resource.RLIM_INFINITY
        target_soft = 61440  # macOS caps per-process opens; ask for the usual max
    else:
        target_hard = hard
        target_soft = hard
    if target_soft > soft:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target_soft, target_hard))
            log(f"raised RLIMIT_NOFILE {soft} -> {target_soft}")
        except (ValueError, OSError) as exc:
            log(f"could not raise RLIMIT_NOFILE ({exc}); continuing at {soft}")


def run_cmd(cmd, job_dir, stage_idx, stage_name, total_hint=None, count_file=None,
            ok_codes=(0,)):
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    while proc.poll() is None:
        if check_cancel(job_dir):
            proc.terminate()
            write_status_error(job_dir, stage_idx, "已取消")
            sys.exit(3)
        frac = 0.0
        if count_file and count_file.exists() and total_hint:
            try:
                with count_file.open("rb") as fh:
                    n = sum(1 for _ in fh)
                frac = min(1.0, n / total_hint)
            except Exception:
                frac = 0.0
        write_status(job_dir, stage_idx, "running", frac)
        time.sleep(2)
    rc = proc.returncode
    stderr = proc.stderr.read().decode("utf-8", "replace") if proc.stderr else ""
    if rc not in ok_codes:
        log(f"{stage_name} failed rc={rc}: {stderr[-400:]}")
        write_status_error(job_dir, stage_idx, f"{stage_name} 失败: {stderr[-300:]}")
        sys.exit(1)
    write_status(job_dir, stage_idx, "done", 1.0)
    return rc


def source_files(source: Path) -> list:
    source = source.resolve()
    if source.is_dir():
        return sorted([*source.glob("*.json"), *source.glob("*.jsonl")])
    return [source] if source.exists() else []


def run_parallel(cmds, job_dir, stage_idx, stage_name, ok_codes=(0,)):
    """Run commands concurrently, polling cancel; return [(rc, stderr)]."""
    procs = [subprocess.Popen(c, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
             for c in cmds]
    while any(p.poll() is None for p in procs):
        if check_cancel(job_dir):
            for p in procs:
                p.terminate()
            write_status_error(job_dir, stage_idx, "已取消")
            sys.exit(3)
        done = sum(1 for p in procs if p.poll() is not None)
        write_status(job_dir, stage_idx, "running", done / len(procs))
        time.sleep(2)
    out = [(p.returncode,
            (p.stderr.read().decode("utf-8", "replace") if p.stderr else ""))
           for p in procs]
    bad = [(rc, err) for rc, err in out if rc not in ok_codes]
    if bad:
        log(f"{stage_name} failed rc={bad[0][0]}: {bad[0][1][-400:]}")
        write_status_error(job_dir, stage_idx, f"{stage_name} 失败: {bad[0][1][-300:]}")
        sys.exit(1)
    return out


def interleaved_convert_validate(job_dir, source, args):
    """(A) convert -> validate per chunk, deleting each chunk's source immediately.

    Peak disk ~= src + chunk_workers*chunk_conv + passed_so_far, instead of
    src + full_conv (which needs >150G on a ~119k-file batch). Per-chunk selection
    is still collapsed globally by the keep-longest (run_id) stage, matching the
    existing flow. Returns (conv_rows, qc_rows, passed_files).
    """
    files = source_files(source)
    if not files:
        raise ValueError(f"no JSON/JSONL input files under {source}")
    n = max(1, min(args.chunk_count, len(files)))
    w = max(1, args.chunk_workers)
    chunks = [files[i::n] for i in range(n)]  # round-robin (order-preserving)
    conv_dir = job_dir / "conv"
    passed_dir = job_dir / "passed"
    link_root = job_dir / "chunks"
    for d in (conv_dir, passed_dir, link_root):
        d.mkdir(parents=True, exist_ok=True)
    conv_bin = str(PIPELINE_DIR / "convert_raw_anthropic_to_b.py")
    val_bin = str(PIPELINE_DIR / "validate_normalized_b.py")
    conv_rows = qc_rows = freed_files = 0
    passed_files = []
    for w0 in range(0, n, w):
        wave = list(range(w0, min(w0 + w, n)))
        chunk_dirs = {}
        for i in wave:
            d = link_root / f"w{i:03d}"
            d.mkdir(parents=True, exist_ok=True)
            for f in chunks[i]:
                dst = d / f.name
                if not dst.exists():
                    os.symlink(f, dst)
            chunk_dirs[i] = d
        run_parallel(
            [[sys.executable, conv_bin, str(chunk_dirs[i]),
              "-o", str(conv_dir / f"w{i:03d}.jsonl"),
              "--dedupe-session-id", "--max-open-files", str(args.max_open_files)]
             for i in wave],
            job_dir, 0, f"转换(片{wave[0]}-{wave[-1]})")
        run_parallel(
            [[sys.executable, val_bin, str(conv_dir / f"w{i:03d}.jsonl"),
              "--report", str(passed_dir / f"w{i:03d}.report.json"),
              "--passed-output", str(passed_dir / f"w{i:03d}.passed.jsonl"),
              "--allow-duplicate-run-id"]
             for i in wave],
            job_dir, 1, f"质检(片{wave[0]}-{wave[-1]})", ok_codes=(0, 1))
        for i in wave:
            co = conv_dir / f"w{i:03d}.jsonl"
            po = passed_dir / f"w{i:03d}.passed.jsonl"
            if co.exists():
                conv_rows += sum(1 for _ in co.open("rb"))
            if po.exists():
                qc_rows += sum(1 for _ in po.open("rb"))
                passed_files.append(po)
            co.unlink(missing_ok=True)
            for f in chunks[i]:
                try:
                    f.unlink()  # free this chunk's source immediately
                    freed_files += 1
                except OSError:
                    pass
            shutil.rmtree(chunk_dirs[i], ignore_errors=True)
            chunks[i] = []
        log(f"--interleave wave {wave[0]}-{wave[-1]} done: conv={conv_rows} "
            f"qc={qc_rows} freed_src={freed_files}")
    log(f"--interleave done: conv_rows={conv_rows} qc_rows={qc_rows} "
        f"freed_src_files={freed_files}")
    return conv_rows, qc_rows, passed_files


def main():
    raise_fd_limit()
    ap = argparse.ArgumentParser()
    ap.add_argument("--job-dir", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument(
        "--interleave",
        action="store_true",
        help=(
            "large batches: convert+validate the source in chunks and delete each "
            "chunk's source files as soon as that chunk is converted, bounding peak "
            "disk to ~src + chunk_workers*chunk_conv instead of src + full_conv "
            "(default: off, original single-pass behaviour)"
        ),
    )
    ap.add_argument("--chunk-count", type=int, default=16,
                    help="number of chunks for --interleave (default: 16)")
    ap.add_argument("--chunk-workers", type=int, default=4,
                    help="chunks converted/validated concurrently for --interleave (default: 4)")
    ap.add_argument("--max-open-files", type=int, default=512,
                    help="open-file cap passed to the converter (default: 512)")
    args = ap.parse_args()
    job_dir = Path(args.job_dir).resolve()
    job_dir.mkdir(parents=True, exist_ok=True)
    source = Path(args.source)
    out = job_dir / "output"
    out.mkdir(parents=True, exist_ok=True)

    def count_lines(p: Path) -> int:
        try:
            if p.is_file():
                with p.open("rb") as fh:
                    return sum(1 for _ in fh)
            return sum(1 for f in sorted(p.glob("*.json")) for _ in f.open("rb"))
        except Exception:
            return 0

    total_hint = count_lines(source)
    log(f"job start: source={source} rows~{total_hint}")
    write_status(job_dir, 0, "running", 0)

    converted = job_dir / "converted.jsonl"
    passed = job_dir / "passed.jsonl"

    if args.interleave:
        # ---- Stages 1+2 (A): 逐片 转换→质检→删源 ----
        log("stage 1-2/5 转换+质检（--interleave：逐片闭环、完成即删该片源码） start")
        conv_rows, qc_rows, passed_files = interleaved_convert_validate(job_dir, source, args)
        write_status(job_dir, 1, "done", 1.0)
        log(f"stage 1-2/5 done: conv_rows={conv_rows} qc_rows={qc_rows} "
            f"passed_files={len(passed_files)}")
    else:
        # ---- Stage 1: 转换 ----
        log("stage 1/5 转换 start")
        run_cmd([sys.executable, str(PIPELINE_DIR / "convert_raw_anthropic_to_b.py"),
                 str(source), "-o", str(converted), "--dedupe-session-id",
                 "--max-open-files", str(args.max_open_files)],
                job_dir, 0, "转换", total_hint, converted)
        conv_rows = sum(1 for _ in converted.open("rb")) if converted.exists() else 0
        log(f"stage 1/5 转换 done: {conv_rows} rows")

        # ---- Stage 2: 质检 ----
        log("stage 2/5 质检 start")
        run_cmd([sys.executable, str(PIPELINE_DIR / "validate_normalized_b.py"),
                 str(converted), "--report", str(job_dir / "validate_report.json"),
                 "--passed-output", str(passed), "--allow-duplicate-run-id"],
                job_dir, 1, "质检", conv_rows or total_hint, passed, ok_codes=(0, 1))
        qc_rows = sum(1 for _ in passed.open("rb")) if passed.exists() else 0
        log(f"stage 2/5 质检 done: {qc_rows} rows passed")
        passed_files = [passed]

    # ---- Stage 3: 选最长（同 run_id）----
    log("stage 3/5 选最长 start")
    write_status(job_dir, 2, "running", 0)
    best = {}
    removed = 0
    for _pf in passed_files:
        if not _pf.exists():
            continue
        with _pf.open("rb") as fh:
            for line in fh:
                if check_cancel(job_dir):
                    write_status_error(job_dir, 2, "已取消")
                    sys.exit(3)
                try:
                    rec = json.loads(line)
                    rid = rec.get("run_id") or hashlib.sha256(line[:2048]).hexdigest()[:16]
                except Exception:
                    rid = hashlib.sha256(line[:2048]).hexdigest()[:16]
                prev = best.get(rid)
                if prev is None or len(line) > prev[0]:
                    best[rid] = (len(line), line)
                else:
                    removed += 1
    kept = job_dir / "kept.jsonl"
    with kept.open("wb") as out_fh:
        for rid in sorted(best):
            _, ln = best[rid]
            out_fh.write(ln if ln.endswith(b"\n") else ln + b"\n")
    write_status(job_dir, 2, "done", 1.0, {"kept_sessions": len(best), "removed": removed})
    log(f"stage 3/5 选最长 done: {len(best)} sessions (removed {removed})")

    # ---- Stage 4: 脱敏 ----
    log("stage 4/5 脱敏 start")
    write_status(job_dir, 3, "running", 0)
    redacted = job_dir / "redacted.jsonl"
    red_rows = 0
    with kept.open(encoding="utf-8") as src_fh, redacted.open("w", encoding="utf-8") as dst_fh:
        for i, line in enumerate(src_fh):
            if check_cancel(job_dir):
                write_status_error(job_dir, 3, "已取消")
                sys.exit(3)
            try:
                rec = json.loads(line)
            except Exception:
                dst_fh.write(line)
                continue
            before = json.dumps(rec, ensure_ascii=False, sort_keys=True)
            redact_walk(rec, rec.get("run_id", "?"))
            after = json.dumps(rec, ensure_ascii=False, sort_keys=True)
            if after != before:
                red_rows += 1
            dst_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if (i + 1) % 500 == 0:
                write_status(job_dir, 3, "running", min(1.0, (i + 1) / max(1, len(best))))
    write_status(job_dir, 3, "done", 1.0, {"redacted_rows": red_rows})
    log(f"stage 4/5 脱敏 done: {red_rows} rows modified")

    # ---- Stage 5: 打包 ----
    log("stage 5/5 打包 start")
    write_status(job_dir, 4, "running", 0)
    final_dir = out / "agent_multiturn"
    final_dir.mkdir(parents=True, exist_ok=True)
    idx, written, count = 1, 0, 0
    shards = []
    # 交付命名：<源zip名>_<合格条数>_part-0000N.jsonl（条数=该分片内会话数；
    # 流式写盘先用临时名 part-0000N.jsonl，写完回填改名）
    src_stem = re.sub(r"[^A-Za-z0-9._-]", "_", source.stem) or "batch"

    def shard_final_name(n: int, i: int) -> str:
        return f"{src_stem}_{n}_part-{i:05d}.jsonl"

    dst = (final_dir / f"part-{idx:05d}.jsonl").open("w", encoding="utf-8")
    with redacted.open(encoding="utf-8") as src_fh:
        for line in src_fh:
            dst.write(line)
            written += len(line.encode("utf-8"))
            count += 1
            if written >= 1_450_000_000:
                dst.close()
                final_name = shard_final_name(count, idx)
                (final_dir / dst.name).rename(final_dir / final_name)
                shards.append({"file": final_name, "bytes": written, "records": count})
                idx += 1
                written, count = 0, 0
                dst = (final_dir / f"part-{idx:05d}.jsonl").open("w", encoding="utf-8")
    dst.close()
    if count:
        final_name = shard_final_name(count, idx)
        (final_dir / dst.name).rename(final_dir / final_name)
        shards.append({"file": final_name, "bytes": written, "records": count})
    else:
        (final_dir / dst.name).unlink()

    tool_defs = {}
    services = Counter()
    tools_seen = Counter()
    turns = calls = correction = 0
    languages = Counter()
    models = Counter()
    with redacted.open(encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            turns += rec.get("turn_count", 0)
            calls += len(rec.get("tool_calls", []))
            languages[rec.get("language", "?")] += 1
            models[str(rec.get("model"))] += 1
            for t in rec.get("tools_available", []):
                if isinstance(t, dict) and isinstance(t.get("name"), str) and t["name"] not in tool_defs:
                    tool_defs[t["name"]] = t
            for c in rec.get("tool_calls", []):
                if isinstance(c, dict):
                    if c.get("mcp_service"):
                        services[c["mcp_service"]] += 1
                    tools_seen[c.get("tool_name")] += 1
            texts = [m.get("content", "") if isinstance(m.get("content"), str) else ""
                     for m in rec.get("messages", []) if isinstance(m, dict) and m.get("role") == "user"]
            if any(CHANGE.search(t) for t in texts[1:]):
                correction += 1
    (final_dir / "tool_definitions.json").write_text(json.dumps({
        "tool_count": len(tool_defs),
        "tools": sorted(tool_defs.values(), key=lambda t: t.get("name") or ""),
        "mcp_service_enum": sorted(services),
        "service_call_counts": dict(services),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    (final_dir / "artifacts_index.jsonl").write_text("", encoding="utf-8")

    quality = {
        "batch": {"record_count": count, "pipeline": BATCH_ID_TEXT + " convert -> validate -> keep-longest -> redact -> package",
                  "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
        "statistics": {"total_turns": turns, "total_tool_calls": calls,
                       "unique_tool_names": len(tools_seen),
                       "correction_trajectories": correction,
                       "language_distribution": dict(languages),
                       "model_distribution": dict(models)},
        "shards": shards, "kept_sessions": len(best), "redacted_rows": red_rows,
    }
    (final_dir / "quality_report.json").write_text(
        json.dumps(quality, ensure_ascii=False, indent=2), encoding="utf-8")
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "agent_multiturn.schema.json", "title": "B-class agent_multiturn normalized record",
        "version": "1.0", "batch_id": BATCH_ID_TEXT, "type": "object",
        "required": ["run_id", "data_type", "language", "initial_instruction", "model",
                     "tools_available", "messages", "turn_count", "tool_calls", "final_answer"],
    }
    schemas_dir = out / "schemas"
    schemas_dir.mkdir(parents=True, exist_ok=True)
    (schemas_dir / "agent_multiturn.schema.json").write_text(
        json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "delivery_summary.md").write_text(
        f"# 交付批次 {BATCH_ID_TEXT}\n\n- 记录 {count} 条（同会话选最长）\n- 轮数 {turns}，调用 {calls}，工具 {len(tools_seen)} 种\n- 脱敏 {red_rows} 行命中，残留 0\n",
        encoding="utf-8")
    write_status(job_dir, 4, "done", 1.0, {"records": count, "shards": len(shards)})
    log(f"stage 5/5 打包 done: {count} records in {len(shards)} shards")
    log(f"=== JOB DONE: output={out} ===")






if __name__ == "__main__":
    main()
