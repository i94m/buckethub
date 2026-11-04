#!/usr/bin/env python3
"""交付打包 + 上传（方案第 7 步，A1 口径）。

只读 ``pipeline/corpus_pipeline.py`` 的交付目录（``<out-dir>``），**不改 pipeline/、accept/
任何代码**；把分片压成 ``<源文件名>.jsonl.zst``，生成 ``delivery-manifest.json`` 与
``README.md``，然后上传到桶内一个**全新**日期目录（``2026-11-04/``，被占用则
``2026-11-04-delivery-1/`` 递增）。

默认 **dry-run**：不压缩、不写盘、不连网，只打印计划。加 ``--execute`` 才真正干活。
凭证只从环境变量取（``B2_KEY_ID`` / ``B2_APP_KEY``，兼容 ``AWS_ACCESS_KEY_ID`` /
``AWS_SECRET_ACCESS_KEY``），不进命令行、不落盘、不回显。

退出码：0 成功 / 2 前置校验失败 / 3 远端同名对象冲突（整批停）/ 4 上传后校验失败。

细节与命令见 ``delivery/README.md``。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = "buckethub-delivery/1"
PACK_STATE = "pack-state.json"
UPLOAD_JOURNAL = "upload.journal.jsonl"
UPLOAD_REPORT = "upload-report.json"
DELIVERY_MANIFEST = "delivery-manifest.json"
SNAPSHOT_DIR = "manifests"
DELIVERY_README = "README.md"
PIPELINE_MANIFEST = "manifest.json"
PIPELINE_LEDGER = "ledger.json"
NON_SHARD_JSONL = {"index.jsonl", "keep.jsonl"}
CHUNK = 4 << 20

DEFAULT_BUCKET = "tvk-data-1012-a"
DEFAULT_ENDPOINT = "https://s3.us-east-005.backblazeb2.com"
DEFAULT_REGION = "us-east-005"


# ---------------------------------------------------------------- 错误

class DeliveryError(Exception):
    """前置校验 / 配置错误（退出码 2）。"""


class ConflictError(DeliveryError):
    """远端已有同名对象但内容不同：整批停，绝不覆盖（退出码 3）。"""


class VerifyError(DeliveryError):
    """上传后校验失败（退出码 4）。"""


class SimulatedCrash(RuntimeError):
    """自测用：模拟进程在 put 中途被杀。"""


# ---------------------------------------------------------------- 小工具

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def human_bytes(n: int) -> str:
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024 or unit == "TB":
            return f"{v:.1f} {unit}" if unit != "B" else f"{int(v)} B"
        v /= 1024
    return f"{n} B"


def sha256_file(path: Path, chunk: int = CHUNK) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def count_lines(path: Path) -> tuple:
    """返回 (行数, 是否以换行结尾)。半行尾是交付目录损坏的信号。"""
    lines = 0
    last = b""
    with open(path, "rb") as fh:
        while True:
            block = fh.read(CHUNK)
            if not block:
                break
            lines += block.count(b"\n")
            last = block[-1:]
    return lines, last == b"\n"


def read_json(path: Path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def lock_is_fresh(path: Path, ttl: int) -> tuple:
    """（与 pipeline 同语义）心跳新鲜 -> (True, 描述)。文件不存在/过期 -> (False, "")。"""
    if not path.exists():
        return False, ""
    try:
        info = read_json(path)
    except (ValueError, OSError):
        return False, ""
    beat = float(info.get("heartbeat") or 0)
    age = time.time() - beat
    if 0 <= age <= ttl:
        return True, f"pid={info.get('pid')} host={info.get('host')} 心跳 {age:.0f}s 前"
    return False, ""


# ---------------------------------------------------------------- 交付目录

@dataclass
class Part:
    source_key: str
    part: int
    shard_name: str
    path: Path
    windows: int
    tokens: int
    machine: str = ""

    @property
    def name(self) -> str:
        """压缩后对象名：分片文件名加 .zst。

        合并目录里的分片可能住在子目录里（``machine-a/gpt-....jsonl``）：对象名只取
        文件名，避免桶里多出一层目录；同名冲突在 inspect 阶段就报错。
        """
        return Path(self.shard_name).name + ".zst"


@dataclass
class SourceFile:
    source_key: str
    status: str
    mode: str
    records: int
    tokens: int
    parts: list = field(default_factory=list)

    @property
    def machines(self) -> list:
        return sorted({p.machine for p in self.parts if p.machine})


@dataclass
class Delivery:
    root: Path
    manifest: dict
    sources: list
    parts: list

    @property
    def windows(self) -> int:
        return sum(p.windows for p in self.parts)

    @property
    def tokens(self) -> int:
        return sum(p.tokens for p in self.parts)

    @property
    def records(self) -> int:
        return sum(s.records for s in self.sources)

    @property
    def machines(self) -> list:
        return sorted({p.machine for p in self.parts if p.machine})

    @property
    def merged_from(self) -> list:
        v = self.manifest.get("merged_from") or self.manifest.get("merge_sources") or []
        return [str(x) for x in v] if isinstance(v, list) else [str(v)]


TERMINAL_STOP_REASONS = ("corpus_exhausted", "token_target_reached", "source_exhausted", "merged")


def _machine_of(row: dict) -> str:
    """合并目录里的来源机：machine / host / worker 任一字段（都没有就留空）。"""
    for k in ("machine", "host", "worker", "source_machine"):
        v = row.get(k)
        if v:
            return str(v)
    return ""


def _check_merge_shape(parts: list, delivered: dict, manifest: dict) -> None:
    """合并目录的形状体检：重名、重号、断号、totals 与逐片对不上。

    单机产物天然满足这些；出错基本只有一种原因——合并时漏了/重了某一片，
    或者 totals 是从某一台机器抄来的而不是重算的。
    """
    names = {}
    for p in parts:
        names.setdefault(p.name, []).append(p.shard_name)
    clash = {k: v for k, v in names.items() if len(v) > 1}
    if clash:
        raise DeliveryError(
            "合并后对象名冲突（不同子目录里同名分片，桶里会互相覆盖）："
            + "；".join(f"{k} <- {v}" for k, v in clash.items()))

    slots = {}
    for p in parts:
        slots.setdefault((p.source_key, p.part), []).append(p.shard_name)
    dup = {k: v for k, v in slots.items() if len(v) > 1}
    if dup:
        raise DeliveryError(
            "同一源文件的同一 part 出现多片（合并时重复计入）："
            + "；".join(f"{k[0]} part-{k[1]:04d} <- {v}" for k, v in dup.items()))

    for key in sorted({p.source_key for p in parts}):
        got = sorted(p.part for p in parts if p.source_key == key)
        if got != list(range(1, len(got) + 1)):
            raise DeliveryError(
                f"{key} 的 part 编号不是 1..{len(got)} 连续：{got}（合并时漏片或重编号）")

    for key, row in delivered.items():
        mine = [p for p in parts if p.source_key == key]
        if int(row.get("parts") or 0) and int(row["parts"]) != len(mine):
            raise DeliveryError(
                f"{key} 在 manifest.delivered_files 里写 {row['parts']} 片，"
                f"实际 {len(mine)} 片")
        if row.get("status") == "delivered" and not mine:
            raise DeliveryError(f"{key} 状态是 delivered，但合并目录里一片都没有")
    if delivered:
        for key in sorted({p.source_key for p in parts}):
            if key not in delivered:
                print(f"[warn] 分片对应的 source_key 不在 delivered_files 里：{key}"
                      f"（合并清单不完整，records/status 会留空）")

    if manifest.get("delivered_records") is not None and delivered:
        rec = sum(int(f.get("records") or 0) for f in delivered.values())
        if int(manifest["delivered_records"]) != rec:
            raise DeliveryError(
                f"manifest.delivered_records={manifest['delivered_records']} ≠ "
                f"delivered_files[].records 之和 {rec}（合并后 totals 没重算）")

    if manifest.get("delivered_samples") is not None:
        win = sum(int(x.get("samples") or 0) for x in manifest.get("shards") or [])
        if win and int(manifest["delivered_samples"]) != win:
            raise DeliveryError(
                f"manifest.delivered_samples={manifest['delivered_samples']} ≠ "
                f"shards[].samples 之和 {win}（合并后 totals 没重算）")


def inspect_delivery(root: Path, allow_lock: bool = False, lock_ttl: int = 300,
                     allow_count_mismatch: bool = False,
                     allow_closure_mismatch: bool = False,
                     allow_stop_reasons=()) -> Delivery:
    """读交付目录并与 manifest.json 逐项对齐。

    既吃单机 ``corpus_pipeline.py`` 的产物，也吃**多机合并后的交付目录**：
    多个 `source_key`、同一源文件的多个 `.part-NNNN`、以及可选的 ``machine`` 来源字段。
    合并目录最容易出的三种错——分片重名、part 编号断号/重号、totals 与逐片之和对不上——
    都在这里当场拦下，绝不带着不一致上传。
    """
    if not root.is_dir():
        raise DeliveryError(f"交付目录不存在：{root}")
    mpath = root / PIPELINE_MANIFEST
    if not mpath.exists():
        raise DeliveryError(f"交付目录缺少 {PIPELINE_MANIFEST}（先跑 pipeline 全流程）：{root}")
    manifest = read_json(mpath)

    stop = manifest.get("STOP_REASON")
    if manifest.get("SEGMENT_PAUSED") or stop == "segment_paused":
        raise DeliveryError(
            "交付目录处于分段暂停态（STOP_REASON=segment_paused）：这不是终态，"
            "先跑完最后一段再打包")
    allowed_stop = tuple(TERMINAL_STOP_REASONS) + tuple(allow_stop_reasons or ())
    if stop not in allowed_stop:
        raise DeliveryError(
            f"STOP_REASON 非终态：{stop!r}（应为 {' / '.join(TERMINAL_STOP_REASONS)}；"
            f"合并目录的自定义值用 --allow-stop-reason 显式放行）")

    if not allow_lock:
        live, desc = lock_is_fresh(root / "run.lock", lock_ttl)
        if live:
            raise DeliveryError(f"交付目录仍有活跃实例（run.lock：{desc}），拒绝打包半成品")

    if not allow_closure_mismatch:
        ledger = manifest.get("ledger") or {}
        if manifest.get("dedup_closure_ok") is False:
            raise DeliveryError("manifest.dedup_closure_ok=false：去重账不闭合，拒绝打包")
        if ledger.get("filter_closure_ok") is False:
            raise DeliveryError("manifest.ledger.filter_closure_ok=false：过滤账不闭合，拒绝打包")

    shards = manifest.get("shards") or []
    if not shards:
        raise DeliveryError("manifest.shards 为空：这个交付目录没有任何分片可打包")

    delivered = {f["source_key"]: f for f in manifest.get("delivered_files") or []}

    parts = []
    for s in shards:
        shard_name = s.get("file")
        key = s.get("source_key")
        if not shard_name or not key:
            raise DeliveryError(f"manifest.shards 条目缺 file/source_key：{s}")
        path = root / shard_name
        if not path.exists():
            raise DeliveryError(f"manifest 列出的分片不存在：{path}")
        if path.stat().st_size == 0:
            raise DeliveryError(f"分片是空文件：{path}")
        lines, ends_nl = count_lines(path)
        if not ends_nl:
            raise DeliveryError(f"分片尾部不是完整行（可能被打断）：{path}")
        expected = int(s.get("samples") or 0)
        if expected and lines != expected:
            msg = (f"分片行数与 manifest 不符：{shard_name} 实际 {lines} 行，"
                   f"manifest.shards[].samples={expected}")
            if not allow_count_mismatch:
                raise DeliveryError(msg + "（确认交付目录没被改过；可用 --allow-count-mismatch 降级为警告）")
            print(f"[warn] {msg}")
        parts.append(Part(source_key=key, part=int(s.get("part") or 1), shard_name=shard_name,
                          path=path, windows=lines, tokens=int(s.get("tokens") or 0),
                          machine=_machine_of(s)))

    _check_merge_shape(parts, delivered, manifest)

    # 分片集合必须与磁盘完全一致：多出来的 *.jsonl 说明 manifest 过期了
    listed = {p.shard_name for p in parts}
    stray = sorted(p.name for p in root.glob("*.jsonl")
                   if p.name not in listed and p.name not in NON_SHARD_JSONL)
    if stray:
        raise DeliveryError(f"交付目录里有 manifest 未列出的 *.jsonl：{stray}（manifest 可能过期）")

    sources = []
    for key in sorted({p.source_key for p in parts}):
        f = delivered.get(key, {})
        file_parts = sorted((p for p in parts if p.source_key == key), key=lambda p: p.part)
        sources.append(SourceFile(
            source_key=key,
            status=f.get("status") or "",
            mode=f.get("mode") or ("parts" if len(file_parts) > 1 else "single"),
            records=int(f.get("records") or 0),
            tokens=int(f.get("tokens") or sum(p.tokens for p in file_parts)),
            parts=file_parts,
        ))

    d = Delivery(root=root, manifest=manifest, sources=sources, parts=parts)

    # 汇总对账：分片 token 之和 == manifest.delivered_tokens
    total_tokens = int(manifest.get("delivered_tokens") or 0)
    if total_tokens and d.tokens != total_tokens:
        raise DeliveryError(
            f"分片 token 之和 {d.tokens:,} 与 manifest.delivered_tokens {total_tokens:,} 不符")
    # 逐文件 token 对账
    for s in sources:
        if s.tokens != sum(p.tokens for p in s.parts):
            raise DeliveryError(f"{s.source_key} 的 token 与分片之和不符")

    ledger_path = root / PIPELINE_LEDGER
    if ledger_path.exists():
        ledger = read_json(ledger_path)
        rows = ledger.get("files") or {}
        for key, row in rows.items():
            if int(row.get("delivered_windows") or 0) and int(row.get("delivered_windows")) != sum(
                    p.windows for p in parts if p.source_key == key):
                raise DeliveryError(
                    f"ledger 与分片行数不一致：{key} ledger={row.get('delivered_windows')} "
                    f"分片={sum(p.windows for p in parts if p.source_key == key)}")
    else:
        print(f"[warn] 交付目录没有 {PIPELINE_LEDGER}，跳过逐文件行数交叉核对")

    return d


# ---------------------------------------------------------------- 压缩

def _zstd_module():
    try:
        import zstandard  # noqa: F401
        return zstandard
    except ImportError:
        return None


def compress_stream(src: Path, dst: Path, level: int, threads: int) -> dict:
    """流式压缩 + 同趟算未压缩 sha256；返回字节数/哈希。"""
    dst.parent.mkdir(parents=True, exist_ok=True)
    h_raw = hashlib.sha256()
    raw_bytes = 0
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    zstd = _zstd_module()
    if zstd is not None:
        cctx = zstd.ZstdCompressor(level=level, threads=threads,
                                   write_checksum=True, write_content_size=True)
        with open(src, "rb") as fi, open(tmp, "wb") as fo:
            writer = cctx.stream_writer(fo, closefd=False)
            while True:
                block = fi.read(CHUNK)
                if not block:
                    break
                h_raw.update(block)
                raw_bytes += len(block)
                writer.write(block)
            writer.flush(zstd.FLUSH_FRAME)
            fo.flush()
            os.fsync(fo.fileno())
    else:
        # 没有 python 包时退回 zstd CLI（仍然流式，不经内存）
        import subprocess
        cmd = ["zstd", "-q", "-f", f"-{level}", f"-T{threads}", str(src), "-o", str(tmp)]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise DeliveryError(f"zstd 压缩失败：{proc.stderr.decode('utf-8', 'replace')[:400]}")
        with open(src, "rb") as fh:
            while True:
                block = fh.read(CHUNK)
                if not block:
                    break
                h_raw.update(block)
                raw_bytes += len(block)
    tmp.replace(dst)
    return {
        "bytes_raw": raw_bytes,
        "sha256_raw": h_raw.hexdigest(),
        "bytes_zst": dst.stat().st_size,
        "sha256_zst": sha256_file(dst),
    }


def decompress_sha256(path: Path, chunk: int = CHUNK) -> str:
    h = hashlib.sha256()
    zstd = _zstd_module()
    if zstd is not None:
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as fh:
            reader = dctx.stream_reader(fh)
            while True:
                block = reader.read(chunk)
                if not block:
                    break
                h.update(block)
        return h.hexdigest()
    import subprocess
    proc = subprocess.Popen(["zstd", "-dc", str(path)], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    while True:
        block = proc.stdout.read(chunk)
        if not block:
            break
        h.update(block)
    if proc.wait() != 0:
        raise DeliveryError(f"zstd 解压失败：{path}")
    return h.hexdigest()


# ---------------------------------------------------------------- 打包（压缩 + manifest + README）

@dataclass
class Object:
    name: str
    path: Path
    size: int
    sha256: str
    content_type: str
    kind: str
    metadata: dict = field(default_factory=dict)

    def key(self, target_prefix: str) -> str:
        return target_prefix + self.name


@dataclass
class Packed:
    delivery: Delivery
    staging: Path
    parts: list            # [{...}] 与 manifest.files[].parts 同构
    files: list
    objects: list          # [Object] 分片对象
    docs: list             # [Object] delivery-manifest.json + README.md（manifest 自己最后写）
    totals: dict
    delivery_id: str
    config: dict
    target_prefix: str
    extras: list = field(default_factory=list)
    manifest_obj: Object = None

    def all_objects(self) -> list:
        return list(self.objects) + list(self.docs) + list(self.extras)


def _load_pack_state(staging: Path) -> dict:
    p = staging / PACK_STATE
    if p.exists():
        try:
            return read_json(p)
        except ValueError:
            return {}
    return {}


def pack(delivery: Delivery, staging: Path, level: int, threads: int,
         target_prefix: str, source_url: str, force: bool = False,
         bucket: str = "", include=()) -> Packed:
    staging.mkdir(parents=True, exist_ok=True)
    state = _load_pack_state(staging)
    entries = dict(state.get("entries") or {})
    reuse_ok = (not force) and int(state.get("level", -1)) == level
    reused = 0

    if staging.resolve() == delivery.root.resolve():
        raise DeliveryError("--staging-dir 不能等于交付目录本身")

    part_rows = []
    objects = []
    for p in delivery.parts:
        dst = staging / p.name
        cached = entries.get(p.shard_name) if reuse_ok else None
        sha_raw = sha256_file(p.path)
        if (cached and cached.get("sha256_raw") == sha_raw
                and cached.get("src_size") == p.path.stat().st_size
                and dst.exists() and dst.stat().st_size == int(cached.get("bytes_zst") or -1)):
            info = {
                "bytes_raw": p.path.stat().st_size,
                "sha256_raw": sha_raw,
                "bytes_zst": dst.stat().st_size,
                "sha256_zst": sha256_file(dst),
            }
            if info["sha256_zst"] != cached.get("sha256_zst"):
                info = None
        else:
            info = None

        if info is None:
            print(f"[pack] 压缩 {p.shard_name} → {p.name} "
                  f"(level {level}, threads {threads or 'auto'})")
            info = compress_stream(p.path, dst, level, threads)
            back = decompress_sha256(dst)
            if back != info["sha256_raw"]:
                raise DeliveryError(f"压缩回读不一致：{p.name}（解压 sha256 {back} ≠ "
                                    f"原始 {info['sha256_raw']}）")
        else:
            reused += 1
            print(f"[pack] 复用已有压缩产物 {p.name}（sha256 命中）")

        entries[p.shard_name] = {
            "src_size": p.path.stat().st_size,
            "src_mtime_ns": p.path.stat().st_mtime_ns,
            "sha256_raw": info["sha256_raw"],
            "bytes_zst": info["bytes_zst"],
            "sha256_zst": info["sha256_zst"],
            "verified_roundtrip": True,
            "at": now_iso(),
        }
        part_rows.append({
            "part": p.part,
            "name": p.name,
            "shard_name": p.shard_name,
            "windows": p.windows,
            "tokens": p.tokens,
            "bytes_raw": info["bytes_raw"],
            "bytes_zst": info["bytes_zst"],
            "sha256_raw": info["sha256_raw"],
            "sha256_zst": info["sha256_zst"],
            "machine": p.machine,
        })
        objects.append(Object(
            name=p.name, path=dst, size=info["bytes_zst"], sha256=info["sha256_zst"],
            content_type="application/zstd", kind="shard",
            metadata={"sha256": info["sha256_zst"], "sha256-raw": info["sha256_raw"],
                      "source-key": p.source_key, "part": str(p.part),
                      "machine": p.machine},
        ))

    write_json(staging / PACK_STATE, {
        "schema": SCHEMA, "level": level, "threads": threads,
        "target_prefix": target_prefix, "bucket": bucket,
        "updated_at": now_iso(), "entries": entries,
    })
    if reused:
        print(f"[pack] {reused}/{len(delivery.parts)} 片复用，未重压")

    # 合并目录体检（只告警）：不同机器若把同一片交了两遍，字节会完全一样
    seen = {}
    for r in part_rows:
        seen.setdefault(r["sha256_raw"], []).append(r["shard_name"])
    for sha, names in seen.items():
        if len(names) > 1:
            print(f"[warn] 这些分片的未压缩内容完全相同（疑似跨机重复交付）：{names}")

    # 逐文件聚合
    files = []
    for s in delivery.sources:
        rows = [r for r in part_rows if r["shard_name"] in {p.shard_name for p in s.parts}]
        rows.sort(key=lambda r: r["part"])
        files.append({
            "source_key": s.source_key,
            "status": s.status or "delivered",
            "mode": s.mode,
            "records": s.records,
            "windows": sum(r["windows"] for r in rows),
            "tokens": s.tokens,
            "bytes_raw": sum(r["bytes_raw"] for r in rows),
            "bytes_zst": sum(r["bytes_zst"] for r in rows),
            "sha256_raw": rows[0]["sha256_raw"] if len(rows) == 1 else _join_hashes(rows, "sha256_raw"),
            "sha256_zst": rows[0]["sha256_zst"] if len(rows) == 1 else _join_hashes(rows, "sha256_zst"),
            "sha256_scope": "single-part" if len(rows) == 1 else "aggregate-of-parts",
            "machines": s.machines,
            "parts": rows,
        })

    totals = {
        "files": len(files),
        "parts": len(part_rows),
        "records": sum(f["records"] for f in files),
        "windows": sum(f["windows"] for f in files),
        "tokens": sum(f["tokens"] for f in files),
        "bytes_raw": sum(r["bytes_raw"] for r in part_rows),
        "bytes_zst": sum(r["bytes_zst"] for r in part_rows),
    }
    totals["compression_ratio"] = round(
        totals["bytes_raw"] / totals["bytes_zst"], 3) if totals["bytes_zst"] else None

    # delivery_id：只依赖相对对象名 + 压缩后哈希 + 内容哈希（与目标目录无关）
    ident = json.dumps(
        sorted([[o.name, o.sha256, o.size] for o in objects], key=lambda x: x[0]),
        ensure_ascii=False, separators=(",", ":"))
    delivery_id = hashlib.sha256(ident.encode("utf-8")).hexdigest()

    config = dict(delivery.manifest.get("config") or {})
    packed = Packed(
        delivery=delivery, staging=staging, parts=part_rows, files=files, objects=objects,
        docs=[], totals=totals, delivery_id=delivery_id, config=config,
        target_prefix=target_prefix,
    )

    # 标识写进每个对象的 metadata
    for o in objects:
        o.metadata.update({"delivery-id": delivery_id, "spec": SCHEMA})

    # 默认随附「证据文件」：pipeline 的 manifest.json 与 ledger.json，买家才能自己跑复算
    auto = [delivery.root / PIPELINE_MANIFEST, delivery.root / PIPELINE_LEDGER]
    include = [str(p) for p in auto if p.exists()] + [str(x) for x in (include or ())]
    seen_names = set()
    for path in include:
        path = Path(path)
        if path.name in seen_names:
            continue
        seen_names.add(path.name)
        if not path.exists():
            raise DeliveryError(f"--include 的文件不存在：{path}")
        ctype = ("application/json" if path.suffix == ".json"
                 else "text/markdown; charset=utf-8" if path.suffix in (".md", ".markdown")
                 else "application/octet-stream")
        packed.extras.append(Object(
            name=path.name, path=path, size=path.stat().st_size,
            sha256=sha256_file(path), content_type=ctype, kind="extra",
            metadata={"sha256": sha256_file(path), "delivery-id": delivery_id,
                      "spec": SCHEMA}))
    _write_docs(packed, source_url)
    write_json(staging / PACK_STATE, {
        "schema": SCHEMA, "level": level, "threads": threads,
        "target_prefix": target_prefix, "delivery_id": delivery_id, "bucket": bucket,
        "updated_at": now_iso(), "entries": entries,
    })
    return packed


def _join_hashes(rows, key: str) -> str:
    """多片时给一个可复算的聚合值：按 part 顺序把各片哈希拼起来再算 sha256。"""
    return hashlib.sha256("".join(r[key] for r in rows).encode("ascii")).hexdigest()


def _write_docs(packed: Packed, source_url: str) -> None:
    manifest = {
        "schema": SCHEMA,
        "generated_at": now_iso(),
        "delivery_id": packed.delivery_id,
        "dataset": {
            "bucket": packed.config.get("bucket") or DEFAULT_BUCKET,
            "source_prefix": packed.config.get("prefix") or "",
            "source_url": source_url,
            "target_prefix": packed.target_prefix,
        },
        "caliber": {
            "profile": "A1",
            "a1_drop_image_records": True,
            "a1_rule": ("request.messages[].content[] 中任一 part 为 input_image / image_url / "
                        "input_image_url（或带 image_url 字段）→ 整条记录不交付"),
            "min_messages": packed.config.get("min_messages"),
            "require_rs_id": True,
            "require_encrypted_content": True,
            "session_dedup": "session_id 的 ~ 之前 uuid 段，保留 messages 最长（并列取较新 timestamp）",
            "prefix_dedup": "messages 成员级去重，字典序相邻严格前缀丢弃",
            "tokenizer": packed.config.get("tokenizer") or "o200k_base",
            "token_counting": ("tiktoken encode_ordinary，逐条 message 的紧凑 JSON（sort_keys）"
                               "单独计数后求和，含 encrypted_content 密文"),
            "max_sample_tokens": packed.config.get("max_sample_tokens"),
            "shard_bytes": packed.config.get("shard_bytes"),
        },
        "totals": dict(packed.totals),
        "provenance": {
            "multi_machine": len(packed.delivery.machines) > 1,
            "machines": packed.delivery.machines,
            "merged_from": packed.delivery.merged_from,
            "shards_per_machine": {
                m: sum(1 for p in packed.delivery.parts if p.machine == m)
                for m in packed.delivery.machines
            },
        },
        "files": packed.files,
        "objects": [{
            "name": o.name, "key": o.key(packed.target_prefix), "kind": o.kind,
            "size": o.size, "sha256": o.sha256,
            "sha256_raw": o.metadata.get("sha256-raw", ""),
            "source_key": o.metadata.get("source-key", ""),
            "part": int(o.metadata.get("part", 0) or 0),
            "machine": o.metadata.get("machine", ""),
        } for o in packed.objects],
        "readme": {},
        "manifest_self": {
            "name": DELIVERY_MANIFEST,
            "note": "清单不写自己这一行的 sha256（自指写不进去）；上传后由 upload-report.json 记录",
        },
        "extra_documents": [{"name": o.name, "size": o.size, "sha256": o.sha256}
                            for o in packed.extras],
        "object_count": len(packed.objects) + 2 + len(packed.extras),
        "pipeline": {
            "stop_reason": packed.delivery.manifest.get("STOP_REASON"),
            "stop_file": packed.delivery.manifest.get("stop_file"),
            "target_tokens": packed.delivery.manifest.get("target_tokens"),
            "delivered_tokens": packed.delivery.manifest.get("delivered_tokens"),
            "delivered_records": packed.delivery.manifest.get("delivered_records"),
            "segments": packed.delivery.manifest.get("segments"),
            "source_manifest_sha256": sha256_file(packed.delivery.root / PIPELINE_MANIFEST),
            "ledger": packed.delivery.manifest.get("ledger") or {},
        },
        "verification": {
            "policy": {
                "head_content_length": True,
                "head_metadata_sha256": True,
                "readback_largest_object": True,
                "readback_documents": True,
                "object_count_must_match": True,
                "never_overwrite": True,
            },
        },
    }

    # 幂等：重跑时不要把清单/README 重新生成一遍——generated_at 一变哈希就变，
    # 续跑会被自己判成"同名不同 sha256"。内容（除生成时间）完全一致就原样复用。
    previous = None
    mpath = packed.staging / DELIVERY_MANIFEST
    if mpath.exists():
        try:
            previous = read_json(mpath)
        except ValueError:
            previous = None
    readme_path = packed.staging / DELIVERY_README
    # 除生成时间外内容一致 -> 沿用上一份清单（否则每次重跑 generated_at 一变，
    # 哈希就变，续跑会被自己判成"同名不同内容"而整批停）。
    candidate = previous if (previous is not None and
                             _payload_equal(previous, manifest, ("generated_at", "readme"))) else None
    base = candidate if candidate is not None else manifest
    rendered = _render_readme(packed, base)
    existing_text = readme_path.read_text(encoding="utf-8") if readme_path.exists() else None
    if existing_text != rendered:
        readme_path.write_text(rendered, encoding="utf-8")
    base["readme"] = {
        "name": DELIVERY_README,
        "size": readme_path.stat().st_size,
        "sha256": sha256_file(readme_path),
    }
    if candidate is not None and _payload_equal(previous, base):
        manifest = previous
        print(f"[pack] 复用已有 {DELIVERY_MANIFEST}（内容一致，不重写）")
    else:
        manifest = base
        write_json(mpath, manifest)

    readme_obj = Object(name=DELIVERY_README, path=readme_path,
                        size=readme_path.stat().st_size,
                        sha256=sha256_file(readme_path),
                        content_type="text/markdown; charset=utf-8", kind="doc",
                        metadata={})
    readme_obj.metadata = {"sha256": readme_obj.sha256,
                           "delivery-id": packed.delivery_id, "spec": SCHEMA}
    manifest_obj = Object(name=DELIVERY_MANIFEST, path=mpath,
                          size=mpath.stat().st_size, sha256=sha256_file(mpath),
                          content_type="application/json", kind="doc",
                          metadata={})
    manifest_obj.metadata = {"sha256": manifest_obj.sha256,
                             "delivery-id": packed.delivery_id, "spec": SCHEMA}
    # 上传顺序：README 先、manifest 最后（清单最后落，避免"有清单没内容"）
    packed.docs = [readme_obj, manifest_obj]
    packed.manifest_obj = manifest_obj


def _payload_equal(a: dict, b: dict, skip=("generated_at",)) -> bool:
    def scrub(obj):
        if isinstance(obj, dict):
            return {k: scrub(v) for k, v in obj.items() if k not in skip}
        if isinstance(obj, list):
            return [scrub(v) for v in obj]
        return obj
    return json.dumps(scrub(a), sort_keys=True, ensure_ascii=False) == \
        json.dumps(scrub(b), sort_keys=True, ensure_ascii=False)


def _render_readme(packed: Packed, m: dict) -> str:
    c = m["caliber"]
    t = m["totals"]
    lines = []
    add = lines.append
    add("# 语料交付说明（A1 口径）")
    add("")
    add(f"- 生成时间：`{m['generated_at']}`")
    add(f"- 交付 ID：`{m['delivery_id']}`")
    add(f"- 数据源：`s3://{m['dataset']['bucket']}/{m['dataset']['source_prefix']}`")
    add(f"- 桶内位置：`s3://{m['dataset']['bucket']}/{m['dataset']['target_prefix']}`")
    add(f"- 清单文件：`{DELIVERY_MANIFEST}`（逐文件 / 逐分片的字节数与 sha256 都在里面）")
    add("")
    add("## 一、口径（A1，2026-11-03 定稿）")
    add("")
    add("| 项 | 口径 |")
    add("| --- | --- |")
    add("| 记录过滤 | `request.messages` ≥ "
        f"`{c.get('min_messages')}` 条；`response.response_data.output[].id` 以 `rs_` 开头；"
        "`messages[].content[]` 至少一个非空 `encrypted_content` |")
    add("| 图片 | **A1：任一条 message 的 content part 是图片（`input_image` / `image_url` / "
        "`input_image_url`，或带 `image_url` 字段）→ 整条记录丢弃**，不是剥 part |")
    add("| session 去重 | `session_id` 的 `~` 之前 uuid 段分组，保留 `messages` 最长的一条 |")
    add("| 前缀去重 | messages 成员级哈希序列，字典序相邻的严格前缀丢弃 |")
    add(f"| token | tiktoken `{c.get('tokenizer')}` + `encode_ordinary`；逐条 message 的"
        "紧凑 JSON 单独计数后求和（**含密文**） |")
    add("| 切窗 | 单条记录 > "
        f"`{c.get('max_sample_tokens')}` token 切成多个窗口行，`_window` 记录 index/of |")
    add(f"| 分片 | 单源 > `{c.get('shard_bytes')}` 字节切 `<主干>.part-NNNN.jsonl`，"
        "压缩后同名加 `.zst` |")
    add("")
    machines = m.get("provenance", {}).get("machines") or []
    has_machine = any(p.get("machine") for f in m["files"] for p in f["parts"])
    add("## 二、文件清单")
    add("")
    if has_machine:
        add("| 压缩后对象 | 源对象 | 行数 | token | 未压缩字节 | 压缩字节 | "
            "sha256（压缩后） | 来源机 |")
        add("| --- | --- | ---: | ---: | ---: | ---: | --- | --- |")
    else:
        add("| 压缩后对象 | 源对象 | 行数 | token | 未压缩字节 | 压缩字节 | "
            "sha256（压缩后） |")
        add("| --- | --- | ---: | ---: | ---: | ---: | --- |")
    for f in m["files"]:
        for p in f["parts"]:
            row = (f"| `{p['name']}` | `{f['source_key']}` | {p['windows']:,} | "
                   f"{p['tokens']:,} | {human_bytes(p['bytes_raw'])} | "
                   f"{human_bytes(p['bytes_zst'])} | `{p['sha256_zst']}` |")
            if has_machine:
                row += f" {p.get('machine') or '—'} |"
            add(row)
    if has_machine:
        prov = m.get("provenance", {})
        add("")
        add(f"来源：**{len(machines)}** 台机器合并交付"
            + (f"（`{'`、`'.join(machines)}`）" if machines else "")
            + "，逐机分片数 "
            + ("、".join(f"{k} {v} 片" for k, v in (prov.get("shards_per_machine") or {}).items())
               or "未标注")
            + "。"
            "每片来自哪台机同时写在 `x-amz-meta-machine` 与清单 `objects[].machine` 里，"
            "逐文件汇总见 `files[].machines`。")
    add("")
    if any(f.get("sha256_scope") == "aggregate-of-parts" for f in m["files"]):
        add("> 单源多片时，`delivery-manifest.json` 里该文件行的 `sha256_raw` / `sha256_zst` "
            "是各分片哈希按 part 顺序拼接后的 sha256（`sha256_scope=aggregate-of-parts`）；"
            "逐片真实哈希看该文件的 `parts[]`。")
        add("")
    add(f"合计：**{t['files']}** 个源文件 / **{t['parts']}** 个分片 / "
        f"**{t['windows']:,}** 行 / **{t['tokens']:,}** token；"
        f"未压缩 {human_bytes(t['bytes_raw'])} → 压缩 {human_bytes(t['bytes_zst'])}"
        f"（{t['compression_ratio']}×）。")
    add("")
    extras = m.get("extra_documents") or []
    if extras:
        add("随附文件：")
        add("")
        for e in extras:
            add(f"- `{e['name']}`（{e['size']} 字节，sha256 `{e['sha256']}`）")
        add("")
    add("## 三、复算命令")
    add("")
    add("```bash")
    add("# 1) 校验压缩包哈希（对照 delivery-manifest.json 的 objects[].sha256）")
    add("sha256sum *.jsonl.zst")
    add("")
    add("# 2) 解压（zstd -d 默认保留 .zst）")
    add("zstd -d gpt-6-astra-0009-20261001.jsonl.zst")
    add("```")
    add("")
    add("token 复算（与 pipeline 同一口径，逐条 message 紧凑 JSON + encode_ordinary）：")
    add("")
    add("```python")
    add("import json, sys, tiktoken")
    add(f"enc = tiktoken.get_encoding({c.get('tokenizer')!r})")
    add("total = lines = 0")
    add("with open(sys.argv[1], encoding='utf-8') as fh:")
    add("    for line in fh:")
    add("        rec = json.loads(line)")
    add("        lines += 1")
    add("        for msg in rec['request']['messages']:")
    add("            compact = json.dumps(msg, ensure_ascii=False, sort_keys=True,")
    add("                                 separators=(',', ':'))")
    add("            total += len(enc.encode_ordinary(compact))")
    add("print('lines', lines, 'tokens', total)")
    add("```")
    add("")
    add("```bash")
    add("python recompute.py gpt-6-astra-0009-20261001.jsonl   # 行数/ token 应与上表一致")
    add("```")
    add("")
    add("## 四、账本与闭合关系")
    add("")
    add(f"- 停止原因：`{m['pipeline']['stop_reason']}`；停止文件："
        f"`{m['pipeline']['stop_file']}`（此后源文件一条不取）")
    add(f"- 源侧交付 token：{m['pipeline']['delivered_tokens']:,}")
    add("- 闭合式（与 `ledger.json` 逐文件可对）：")
    add("  - `scanned = 各过滤原因 + passed`（含 `has_image`）")
    add("  - `passed = session_removed + prefix_removed + kept_records`")
    add("  - `kept_records = delivered_records + not_delivered`")
    add("  - `delivered_windows ≥ delivered_records`（切窗记录一行多窗口）")
    add("")
    add("## 五、重复内容说明（`content_dup_pairs`）")
    add("")
    add("`content_dup_pairs` 统计的是**内容相同、但 session uuid 不同**的记录对。按锁定口径，"
        "`seq` 以 session 为边界计算，这类记录属于**不同记录**，因此**已原样保留**；"
        "此计数仅提示内容重复，不是缺陷，不应当作需要删除的重复数据。"
        "（同一 session 内 `seq` 互为前缀的记录仍按去重规则丢弃，那部分已计入 `prefix_removed`。）")
    add("")
    add("## 六、完整性")
    add("")
    add("每个对象在桶上的 `ContentLength` 与 `x-amz-meta-sha256` 都已在上传时核对；"
        "最大的分片做过全文回读 sha256 比对；桶内对象数与 `delivery-manifest.json` 的 "
        f"`object_count`（{m['object_count']}）一致。逐片 `sha256_raw`（未压缩）与 "
        "`sha256_zst`（压缩后）都在清单里，可任选一侧复算。")
    add("")
    return "\n".join(lines)


# ---------------------------------------------------------------- 传输层

class S3Transport:
    """真实 B2（S3 兼容）。凭证只从环境变量读。"""

    def __init__(self, bucket: str, endpoint: str, region: str):
        import boto3
        from botocore.config import Config

        ak = os.environ.get("B2_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
        sk = os.environ.get("B2_APP_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not ak or not sk:
            raise DeliveryError("缺少凭证：请设置 B2_KEY_ID / B2_APP_KEY（只在环境变量里）")
        self.bucket = bucket
        self.client = boto3.client(
            "s3", endpoint_url=endpoint, region_name=region,
            aws_access_key_id=ak, aws_secret_access_key=sk,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          retries={"max_attempts": 5}, max_pool_connections=8),
        )

    def list_prefix(self, prefix: str) -> dict:
        out, token = {}, None
        while True:
            kw = dict(Bucket=self.bucket, Prefix=prefix, MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            r = self.client.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                out[o["Key"]] = {"size": int(o["Size"])}
            if not r.get("IsTruncated"):
                break
            token = r["NextContinuationToken"]
        return out

    def head(self, key: str):
        from botocore.exceptions import ClientError
        try:
            r = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return {"size": int(r.get("ContentLength") or 0),
                "metadata": {k.lower(): v for k, v in (r.get("Metadata") or {}).items()},
                "etag": (r.get("ETag") or "").strip('"')}

    def put_file(self, key: str, path: Path, metadata: dict, content_type: str) -> dict:
        extra = {"ContentType": content_type,
                 "Metadata": {k: str(v) for k, v in metadata.items() if v not in (None, "")}}
        with open(path, "rb") as fh:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=fh, **extra)
        h = self.head(key)
        return {"etag": (h or {}).get("etag", "")}

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def hash_object(self, key: str) -> str:
        h = hashlib.sha256()
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            while True:
                block = body.read(CHUNK)
                if not block:
                    break
                h.update(block)
        finally:
            body.close()
        return h.hexdigest()


class LocalTransport:
    """自测用的假 S3：``<root>/objects/<key>`` + ``<root>/meta/<key>.json``。

    只在 ``--simulate-local-s3`` 下启用；语义与 S3Transport 对齐（含中断注入）。
    """

    def __init__(self, root: Path, fail_after: int = 0):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.fail_after = int(fail_after or 0)
        self.puts = 0
        self.bucket = "simulated"

    def _obj(self, key: str) -> Path:
        return self.root / "objects" / key

    def _meta(self, key: str) -> Path:
        return self.root / "meta" / (key + ".json")

    def list_prefix(self, prefix: str) -> dict:
        base = self.root / "objects"
        out = {}
        if not base.exists():
            return out
        for p in base.rglob("*"):
            if p.is_file():
                rel = p.relative_to(base).as_posix()
                if rel.startswith(prefix):
                    out[rel] = {"size": p.stat().st_size}
        return out

    def head(self, key: str):
        p = self._obj(key)
        if not p.exists():
            return None
        meta = {}
        if self._meta(key).exists():
            meta = read_json(self._meta(key)).get("metadata") or {}
        return {"size": p.stat().st_size, "metadata": meta, "etag": "sim"}

    def put_file(self, key: str, path: Path, metadata: dict, content_type: str) -> dict:
        if self.fail_after and self.puts >= self.fail_after:
            # 模拟"put 到一半进程被杀"：留下半截对象、没有 metadata
            p = self._obj(key)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "rb") as src, open(p, "wb") as dst:
                dst.write(src.read(max(1, path.stat().st_size // 2)))
            raise SimulatedCrash(f"模拟中断：{key}")
        p = self._obj(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "rb") as src, open(p, "wb") as dst:
            while True:
                block = src.read(CHUNK)
                if not block:
                    break
                dst.write(block)
        mp = self._meta(key)
        mp.parent.mkdir(parents=True, exist_ok=True)
        write_json(mp, {"metadata": {k: str(v) for k, v in metadata.items() if v not in (None, "")},
                        "content_type": content_type, "at": now_iso()})
        self.puts += 1
        return {"etag": "sim"}

    def read_bytes(self, key: str) -> bytes:
        return self._obj(key).read_bytes()

    def hash_object(self, key: str) -> str:
        return sha256_file(self._obj(key))


# ---------------------------------------------------------------- 上传

def _append_journal(staging: Path, event: str, **fields) -> None:
    row = {"ts": now_iso(), "event": event}
    row.update({k: v for k, v in fields.items() if v is not None})
    with open(staging / UPLOAD_JOURNAL, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def load_journal(staging: Path) -> dict:
    """按 key 汇总：最近一次 start 之后没有 put -> in_flight（说明上次是半截上传）。"""
    p = staging / UPLOAD_JOURNAL
    out = {"target": None, "objects": {}}
    if not p.exists():
        return out
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue          # 尾部半行（进程被杀）忽略
            if row.get("event") == "target":
                out["target"] = row.get("prefix")
                continue
            key = row.get("key")
            if not key:
                continue
            st = out["objects"].setdefault(key, {"in_flight": False, "puts": 0,
                                                 "delivery_id": None, "sha256": None})
            if row.get("event") == "start":
                st["in_flight"] = True
                st["delivery_id"] = row.get("delivery_id")
                st["sha256"] = row.get("sha256")
            elif row.get("event") in ("put", "skip", "verified", "repaired"):
                st["in_flight"] = False
                st["sha256"] = row.get("sha256") or st["sha256"]
                if row.get("event") in ("put", "repaired"):
                    st["puts"] += 1
    return out


def resolve_target(transport, args, staging: Path, journal: dict) -> str:
    root = args.root_prefix
    if root and not root.endswith("/"):
        root += "/"
    if args.target_prefix:
        tp = args.target_prefix
        return tp if tp.endswith("/") else tp + "/"

    state = _load_pack_state(staging)
    pinned = state.get("target_prefix") or journal.get("target")
    pinned_bucket = state.get("bucket")
    if pinned_bucket and transport.bucket and pinned_bucket != transport.bucket:
        print(f"[target] 暂存目录上次写的是另一个桶（{pinned_bucket}），不沿用 {pinned}")
        pinned = None
    if pinned and not args.new_target:
        pinned = str(pinned)
        print(f"[target] 续跑：沿用上次的目标目录 {pinned}")
        return pinned if pinned.endswith("/") else pinned + "/"

    base = args.date or time.strftime("%Y-%m-%d", time.gmtime())
    if getattr(args, "rolling", False) and not args.new_target:
        existing = f"{root}{base}/"
        if transport.head(existing + DELIVERY_MANIFEST) is not None:
            print(f"[target] rolling：沿用已有交付目录 {existing}")
            return existing
    candidates = [base] + [f"{base}-delivery-{i}" for i in range(1, 100)]
    for cand in candidates:
        prefix = f"{root}{cand}/"
        if not transport.list_prefix(prefix):
            print(f"[target] 选定全新目录 {prefix}")
            return prefix
    raise DeliveryError(f"{base} 与 {base}-delivery-1..99 都已被占用，拒绝覆盖；请人工指定 --target-prefix")


def _classify(remote, obj: Object, journal_row, allow_repair: bool) -> str:
    """-> skip / upload / repair / conflict"""
    if remote is None:
        return "upload"
    md = remote.get("metadata") or {}
    want = obj.sha256
    if md.get("sha256") == want and remote.get("size") == obj.size:
        return "skip"
    if md.get("sha256"):
        return "conflict"                       # 同名且哈希不同：绝不覆盖
    # 没有 sha256 元数据：只有本次交付自己写了一半的才允许重传
    if (allow_repair and journal_row and journal_row.get("in_flight")
            and journal_row.get("delivery_id") == obj.metadata.get("delivery-id")):
        if remote.get("size") == obj.size:
            return "verify"                     # 尺寸对但没元数据：回读哈希再决定
        return "repair"
    return "conflict"


def load_prev_manifest(transport, target: str):
    """读桶上已有的 delivery-manifest.json（rolling 的「上一轮」）。没有就返回 (None, None)。"""
    key = target + DELIVERY_MANIFEST
    if transport.head(key) is None:
        return None, None
    raw = transport.read_bytes(key)
    try:
        return json.loads(raw), raw
    except ValueError:
        raise DeliveryError(f"远端 {key} 不是合法 JSON，无法做 rolling 的「只追加」校验")


def prev_subset_ok(prev_manifest: dict, plan: list, target: str) -> tuple:
    """上一轮清单里的对象必须全部出现在本轮计划里，且 sha256/字节数不变。

    rolling 只允许**追加分片**，不允许删、不允许改（分片不可变）。
    """
    current = {o.name: (o.sha256, o.size) for o in plan}
    problems = []
    for row in prev_manifest.get("objects") or []:
        name = row.get("name")
        if not name:
            problems.append("上一轮清单里有缺 name 的对象")
            continue
        if name not in current:
            problems.append(f"上一轮的 {name} 不在本轮计划里（rolling 只能追加，不能删）")
        elif current[name] != (row.get("sha256"), row.get("size")):
            problems.append(f"{name} 内容与本轮不同（分片不可变）")
    return (not problems), problems


def snapshot_prev_manifest(transport, target: str, staging: Path):
    """把桶上即将被替换的旧清单原样存到 manifests/<utc>-<sha12>.json。"""
    key = target + DELIVERY_MANIFEST
    if transport.head(key) is None:
        return None
    raw = transport.read_bytes(key)
    sha = hashlib.sha256(raw).hexdigest()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    name = f"{SNAPSHOT_DIR}/{stamp}-{sha[:12]}.json"
    dest = staging / SNAPSHOT_DIR / f"{stamp}-{sha[:12]}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    skey = target + name
    if transport.head(skey) is None:          # 已存在同样快照就不重写
        dest.write_bytes(raw)
        transport.put_file(skey, dest,
                           {"sha256": sha, "kind": "manifest-snapshot"}, "application/json")
        print(f"[rolling] 旧清单已快照 → {skey}")
    return skey



def _file_sig(f: dict) -> str:
    return json.dumps([[p.get("name"), p.get("sha256_zst")] for p in f.get("parts") or []],
                      sort_keys=True)


def _merge_totals(files: list) -> dict:
    t = {
        "files": len(files),
        "parts": sum(len(f.get("parts") or []) for f in files),
        "records": sum(int(f.get("records") or 0) for f in files),
        "windows": sum(int(f.get("windows") or 0) for f in files),
        "tokens": sum(int(f.get("tokens") or 0) for f in files),
        "bytes_raw": sum(int(f.get("bytes_raw") or 0) for f in files),
        "bytes_zst": sum(int(f.get("bytes_zst") or 0) for f in files),
    }
    t["compression_ratio"] = (round(t["bytes_raw"] / t["bytes_zst"], 3)
                              if t["bytes_zst"] else None)
    return t


def merge_manifests(prev: dict, local: dict) -> dict:
    """rolling 的清单合并：只增不减。上一轮的文件行/对象行并进本轮清单。

    多台机各写各的 key 时，本地清单只含自己那几件——不合并的话第二轮清单会把
    第一轮的交付"从清单里抹掉"。这里按 source_key / name 取并集；同名但内容不同
    说明两轮对同一个对象有两种说法，直接冲突（绝不静默覆盖）。
    """
    files = {f["source_key"]: f for f in prev.get("files") or []}
    for f in local.get("files") or []:
        old = files.get(f["source_key"])
        if old is not None and _file_sig(old) != _file_sig(f):
            raise ConflictError(f"同一源文件在两轮里内容不同：{f['source_key']}")
        files[f["source_key"]] = f

    objects = {o["name"]: o for o in prev.get("objects") or []}
    for o in local.get("objects") or []:
        old = objects.get(o["name"])
        if old is not None and old.get("sha256") != o.get("sha256"):
            raise ConflictError(f"同名分片在两轮里内容不同：{o['name']}")
        objects[o["name"]] = o

    prev_objects = {o["name"]: o for o in prev.get("objects") or []}
    prev_files = {f["source_key"]: f for f in prev.get("files") or []}
    no_new = all(name in prev_objects and prev_objects[name].get("sha256") == row.get("sha256")
                 for name, row in objects.items() if prev_objects)
    no_new = no_new and all(key in prev_files and _file_sig(prev_files[key]) == _file_sig(row)
                            for key, row in files.items())
    if no_new and prev_objects:
        return prev          # 这一轮没有新增：原样沿用上一轮清单，保证重跑幂等

    merged = dict(local)
    merged["files"] = sorted(files.values(), key=lambda f: f["source_key"])
    merged["objects"] = sorted(objects.values(), key=lambda o: o["name"])
    merged["totals"] = _merge_totals(merged["files"])
    extras = {e["name"]: e for e in prev.get("extra_documents") or []}
    for e in local.get("extra_documents") or []:
        # 随附文件（manifest.json / ledger.json / accept-report）各轮内容不同是常态：
        # 以本轮为准，不在两轮之间做冲突判断（分片仍然严格不可变）
        extras[e["name"]] = e
    merged["extra_documents"] = sorted(extras.values(), key=lambda e: e["name"])
    merged["object_count"] = len(merged["objects"]) + 2 + len(merged["extra_documents"])
    ident = json.dumps(sorted([[o["name"], o["sha256"], o["size"]]
                               for o in merged["objects"]]),
                       ensure_ascii=False, separators=(",", ":"))
    merged["delivery_id"] = hashlib.sha256(ident.encode("utf-8")).hexdigest()

    prov = dict(local.get("provenance") or {})
    machines = sorted({m for f in merged["files"] for m in (f.get("machines") or [])})
    per = {}
    for f in merged["files"]:
        for part in f.get("parts") or []:
            m = part.get("machine") or ""
            if m:
                per[m] = per.get(m, 0) + 1
    prov.update({"machines": machines, "multi_machine": len(machines) > 1,
                 "shards_per_machine": per})
    merged["provenance"] = prov
    merged["rolling"] = {
        "rounds": int((prev.get("rolling") or {}).get("rounds", 1)) + 1,
        "previous_delivery_id": prev.get("delivery_id"),
    }
    return merged


def write_merged_docs(packed: Packed, merged: dict) -> None:
    """把合并后的清单/README 写回暂存目录，并同步 packed.docs 的哈希。"""
    staging = packed.staging
    readme_path = staging / DELIVERY_README
    readme_path.write_text(_render_readme(packed, merged), encoding="utf-8")
    merged["readme"] = {"name": DELIVERY_README,
                        "size": readme_path.stat().st_size,
                        "sha256": sha256_file(readme_path)}
    write_json(staging / DELIVERY_MANIFEST, merged)
    mpath = staging / DELIVERY_MANIFEST
    packed.docs = [
        Object(name=DELIVERY_README, path=readme_path, size=readme_path.stat().st_size,
               sha256=sha256_file(readme_path),
               content_type="text/markdown; charset=utf-8", kind="doc",
               metadata={"sha256": sha256_file(readme_path),
                         "delivery-id": merged["delivery_id"], "spec": SCHEMA}),
        Object(name=DELIVERY_MANIFEST, path=mpath, size=mpath.stat().st_size,
               sha256=sha256_file(mpath), content_type="application/json", kind="doc",
               metadata={"sha256": sha256_file(mpath),
                         "delivery-id": merged["delivery_id"], "spec": SCHEMA}),
    ]
    packed.manifest_obj = packed.docs[1]


def upload(packed: Packed, transport, args, put: bool = True) -> dict:
    staging = packed.staging
    target = packed.target_prefix
    rolling = bool(getattr(args, "rolling", False))
    prev_manifest = None
    if rolling:
        prev_manifest, _prev_raw = load_prev_manifest(transport, target)
        if prev_manifest is not None:
            local_manifest = read_json(staging / DELIVERY_MANIFEST)
            merged = merge_manifests(prev_manifest, local_manifest)
            if not _payload_equal(merged, read_json(staging / DELIVERY_MANIFEST), ()):
                # 本轮要上传的清单就是合并后的这份（哪怕"没有新增"，也要把上一轮的
                # 完整清单原样写回，避免把桶里的清单替换成只含本机那几件的版本）
                write_merged_docs(packed, merged)
                print(f"[rolling] 清单已合并：上一轮 {len(prev_manifest.get('objects') or [])} 个对象"
                      f" + 本轮 {len(local_manifest.get('objects') or [])} 个对象"
                      f" → 交付对象数 {merged['object_count']}")
    journal = load_journal(staging)
    plan = packed.all_objects()
    merged_manifest = read_json(staging / DELIVERY_MANIFEST)
    all_expected = {target + o["name"] for o in merged_manifest.get("objects") or []}
    all_expected |= {o.key(target) for o in packed.docs}
    all_expected |= {o.key(target) for o in packed.extras}
    prev_objs = {target + o["name"]: {"size": o.get("size"), "sha256": o.get("sha256")}
                 for o in (prev_manifest or {}).get("objects") or []
                 if target + o["name"] not in {x.key(target) for x in plan}}
    remote = transport.list_prefix(target)
    # 列表只给大小，sha256 在对象元数据里：对已存在的同名对象补一次 head
    heads = {o.key(target): transport.head(o.key(target))
             for o in plan if o.key(target) in remote}

    decisions = []
    conflicts = []
    snap_prefix = target + SNAPSHOT_DIR + "/"

    if remote and not args.allow_extra_objects:
        for k in sorted(set(remote) - all_expected):
            if rolling and k.startswith(snap_prefix):
                continue        # rolling 的历史清单快照是有意留下的
            conflicts.append({
                "key": k,
                "reason": "目标目录里已有非本次交付的对象（本工具只写全新目录，绝不覆盖；"
                          "换 --new-target，或确认后加 --allow-extra-objects）"})

    for obj in plan:
        key = obj.key(target)
        row = journal["objects"].get(key)
        decision = _classify(heads.get(key), obj, row, allow_repair=not args.no_repair)
        if decision == "conflict" and rolling and obj.kind in ("doc", "extra"):
            decision = "replace"       # 只有清单/README/随附证据允许整体替换（分片永不可变）
        if decision == "conflict":
            r = heads.get(key) or {}
            reasons = []
            if r:
                md = (r.get("metadata") or {})
                if md.get("sha256"):
                    reasons.append(f"远端 sha256={md['sha256']} ≠ 本地 {obj.sha256}")
                else:
                    reasons.append("远端无 sha256 元数据，无法证明是本次交付的半截对象")
                if r.get("size") != obj.size:
                    reasons.append(f"远端 {r.get('size')} 字节 ≠ 本地 {obj.size} 字节")
            conflicts.append({"key": key, "reason": "；".join(reasons) or "同名对象"})
        decisions.append((obj, key, decision))

    if conflicts:
        _append_journal(staging, "conflict", count=len(conflicts),
                        keys=[c["key"] for c in conflicts])
        lines = "\n".join(f"  - {c['key']}: {c['reason']}" for c in conflicts)
        raise ConflictError(
            "远端已有同名对象但内容不同，**整批停止**（一个字节都没写，绝不覆盖）：\n" + lines)

    if not put:
        readbacks = verify_remote(packed, transport, target,
                                  allow_extra=args.allow_extra_objects, rolling=rolling,
                                  extra_expected=prev_objs)
        report = {
            "schema": SCHEMA, "generated_at": now_iso(),
            "delivery_id": packed.delivery_id, "bucket": transport.bucket,
            "target_prefix": target,
            "object_count": {"expected": len(all_expected),
                             "remote": len([k for k in transport.list_prefix(target)
                                            if not k.startswith(target + SNAPSHOT_DIR + "/")])},
            "uploaded": [], "repaired": [], "skipped": [
                k for _, k, d in decisions if d in ("skip", "verify")],
            "readback": readbacks, "totals": packed.totals, "ok": True,
        }
        write_json(staging / UPLOAD_REPORT, report)
        return report

    # 上传顺序：分片 -> README -> manifest（清单最后）
    order = {o.name: i for i, o in enumerate(packed.objects)}
    ordered = sorted(decisions, key=lambda t: (t[0].kind != "shard", order.get(t[0].name, 999)))

    done, repaired, skipped, snapshots, replaced = [], [], [], [], []
    for obj, key, decision in ordered:
        if decision == "skip":
            skipped.append(key)
            _append_journal(staging, "skip", key=key, sha256=obj.sha256, size=obj.size)
            print(f"[upload] 跳过（已存在且 sha256 相同）{key}")
            continue
        if decision == "verify":
            got = transport.hash_object(key)
            if got == obj.sha256:
                skipped.append(key)
                _append_journal(staging, "skip", key=key, sha256=obj.sha256, size=obj.size)
                print(f"[upload] 跳过（回读哈希一致）{key}")
                continue
            decision = "repair"

        if decision == "replace" and obj.name == DELIVERY_MANIFEST:
            snap = snapshot_prev_manifest(transport, target, staging)
            if snap:
                snapshots.append(snap)

        _append_journal(staging, "start", key=key, sha256=obj.sha256, size=obj.size,
                        delivery_id=obj.metadata.get("delivery-id"))
        label = {"repair": "重传半截对象", "replace": "替换文档（rolling）"}.get(decision, "上传")
        print(f"[upload] {label} {key} ({human_bytes(obj.size)})")
        transport.put_file(key, obj.path, obj.metadata, obj.content_type)

        h = transport.head(key)
        if h is None:
            raise VerifyError(f"上传后 head_object 找不到对象：{key}")
        if h["size"] != obj.size:
            raise VerifyError(f"ContentLength 不符：{key} 远端 {h['size']} ≠ 本地 {obj.size}")
        if (h.get("metadata") or {}).get("sha256") != obj.sha256:
            raise VerifyError(f"对象元数据 sha256 不符：{key}")
        _append_journal(staging,
                        {"repair": "repaired", "replace": "replaced"}.get(decision, "put"),
                        key=key, sha256=obj.sha256, size=obj.size, etag=h.get("etag"))
        if decision == "replace":
            replaced.append(key)
        else:
            (repaired if decision == "repair" else done).append(key)

    # 对象集合必须与 manifest 完全一致 + 回读抽检
    readbacks = verify_remote(packed, transport, target,
                              allow_extra=args.allow_extra_objects, rolling=rolling,
                              extra_expected=prev_objs)

    report = {
        "schema": SCHEMA,
        "generated_at": now_iso(),
        "delivery_id": packed.delivery_id,
        "bucket": transport.bucket,
        "target_prefix": target,
        "object_count": {"expected": len(all_expected),
                         "remote": len([k for k in transport.list_prefix(target)
                                        if not k.startswith(target + SNAPSHOT_DIR + "/")])},
        "uploaded": done,
        "repaired": repaired,
        "replaced": replaced,
        "snapshots": snapshots,
        "rolling": rolling,
        "skipped": skipped,
        "readback": readbacks,
        "totals": packed.totals,
        "ok": True,
    }
    write_json(staging / UPLOAD_REPORT, report)
    _append_journal(staging, "complete", count=report["object_count"]["remote"],
                    delivery_id=packed.delivery_id)
    return report


def verify_remote(packed: Packed, transport, target: str, allow_extra: bool = False,
                  rolling: bool = False, extra_expected: dict | None = None) -> list:
    """对象集合逐一对齐 + 每个对象 head 核大小/元数据 + 最大分片与文档全文回读。

    rolling 模式下，`<target>manifests/` 下的历史清单快照不算"多余对象"
    （它们是有意保留的可回溯副本）。
    """
    plan = packed.all_objects()
    remote = transport.list_prefix(target)
    expected = {o.key(target) for o in plan}
    expected |= set(extra_expected or {})
    got = set(remote)
    if rolling:
        snap_prefix = target + SNAPSHOT_DIR + "/"
        got = {k for k in got if not k.startswith(snap_prefix)}
    missing = sorted(expected - got)
    extra = sorted(got - expected)
    if missing or (extra and not allow_extra):
        raise VerifyError(
            f"桶内对象集合与 manifest 不一致（expected {len(expected)} / remote {len(got)}）："
            f"缺失 {missing or '无'}；多出 {extra or '无'}")
    if extra:
        print(f"[warn] 目标目录另有 {len(extra)} 个 manifest 之外的对象（--allow-extra-objects）：{extra}")
    for obj in plan:
        h = transport.head(obj.key(target))
        if h is None:
            raise VerifyError(f"对象缺失：{obj.key(target)}")
        if h["size"] != obj.size:
            raise VerifyError(f"ContentLength 不符：{obj.key(target)} 远端 {h['size']} "
                              f"≠ 本地 {obj.size}")
        if (h.get("metadata") or {}).get("sha256") != obj.sha256:
            raise VerifyError(f"对象元数据 sha256 不符：{obj.key(target)}")

    for key, want in (extra_expected or {}).items():
        h = transport.head(key)
        if h is None:
            raise VerifyError(f"上一轮的交付对象不见了：{key}")
        if h["size"] != want["size"]:
            raise VerifyError(f"上一轮对象字节数变了：{key} {h['size']} ≠ {want['size']}")
        if (h.get("metadata") or {}).get("sha256") != want["sha256"]:
            raise VerifyError(f"上一轮对象 sha256 变了：{key}")

    readbacks = []
    shards = sorted(packed.objects, key=lambda o: o.size, reverse=True)
    targets = ([shards[0]] if shards else []) + list(packed.docs)
    for obj in targets:
        key = obj.key(target)
        got_sha = transport.hash_object(key)
        ok = got_sha == obj.sha256
        readbacks.append({"key": key, "sha256": got_sha, "expected": obj.sha256,
                          "ok": ok, "size": obj.size})
        if not ok:
            _append_journal(packed.staging, "verify_failed", key=key, sha256=got_sha)
            raise VerifyError(f"全文回读 sha256 不符：{key}")
        _append_journal(packed.staging, "verified", key=key, sha256=got_sha)
        print(f"[verify] 回读一致 {key} ({human_bytes(obj.size)})")
    return readbacks


# ---------------------------------------------------------------- dry-run

def print_plan(delivery: Delivery, args, staging: Path) -> None:
    m = delivery.manifest
    date = args.date or time.strftime("%Y-%m-%d", time.gmtime())
    target_candidate = args.target_prefix or f"{date}/"
    print("=== dry-run：未加 --execute，不压缩、不写盘、不连网，只打印计划 ===")
    print(f"交付目录    : {delivery.root.resolve()}")
    print(f"暂存目录    : {staging.resolve()}"
          + ("（已存在压缩产物会复用）" if (staging / PACK_STATE).exists() else ""))
    print(f"数据源      : {m.get('config', {}).get('bucket', DEFAULT_BUCKET)} / "
          f"{m.get('config', {}).get('prefix', '')}")
    print(f"停止状态    : {m.get('STOP_REASON')} / stop_file={m.get('stop_file')} / "
          f"delivered_tokens={int(m.get('delivered_tokens') or 0):,}")
    print(f"口径        : A1（任一条 message 含图 → 整条丢弃）| tokenizer "
          f"{m.get('config', {}).get('tokenizer', 'o200k_base')} / encode_ordinary")
    print(f"待打包      : {delivery.windows:,} 行 / {delivery.records:,} 条源记录 / "
          f"{delivery.tokens:,} token")
    print(f"文件        : {len(delivery.sources)} 个源文件 → {len(delivery.parts)} 个分片 "
          f"→ 同名加 .zst")
    if delivery.machines:
        per = {m: sum(1 for p in delivery.parts if p.machine == m) for m in delivery.machines}
        print(f"合并来源    : {len(delivery.machines)} 台机 {delivery.machines} / 逐机分片 "
              f"{per}" + (f" / merged_from={delivery.merged_from}" if delivery.merged_from else ""))
    print(f"未压缩合计  : {human_bytes(sum(p.path.stat().st_size for p in delivery.parts))} "
          f"（压缩后未估算，执行时记录）")
    print(f"上传目标    : {target_candidate}（--execute 时才检查是否被占用；"
          f"占用则 ……-delivery-1 递增）")
    print(f"对象数      : {len(delivery.parts) + 2} = {len(delivery.parts)} 分片 + "
          f"{DELIVERY_MANIFEST} + {DELIVERY_README}")
    print("校验        : 每对象 head_object 核 ContentLength + metadata sha256；"
          "最大分片与两个文档全文回读比对；桶内对象集合与 manifest 完全一致")
    print("幂等        : 同名 + 同 sha256 → 跳过；同名 + 不同 sha256 → 整批停（退出码 3）；"
          "上次自己写了一半的对象 → 重传")
    if getattr(args, "rolling", False):
        print("滚动上传    : 开启（分片只追加；manifest/README 可在「上一轮对象集 ⊆ 本轮」时替换，"
              "旧清单存 manifests/ 快照）")
    print("")
    print("执行方式（T14 收尾时原样跑；凭证先 export 到环境变量）：")
    print("  export B2_KEY_ID=… B2_APP_KEY=…")
    print(f"  python delivery/delivery_pack.py {delivery.root} --execute --date {date}")
    print("先只看计划（不连网、不写盘）：")
    print(f"  python delivery/delivery_pack.py {delivery.root} --date {date}")


# ---------------------------------------------------------------- CLI

def build_parser(argv=None):
    ap = argparse.ArgumentParser(
        description="交付打包 + 上传（A1 口径；默认 dry-run，--execute 才压缩/上传）")
    ap.add_argument("delivery_dir", help="pipeline 的 --out-dir（含 manifest.json / 分片）")
    ap.add_argument("--execute", action="store_true", help="真正压缩 + 上传（默认只打印计划）")
    ap.add_argument("--phase", choices=["all", "pack", "upload", "verify"], default="all",
                    help="all=打包+上传+校验；pack=只打包；upload=打包(可复用)+上传；verify=只核对")
    ap.add_argument("--staging-dir", help="压缩产物/manifest 的暂存目录（默认 <交付目录>/_delivery_pack）")
    ap.add_argument("--level", type=int, default=10, help="zstd 压缩级别（默认 10）")
    ap.add_argument("--threads", type=int, default=0, help="zstd 线程数（0=按核数）")
    ap.add_argument("--bucket", default=None, help=f"目标桶（默认取 pipeline manifest，回退 {DEFAULT_BUCKET}）")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--root-prefix", default="", help="日期目录的父前缀（默认桶根）")
    ap.add_argument("--date", default=None, help="日期目录名（默认今天 UTC，形如 2026-11-04）")
    ap.add_argument("--target-prefix", default=None, help="显式指定完整目标前缀（调试/续跑用）")
    ap.add_argument("--new-target", action="store_true", help="忽略上次记录，重新挑一个全新日期目录")
    ap.add_argument("--rolling", action="store_true",
                    help="滚动上传：同一日期目录多轮追加分片；只有 manifest/README 可替换"
                         "（且上一轮对象集必须是本轮子集），旧清单留 manifests/ 快照")
    ap.add_argument("--no-repair", action="store_true",
                    help="连自己上次写了一半的对象也不重传（遇到就整批停）")
    ap.add_argument("--allow-count-mismatch", action="store_true",
                    help="分片行数与 manifest 不符时只警告（默认硬失败）")
    ap.add_argument("--allow-closure-mismatch", action="store_true",
                    help="账本不闭合时只警告（默认硬失败）")
    ap.add_argument("--allow-stop-reason", action="append", default=[], metavar="REASON",
                    help="额外放行的 STOP_REASON（合并目录自定义终态用，可重复）")
    ap.add_argument("--allow-extra-objects", action="store_true",
                    help="目标目录里有 manifest 之外的对象时只警告（默认视为不一致并失败）")
    ap.add_argument("--include", action="append", default=[], metavar="PATH",
                    help="随交付一起上传的附加文件（如 accept-report.json），可重复")
    ap.add_argument("--force-pack", action="store_true", help="忽略压缩缓存，全部重压")
    ap.add_argument("--ignore-lock", action="store_true", help="忽略 run.lock（确认没有实例在跑时用）")
    ap.add_argument("--lock-ttl", type=int, default=300)
    ap.add_argument("--simulate-local-s3", default=None,
                    help="自测专用：用本地目录假装 S3（不连网、不需要凭证）")
    ap.add_argument("--simulate-fail-after", type=int, default=0,
                    help="自测专用：第 N 个 put 之后模拟进程被杀（留下半截对象）")
    return ap


def main(argv=None) -> int:
    args = build_parser(argv).parse_args(argv)
    if args.simulate_fail_after and not args.simulate_local_s3:
        print("[error] --simulate-fail-after 只能配合 --simulate-local-s3 自测用", file=sys.stderr)
        return 2
    root = Path(args.delivery_dir)
    staging = Path(args.staging_dir) if args.staging_dir else root / "_delivery_pack"

    try:
        delivery = inspect_delivery(
            root, allow_lock=args.ignore_lock, lock_ttl=args.lock_ttl,
            allow_count_mismatch=args.allow_count_mismatch,
            allow_closure_mismatch=args.allow_closure_mismatch,
            allow_stop_reasons=tuple(args.allow_stop_reason or ()))
    except DeliveryError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 2

    if not args.execute:
        print_plan(delivery, args, staging)
        return 0

    manifest = delivery.manifest
    cfg = manifest.get("config") or {}
    bucket = args.bucket or cfg.get("bucket") or DEFAULT_BUCKET
    transport = None
    if args.simulate_local_s3:
        transport = LocalTransport(Path(args.simulate_local_s3), fail_after=args.simulate_fail_after)
        bucket = transport.bucket
    elif args.phase != "pack":
        try:
            transport = S3Transport(bucket, args.endpoint, args.region)
        except DeliveryError as e:
            print(f"[error] {e}", file=sys.stderr)
            return 2

    if args.phase == "verify":
        state = _load_pack_state(staging)
        if not (args.target_prefix or state.get("target_prefix")
                or load_journal(staging).get("target")):
            print("[error] --phase verify 需要已知目标目录：先跑一次上传，或显式给 --target-prefix",
                  file=sys.stderr)
            return 2

    target = ""
    if transport is not None:
        try:
            target = resolve_target(transport, args, staging, load_journal(staging))
        except DeliveryError as e:
            print(f"[error] {e}", file=sys.stderr)
            return 2

    source_url = (f"s3://{cfg.get('bucket') or DEFAULT_BUCKET}/"
                  f"{cfg.get('prefix') or ''}")
    try:
        packed = pack(delivery, staging, args.level, args.threads, target,
                      source_url, force=args.force_pack, bucket=bucket,
                      include=args.include)
    except DeliveryError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 2

    t = packed.totals
    print(f"[pack] 完成：{t['parts']} 片 / {t['windows']:,} 行 / {t['tokens']:,} token / "
          f"{human_bytes(t['bytes_raw'])} → {human_bytes(t['bytes_zst'])} "
          f"({t['compression_ratio']}×) / delivery_id={packed.delivery_id[:16]}…")
    print(f"[pack] 清单：{staging / DELIVERY_MANIFEST}")
    print(f"[pack] 说明：{staging / DELIVERY_README}")

    if args.phase == "pack":
        return 0

    _append_journal(staging, "target", prefix=target, delivery_id=packed.delivery_id)
    try:
        report = upload(packed, transport, args, put=(args.phase in ("all", "upload")))
    except ConflictError as e:
        print(f"[conflict] {e}", file=sys.stderr)
        return 3
    except VerifyError as e:
        print(f"[verify-failed] {e}", file=sys.stderr)
        return 4
    except SimulatedCrash as e:
        print(f"[simulated-crash] {e}")
        return 137
    except DeliveryError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 2
    except Exception as e:  # 网络/5xx/进程被杀：已完成的进度都在流水里，原样重跑续跑
        print(f"[error] 上传中断：{type(e).__name__}: {e}\n"
              f"        已完成对象的进度在 {staging / UPLOAD_JOURNAL}，原样重跑同一命令即可续跑",
              file=sys.stderr)
        return 2

    if args.phase == "verify":
        print(f"[verify] 目标目录 {target}：{report['object_count']['remote']} 个对象全部核对通过")
        return 0

    print("")
    print("=== 上传完成 ===")
    print(f"目标        : s3://{bucket}/{target}")
    print(f"对象数      : {report['object_count']['remote']}（manifest 期望 "
          f"{report['object_count']['expected']}）")
    print(f"本次上传    : {len(report['uploaded'])}；续跑重传 {len(report['repaired'])}；"
          f"跳过 {len(report['skipped'])}")
    print(f"回读校验    : {len(report['readback'])} 个对象 sha256 一致")
    print(f"回读报告    : {staging / UPLOAD_REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
