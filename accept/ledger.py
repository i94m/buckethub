#!/usr/bin/env python3
"""验收侧：manifest 归一化 + 逐文件对账。

对账恒等式（锁定口径）：

    原始条数 = 各步移除量之和 + 交付条数

其中"各步移除量"是 parse_errors + filter_rs_id + filter_encrypted_content +
filter_min_messages + dedupe_session + dedupe_prefix，逐文件、逐阶段一一对上，
并且各文件之和要等于 manifest 的总量。

manifest 的字段名两边可能各写各的，所以这里做归一并**默认 fail-closed**：
出现认不出的移除步骤，直接报错而不是悄悄漏算（漏算比算错更危险）。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

MANIFEST_SCHEMA = "buckethub.dataset.manifest/v1"

#: 对账必须出现的移除步骤（缺一步就说明有阶段的量没记账）。
CANONICAL_STEPS = (
    "parse_errors",
    "filter_rs_id",
    "filter_encrypted_content",
    "filter_min_messages",
    "dedupe_session",
    "dedupe_prefix",
)

#: 只在"1B 到达即停"场景才出现：停止后不交付的那部分必须记账，否则对账不闭合。
OPTIONAL_STEPS = ("stop_cut",)

STOP_REASONS = ("token_target_reached", "source_exhausted")

_SOURCE_KEY_ALIASES = ("source_key", "key", "name", "file", "filename", "source", "object_key")
_RAW_LINES_ALIASES = ("raw_lines", "raw", "input_lines", "total_lines", "lines", "line_count")
_PARSE_ERRORS_ALIASES = ("parse_errors", "parse_error", "invalid_lines", "bad_lines", "parse_failures")
_REMOVED_ALIASES = ("removed", "removals", "removed_counts", "dropped", "drops", "removed_by_step", "rejected")
_DELIVERED_ALIASES = ("delivered_records", "delivered", "kept", "output_records", "kept_records", "records")
_DELIVERED_TOKENS_ALIASES = ("delivered_tokens", "tokens", "token_count", "kept_tokens")
_STATUS_ALIASES = ("status", "state", "result")
_WINDOW_PIECES_ALIASES = ("window_pieces", "windows", "window_extra", "split_pieces", "extra_pieces")

_NESTED_CONTAINERS = ("quality", "stats", "counts", "metrics", "numbers")


@dataclass
class Problem:
    level: str  # error | warn
    where: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"level": self.level, "where": self.where, "message": self.message}


@dataclass
class FileLedger:
    source_key: str
    raw_lines: int = 0
    removed: dict[str, int] = field(default_factory=dict)
    delivered_records: int = 0
    delivered_tokens: int = 0
    window_pieces: int = 0
    status: str = ""
    unknown_steps: dict[str, int] = field(default_factory=dict)

    @property
    def removed_total(self) -> int:
        return sum(self.removed.values())

    @property
    def accounted(self) -> int:
        return self.removed_total + self.delivered_records

    @property
    def balanced(self) -> bool:
        return self.raw_lines == self.accounted

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "raw_lines": self.raw_lines,
            "removed": dict(sorted(self.removed.items())),
            "removed_total": self.removed_total,
            "delivered_records": self.delivered_records,
            "delivered_tokens": self.delivered_tokens,
            "window_pieces": self.window_pieces,
            "accounted": self.accounted,
            "balanced": self.balanced,
            "status": self.status,
            **({"unknown_steps": self.unknown_steps} if self.unknown_steps else {}),
        }


@dataclass
class ShardLedger:
    name: str
    records: int = 0
    bytes: int = 0
    tokens: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "records": self.records, "bytes": self.bytes, "tokens": self.tokens}


@dataclass
class Manifest:
    schema: str = ""
    stop_reason: str = ""
    token_target: int = 0
    tokenizer: dict[str, Any] = field(default_factory=dict)
    total: FileLedger = field(default_factory=lambda: FileLedger(source_key="<totals>"))
    files: list[FileLedger] = field(default_factory=list)
    shards: list[ShardLedger] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "stop_reason": self.stop_reason,
            "token_target": self.token_target,
            "tokenizer": self.tokenizer,
            "totals": self.total.as_dict(),
            "files": [f.as_dict() for f in self.files],
            "shards": [s.as_dict() for s in self.shards],
        }


# --------------------------------------------------------------------------
# 归一化
# --------------------------------------------------------------------------


def _norm_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(key).lower())


def _pick(mapping: dict[str, Any], aliases: Iterable[str], default: Any = None) -> Any:
    lowered = {_norm_key(k): v for k, v in mapping.items()}
    for alias in aliases:
        if _norm_key(alias) in lowered:
            return lowered[_norm_key(alias)]
    return default


def classify_step(name: str) -> str | None:
    """把任意命名的移除步骤归到锁定口径的 6 个名字之一；认不出返回 None。"""
    key = _norm_key(name)
    if not key:
        return None
    if "parse" in key or "invalid" in key or "badline" in key:
        return "parse_errors"
    if "prefix" in key:
        return "dedupe_prefix"
    if "session" in key:
        return "dedupe_session"
    if "encrypt" in key:
        return "filter_encrypted_content"
    if "stop" in key or "target" in key or "cut" in key:
        return "stop_cut"
    if "rsid" in key or "reasonid" in key:
        return "filter_rs_id"
    if "message" in key or "round" in key or "turn" in key or "minmsg" in key or "100" in key:
        return "filter_min_messages"
    return None


def _collect_counts(mapping: dict[str, Any]) -> dict[str, Any]:
    """把散落在嵌套容器里的计数字段摊平（只看一层嵌套）。"""
    flat: dict[str, Any] = dict(mapping)
    for container in _NESTED_CONTAINERS:
        value = _pick(mapping, (container,))
        if isinstance(value, dict):
            for key, inner in value.items():
                flat.setdefault(key, inner)
    return flat


def normalize_removed(mapping: dict[str, Any]) -> tuple[dict[str, int], dict[str, int]]:
    """返回 (规范化移除量, 认不出的步骤)。"""
    canonical: dict[str, int] = {}
    unknown: dict[str, int] = {}
    for name, value in mapping.items():
        if not isinstance(value, (int, float)):
            continue
        step = classify_step(name)
        if step is None:
            unknown[str(name)] = int(value)
            continue
        canonical[step] = canonical.get(step, 0) + int(value)
    return canonical, unknown


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _file_ledger(entry: dict[str, Any]) -> FileLedger:
    flat = _collect_counts(entry)
    removed_raw = _pick(flat, _REMOVED_ALIASES, {}) or {}
    if not isinstance(removed_raw, dict):
        removed_raw = {}
    removed, unknown = normalize_removed(removed_raw)

    # 有的实现把 parse_errors 写在移除集合外面。
    parse_errors = _as_int(_pick(flat, _PARSE_ERRORS_ALIASES, 0))
    if parse_errors and "parse_errors" not in removed:
        removed["parse_errors"] = parse_errors

    ledger = FileLedger(
        source_key=str(_pick(flat, _SOURCE_KEY_ALIASES, "<unknown>")),
        raw_lines=_as_int(_pick(flat, _RAW_LINES_ALIASES, 0)),
        removed=removed,
        delivered_records=_as_int(_pick(flat, _DELIVERED_ALIASES, 0)),
        delivered_tokens=_as_int(_pick(flat, _DELIVERED_TOKENS_ALIASES, 0)),
        window_pieces=_as_int(_pick(flat, _WINDOW_PIECES_ALIASES, 0)),
        status=str(_pick(flat, _STATUS_ALIASES, "") or ""),
        unknown_steps=unknown,
    )
    return ledger


def _shard_ledger(entry: Any) -> ShardLedger:
    if isinstance(entry, str):
        return ShardLedger(name=entry)
    if not isinstance(entry, dict):
        return ShardLedger(name=str(entry))
    flat = _collect_counts(entry)
    return ShardLedger(
        name=str(_pick(flat, ("name", "shard", "file", "path", "key"), "<unknown>")),
        records=_as_int(_pick(flat, ("records", "count", "lines", "delivered_records"), 0)),
        bytes=_as_int(_pick(flat, ("bytes", "size", "byte_size"), 0)),
        tokens=_as_int(_pick(flat, ("tokens", "token_count"), 0)),
    )


def normalize_manifest(raw: dict[str, Any]) -> Manifest:
    if not isinstance(raw, dict):
        raise ValueError("manifest 顶层必须是 JSON 对象")

    totals_raw = _pick(raw, ("totals", "total", "summary", "grand_total"), {}) or {}
    if not isinstance(totals_raw, dict):
        totals_raw = {}
    total = _file_ledger({**totals_raw, "source_key": "<totals>"})

    files_raw = _pick(raw, ("files", "per_file", "sources", "inputs"), []) or []
    if isinstance(files_raw, dict):
        files_raw = [{"source_key": k, **(v if isinstance(v, dict) else {})} for k, v in files_raw.items()]
    files = [_file_ledger(entry) for entry in files_raw if isinstance(entry, dict)]

    shards_raw = _pick(raw, ("shards", "parts", "packages", "outputs"), []) or []
    if not isinstance(shards_raw, list):
        shards_raw = []
    shards = [_shard_ledger(entry) for entry in shards_raw]

    return Manifest(
        schema=str(_pick(raw, ("schema", "manifest_schema", "version"), "") or ""),
        stop_reason=str(_pick(raw, ("stop_reason", "stopreason", "stop", "STOP_REASON"), "") or ""),
        token_target=_as_int(_pick(raw, ("token_target", "target_tokens", "token_goal"), 0)),
        tokenizer=_pick(raw, ("tokenizer", "tokenization", "tokenizer_info"), {}) or {},
        total=total,
        files=files,
        shards=shards,
        raw=raw,
    )


def load_manifest(path: str | Path) -> Manifest:
    with open(path, "r", encoding="utf-8") as handle:
        return normalize_manifest(json.load(handle))


# --------------------------------------------------------------------------
# 对账
# --------------------------------------------------------------------------


def reconcile(
    manifest: Manifest,
    *,
    required_steps: Iterable[str] = CANONICAL_STEPS,
    allow_missing_steps: bool = False,
    strict_totals: bool = True,
) -> list[Problem]:
    problems: list[Problem] = []
    required = tuple(required_steps)

    if not manifest.files:
        problems.append(Problem("error", "manifest.files", "没有任何逐文件记录，无法逐文件对账"))

    for ledger in manifest.files:
        where = f"file:{ledger.source_key}"
        if ledger.unknown_steps:
            problems.append(
                Problem(
                    "error",
                    where,
                    "存在认不出的移除步骤，未纳入对账（漏算）："
                    + json.dumps(ledger.unknown_steps, ensure_ascii=False, sort_keys=True),
                )
            )
        if ledger.raw_lines < 0:
            problems.append(Problem("error", where, f"原始条数为负: {ledger.raw_lines}"))
        if ledger.raw_lines and not ledger.balanced:
            problems.append(
                Problem(
                    "error",
                    where,
                    "对账不闭合: 原始 {raw} ≠ 移除 {removed} + 交付 {delivered}（差 {diff:+d}）".format(
                        raw=ledger.raw_lines,
                        removed=ledger.removed_total,
                        delivered=ledger.delivered_records,
                        diff=ledger.raw_lines - ledger.accounted,
                    ),
                )
            )
        missing = [step for step in required if step not in ledger.removed]
        if missing and not allow_missing_steps:
            problems.append(
                Problem(
                    "error",
                    where,
                    "缺少移除步骤记账: " + ", ".join(missing) + "（该阶段一条都没删也必须写 0，否则无法证明跑过）",
                )
            )

    # ---- 总量 vs 逐文件 ----
    if manifest.files:
        sum_raw = sum(f.raw_lines for f in manifest.files)
        sum_delivered = sum(f.delivered_records for f in manifest.files)
        sum_tokens = sum(f.delivered_tokens for f in manifest.files)
        sum_removed = sum(f.removed_total for f in manifest.files)
        total = manifest.total

        if total.raw_lines and total.raw_lines != sum_raw:
            problems.append(
                Problem("error", "manifest.totals", f"总原始条数 {total.raw_lines} ≠ 逐文件之和 {sum_raw}")
            )
        if total.delivered_records and total.delivered_records != sum_delivered:
            problems.append(
                Problem(
                    "error",
                    "manifest.totals",
                    f"总交付条数 {total.delivered_records} ≠ 逐文件之和 {sum_delivered}",
                )
            )
        if total.delivered_tokens and strict_totals and total.delivered_tokens != sum_tokens:
            problems.append(
                Problem(
                    "error",
                    "manifest.totals",
                    f"总交付 token {total.delivered_tokens} ≠ 逐文件之和 {sum_tokens}"
                    f"（差 {total.delivered_tokens - sum_tokens:+d}）",
                )
            )
        if not total.raw_lines and not total.delivered_records:
            problems.append(Problem("warn", "manifest.totals", "缺少总量记录，只能逐文件对账"))

        # 总量自身也要闭合
        if total.raw_lines:
            expected_total_removed = sum_removed
            if total.removed_total and total.removed_total != expected_total_removed:
                problems.append(
                    Problem(
                        "error",
                        "manifest.totals",
                        f"总移除量 {total.removed_total} ≠ 逐文件之和 {expected_total_removed}",
                    )
                )
            if total.raw_lines != total.accounted:
                problems.append(
                    Problem(
                        "error",
                        "manifest.totals",
                        "总量对账不闭合: 原始 {raw} ≠ 移除 {removed} + 交付 {delivered}（差 {diff:+d}）".format(
                            raw=total.raw_lines,
                            removed=total.removed_total,
                            delivered=total.delivered_records,
                            diff=total.raw_lines - total.accounted,
                        ),
                    )
                )

    # ---- 分片 vs 总量 ----
    if manifest.shards:
        shard_records = sum(s.records for s in manifest.shards)
        shard_tokens = sum(s.tokens for s in manifest.shards)
        delivered = manifest.total.delivered_records or sum(f.delivered_records for f in manifest.files)
        windows = manifest.total.window_pieces or sum(f.window_pieces for f in manifest.files)
        tokens = manifest.total.delivered_tokens or sum(f.delivered_tokens for f in manifest.files)
        expected_records = delivered + windows
        if delivered and shard_records and shard_records != expected_records:
            problems.append(
                Problem(
                    "error",
                    "manifest.shards",
                    f"分片条数之和 {shard_records} ≠ 交付源记录 {delivered}"
                    + (f" + 切窗片段 {windows}" if windows else "")
                    + f" = {expected_records}",
                )
            )
        if tokens and shard_tokens and shard_tokens != tokens:
            problems.append(
                Problem(
                    "error",
                    "manifest.shards",
                    f"分片 token 之和 {shard_tokens} ≠ 交付 token {tokens}（差 {tokens - shard_tokens:+d}）",
                )
            )
    elif manifest.total.delivered_records:
        problems.append(Problem("warn", "manifest.shards", "manifest 未记录分片清单，无法校验分片与总量一致"))

    # ---- 停止口径 ----
    if manifest.stop_reason:
        if manifest.stop_reason not in STOP_REASONS:
            problems.append(
                Problem(
                    "error",
                    "manifest.stop_reason",
                    f"未知停止原因 {manifest.stop_reason!r}，只允许 {', '.join(STOP_REASONS)}",
                )
            )
        elif manifest.stop_reason == "token_target_reached":
            target = manifest.token_target
            delivered_tokens = manifest.total.delivered_tokens or sum(
                f.delivered_tokens for f in manifest.files
            )
            if not target:
                problems.append(Problem("warn", "manifest.token_target", "到达 token 目标但没写目标值"))
            elif delivered_tokens < target:
                problems.append(
                    Problem(
                        "error",
                        "manifest.token_target",
                        f"声称到达目标但交付只有 {delivered_tokens} < 目标 {target}",
                    )
                )
            stopped = [f for f in manifest.files if f.status in ("stopped", "stop", "partial", "done_partial")]
            if len(stopped) > 1:
                problems.append(
                    Problem(
                        "error",
                        "manifest.files",
                        f"{len(stopped)} 个文件被标为中途停止；到达目标后应立刻停止拉取，至多一个文件收尾",
                    )
                )
            elif stopped and stopped[-1] is not manifest.files[-1]:
                problems.append(
                    Problem("error", "manifest.files", "中途停止的文件不是最后一个，停止点不可信")
                )
    else:
        problems.append(Problem("warn", "manifest.stop_reason", "manifest 缺少 stop_reason"))

    return problems
