#!/usr/bin/env python3
"""验收侧：manifest 归一化 + 逐文件对账。

对账恒等式（锁定口径）：

    原始条数 = 各过滤步骤 + 两步去重 + 交付条数 + 停止后未交付条数

逐文件、逐阶段一一对上，并且各文件之和要等于 manifest 的总量。

已知两种交付方格式（验收侧都要能读）：

1. 本仓库参考格式：``files[]`` 逐文件内嵌 ``removed{}``；
2. 开发侧 ``corpus_pipeline`` 实际格式：manifest 只有聚合 ``ledger``，
   逐文件明细在同目录的 ``ledger.json`` 的 ``files{}`` 里，字段是扁平的
   （``parse_error`` / ``no_rs_id`` / ``session_removed`` / ``passed`` …）。

字段名两边各写各的，所以这里做归一，且**默认 fail-closed**：出现认不出的
计数项直接报错，而不是悄悄漏算（漏算比算错更危险）。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

MANIFEST_SCHEMA = "buckethub.dataset.manifest/v1"

#: 两侧实现都必须记账的核心阶段；缺一步说明有阶段的量没记录。
CORE_STEPS = (
    "parse_errors",
    "filter_rs_id",
    "filter_encrypted_content",
    "filter_min_messages",
    "dedupe_session",
    "dedupe_prefix",
)

#: 可选阶段：有的实现在过滤阶段额外丢掉没有 session_id 的记录；停止后未交付部分
#: 也必须单独记账（``stop_cut``），否则对账少一截。
OPTIONAL_STEPS = ("filter_no_session_id", "stop_cut")

CANONICAL_STEPS = CORE_STEPS + OPTIONAL_STEPS

STOP_REASONS = ("token_target_reached", "source_exhausted")
STOP_REASON_ALIASES = {
    "corpusexhausted": "source_exhausted",
    "sourceexhausted": "source_exhausted",
    "dataexhausted": "source_exhausted",
    "tokentargetreached": "token_target_reached",
    "targetreached": "token_target_reached",
}

#: 这些字段是汇总值/元数据，不是"某一步移除了多少条"，归类时必须跳过。
_NON_STEP_FIELDS = {
    "scanned",
    "rawlines",
    "inputlines",
    "totallines",
    "lines",
    "passed",
    "passedrecords",
    "kept",
    "keptrecords",
    "keepcount",
    "delivered",
    "deliveredrecords",
    "deliveredsamples",
    "deliveredwindows",
    "deliveredtokens",
    "windowpieces",
    "extrapieces",
    "splitpieces",
    "undelivered",
    "notdelivered",
    "tokens",
    "tokencount",
    "bytes",
    "samples",
    "status",
    "state",
    "result",
    "exactdup",
    "filterclosureok",
    "dedupclosureok",
}

_SOURCE_KEY_ALIASES = ("source_key", "key", "name", "file", "filename", "source", "object_key")
_RAW_LINES_ALIASES = ("raw_lines", "scanned", "raw", "input_lines", "total_lines", "lines", "line_count")
_REMOVED_ALIASES = (
    "removed",
    "removals",
    "removed_counts",
    "dropped",
    "drops",
    "removed_by_step",
    "rejected",
    "removed_breakdown",
)
_DELIVERED_ALIASES = ("delivered_records", "delivered", "output_records")
_KEPT_ALIASES = ("kept_records", "kept", "keep_count", "kept_total")
_PASSED_ALIASES = ("passed", "passed_records", "survivors", "kept_after_filter")
_DELIVERED_TOKENS_ALIASES = ("delivered_tokens", "tokens", "token_count", "kept_tokens")
_NOT_DELIVERED_ALIASES = ("not_delivered", "undelivered", "stop_cut", "dropped_at_stop", "left_over")
_DELIVERED_SAMPLES_ALIASES = ("delivered_samples", "samples", "delivered_lines", "lines_written", "delivered_windows")
_WINDOW_PIECES_ALIASES = ("window_pieces", "window_extra", "extra_pieces", "split_pieces")
_STATUS_ALIASES = ("status", "state", "result")

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
    delivered_samples: int = 0
    window_pieces: int = 0
    not_delivered: int = 0
    kept_records: int | None = None
    passed: int | None = None
    status: str = ""
    unknown_steps: dict[str, int] = field(default_factory=dict)

    @property
    def removed_total(self) -> int:
        return sum(self.removed.values())

    @property
    def filter_total(self) -> int:
        return sum(
            self.removed.get(step, 0)
            for step in ("parse_errors", "filter_no_session_id", "filter_rs_id",
                         "filter_encrypted_content", "filter_min_messages")
        )

    @property
    def dedupe_total(self) -> int:
        return self.removed.get("dedupe_session", 0) + self.removed.get("dedupe_prefix", 0)

    @property
    def accounted(self) -> int:
        return self.removed_total + self.delivered_records + self.not_delivered

    @property
    def balanced(self) -> bool:
        return self.raw_lines == self.accounted

    def as_dict(self) -> dict[str, Any]:
        out = {
            "source_key": self.source_key,
            "raw_lines": self.raw_lines,
            "removed": dict(sorted(self.removed.items())),
            "removed_total": self.removed_total,
            "delivered_records": self.delivered_records,
            "delivered_tokens": self.delivered_tokens,
            "delivered_samples": self.delivered_samples,
            "window_pieces": self.window_pieces,
            "not_delivered": self.not_delivered,
            "accounted": self.accounted,
            "balanced": self.balanced,
            "status": self.status,
        }
        if self.kept_records is not None:
            out["kept_records"] = self.kept_records
        if self.passed is not None:
            out["passed"] = self.passed
        if self.unknown_steps:
            out["unknown_steps"] = self.unknown_steps
        return out


@dataclass
class ShardLedger:
    name: str
    records: int = 0
    bytes: int = 0
    tokens: int = 0
    #: 按源文件命名时该分片属于哪个源对象（旧格式为空）
    source_key: str = ""
    part: int = 0

    def as_dict(self) -> dict[str, Any]:
        out = {"name": self.name, "records": self.records, "bytes": self.bytes, "tokens": self.tokens}
        if self.source_key:
            out["source_key"] = self.source_key
        if self.part:
            out["part"] = self.part
        return out


@dataclass
class Manifest:
    schema: str = ""
    stop_reason: str = ""
    token_target: int = 0
    tokenizer: dict[str, Any] = field(default_factory=dict)
    total: FileLedger = field(default_factory=lambda: FileLedger(source_key="<totals>"))
    files: list[FileLedger] = field(default_factory=list)
    shards: list[ShardLedger] = field(default_factory=list)
    files_source: str = ""
    #: 1B 到顶时停在的那条记录（开发侧新增字段）
    stop_after_record: dict[str, Any] | None = None
    #: 1B 到顶时停在哪一个源文件的边界上（按源文件命名的交付才有）
    stop_file: str = ""
    #: 逐源文件的交付状态（status / parts / records / tokens）
    delivered_files: list[dict[str, Any]] = field(default_factory=list)
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
            "files_source": self.files_source,
            "stop_after_record": self.stop_after_record,
            "stop_file": self.stop_file,
            "delivered_files": self.delivered_files,
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
    """把任意命名的移除步骤归到规范名字；认不出返回 None。"""
    key = _norm_key(name)
    if not key or key in _NON_STEP_FIELDS or key.startswith("reason"):
        return None
    if "nosession" in key or ("session" in key and ("no" in key or "missing" in key or "empty" in key)):
        return "filter_no_session_id"
    if "parse" in key or "invalid" in key or "badline" in key:
        return "parse_errors"
    if "prefix" in key:
        return "dedupe_prefix"
    if "session" in key:
        return "dedupe_session"
    if "encrypt" in key:
        return "filter_encrypted_content"
    if "stop" in key or "cut" in key:
        return "stop_cut"
    if "rsid" in key or "reasonid" in key:
        return "filter_rs_id"
    if "message" in key or "round" in key or "turn" in key or "minmsg" in key or "tooshort" in key:
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
    """把任意形态的计数字典归成规范移除量；认不出的非零项单独返回。"""
    canonical: dict[str, int] = {}
    unknown: dict[str, int] = {}
    for name, value in mapping.items():
        if not isinstance(value, (int, float)):
            continue
        step = classify_step(name)
        if step is None:
            if _norm_key(name) in _NON_STEP_FIELDS or _norm_key(name).startswith("reason"):
                continue
            if int(value):
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
    removed_raw = _pick(flat, _REMOVED_ALIASES, None)
    if not isinstance(removed_raw, dict):
        removed_raw = {}
    removed, unknown = normalize_removed({**flat, **removed_raw})

    delivered = _as_int(_pick(flat, _DELIVERED_ALIASES, 0))
    kept_value = _pick(flat, _KEPT_ALIASES, None)
    kept = _as_int(kept_value, 0) if kept_value is not None else None
    passed_value = _pick(flat, _PASSED_ALIASES, None)
    passed = _as_int(passed_value, 0) if passed_value is not None else None

    not_delivered_value = _pick(flat, _NOT_DELIVERED_ALIASES, None)
    if not_delivered_value is not None:
        not_delivered = _as_int(not_delivered_value, 0)
    elif kept is not None:
        not_delivered = max(0, kept - delivered)
    else:
        not_delivered = 0

    return FileLedger(
        source_key=str(_pick(flat, _SOURCE_KEY_ALIASES, "<unknown>")),
        raw_lines=_as_int(_pick(flat, _RAW_LINES_ALIASES, 0)),
        removed=removed,
        delivered_records=delivered,
        delivered_tokens=_as_int(_pick(flat, _DELIVERED_TOKENS_ALIASES, 0)),
        delivered_samples=_as_int(_pick(flat, _DELIVERED_SAMPLES_ALIASES, 0)),
        window_pieces=_as_int(_pick(flat, _WINDOW_PIECES_ALIASES, 0)),
        not_delivered=not_delivered,
        kept_records=kept,
        passed=passed,
        status=str(_pick(flat, _STATUS_ALIASES, "") or ""),
        unknown_steps=unknown,
    )


def _shard_ledger(entry: Any) -> ShardLedger:
    if isinstance(entry, str):
        return ShardLedger(name=entry)
    if not isinstance(entry, dict):
        return ShardLedger(name=str(entry))
    flat = _collect_counts(entry)
    return ShardLedger(
        name=str(_pick(flat, ("name", "shard", "file", "path", "key"), "<unknown>")),
        records=_as_int(_pick(flat, ("records", "samples", "count", "lines", "delivered_records"), 0)),
        bytes=_as_int(_pick(flat, ("bytes", "size", "byte_size"), 0)),
        tokens=_as_int(_pick(flat, ("tokens", "token_count"), 0)),
        source_key=str(_pick(flat, ("source_key", "source", "input_key"), "") or ""),
        part=_as_int(_pick(flat, ("part", "part_index", "shard_part"), 0)),
    )


def _normalize_stop_reason(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    key = _norm_key(text)
    if key in STOP_REASON_ALIASES:
        return STOP_REASON_ALIASES[key]
    return text


def _tokenizer_meta(raw: dict[str, Any]) -> dict[str, Any]:
    meta = _pick(raw, ("tokenizer", "tokenization", "tokenizer_info"), None)
    if isinstance(meta, dict) and meta:
        return meta
    config = raw.get("config") if isinstance(raw.get("config"), dict) else {}
    encoding = _pick(config, ("tokenizer", "encoding"), None)
    if encoding:
        # 开发侧只写了编码器名字；方法按锁定口径推断，交由 reconcile 提示。
        return {"encoding": str(encoding), "source": "config.tokenizer"}
    return {}


def _totals_from_flat(raw: dict[str, Any]) -> dict[str, Any]:
    """开发侧 manifest 没有 totals{}，汇总值散在顶层 + ledger 聚合里。"""
    merged: dict[str, Any] = {}
    aggregate = _pick(raw, ("ledger", "aggregate", "summary"), {})
    if isinstance(aggregate, dict):
        merged.update(aggregate)
    for key in (
        "delivered_records",
        "delivered_tokens",
        "delivered_samples",
        "kept_records",
        "not_delivered",
        "window_pieces",
        "target_tokens",
    ):
        for alias in (key,):
            value = _pick(raw, (alias,), None)
            if value is not None:
                merged[key] = value
    return merged


def normalize_manifest(raw: dict[str, Any]) -> Manifest:
    if not isinstance(raw, dict):
        raise ValueError("manifest 顶层必须是 JSON 对象")

    totals_raw = _pick(raw, ("totals", "total", "grand_total"), None)
    if not isinstance(totals_raw, dict):
        totals_raw = _totals_from_flat(raw)
    total = _file_ledger({**totals_raw, "source_key": "<totals>"})

    files_raw = _pick(raw, ("files", "per_file", "sources", "inputs"), []) or []
    if isinstance(files_raw, dict):
        files_raw = [
            {"source_key": k, **(v if isinstance(v, dict) else {})} for k, v in files_raw.items()
        ]
    files = [_file_ledger(entry) for entry in files_raw if isinstance(entry, dict)]

    shards_raw = _pick(raw, ("shards", "parts", "packages", "outputs"), []) or []
    if not isinstance(shards_raw, list):
        shards_raw = []
    shards = [_shard_ledger(entry) for entry in shards_raw]

    stop_record = _pick(raw, ("stop_after_record", "stop_record", "stopped_at_record"), None)
    delivered_files = _pick(raw, ("delivered_files", "files_delivered"), []) or []
    if not isinstance(delivered_files, list):
        delivered_files = []
    return Manifest(
        schema=str(_pick(raw, ("schema", "manifest_schema", "version"), "") or ""),
        stop_reason=_normalize_stop_reason(_pick(raw, ("stop_reason", "STOP_REASON", "stop"), "")),
        token_target=_as_int(_pick(raw, ("token_target", "target_tokens", "token_goal"), 0)),
        tokenizer=_tokenizer_meta(raw),
        total=total,
        files=files,
        shards=shards,
        stop_after_record=stop_record if isinstance(stop_record, dict) else None,
        stop_file=str(_pick(raw, ("stop_file", "stopped_at_file", "stop_source"), "") or ""),
        delivered_files=[f for f in delivered_files if isinstance(f, dict)],
        raw=raw,
    )


def load_manifest(path: str | Path) -> Manifest:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    manifest = normalize_manifest(raw)
    if not manifest.files:
        sidecar = path.with_name("ledger.json")
        if sidecar.exists():
            with open(sidecar, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            files_map = payload.get("files") if isinstance(payload, dict) else None
            if isinstance(files_map, dict):
                manifest.files = [
                    _file_ledger({"source_key": key, **(value if isinstance(value, dict) else {})})
                    for key, value in files_map.items()
                ]
                manifest.files_source = str(sidecar)
    return manifest


# --------------------------------------------------------------------------
# 对账
# --------------------------------------------------------------------------


def reconcile(
    manifest: Manifest,
    *,
    required_steps: Iterable[str] = CORE_STEPS,
    allow_missing_steps: bool = False,
    strict_totals: bool = True,
) -> list[Problem]:
    problems: list[Problem] = []
    required = tuple(required_steps)

    if not manifest.files:
        problems.append(
            Problem(
                "error",
                "manifest.files",
                "没有任何逐文件记录（manifest.files 与同目录 ledger.json 都没有），无法逐文件对账",
            )
        )

    imbalances: list[tuple[FileLedger, str, int]] = []
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
            imbalances.append((ledger, where, ledger.raw_lines - ledger.accounted))
            kept = ledger.raw_lines - ledger.filter_total - ledger.dedupe_total
            if ledger.delivered_records == 0 and kept > 0:
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"该文件去重后应保留 {kept} 条，但交付条数记的是 0——交付计数没有回写账本",
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
        # 分阶段闭合：过滤段与去重段都各有自己的恒等式
        if ledger.passed is not None and ledger.raw_lines:
            expected_passed = ledger.raw_lines - ledger.filter_total
            if ledger.passed != expected_passed:
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"过滤段不闭合: 扫描 {ledger.raw_lines} − 过滤 {ledger.filter_total} "
                        f"= {expected_passed}，但账本记 passed={ledger.passed}",
                    )
                )
        if ledger.kept_records is not None and ledger.passed is not None:
            expected_kept = ledger.passed - ledger.dedupe_total
            if ledger.kept_records != expected_kept:
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"去重段不闭合: passed {ledger.passed} − 去重 {ledger.dedupe_total} "
                        f"= {expected_kept}，但账本记本保留 {ledger.kept_records}",
                    )
                )

    # ---- 逐文件不闭合：若差额正好等于总量里的"停止后未交付"，降级为 warn ----
    total = manifest.total
    if imbalances:
        diffs = [diff for _, _, diff in imbalances]
        if (
            total.not_delivered
            and all(diff > 0 for diff in diffs)
            and sum(diffs) == total.not_delivered
        ):
            problems.append(
                Problem(
                    "warn",
                    "manifest.files",
                    f"停止后未交付的 {total.not_delivered} 条只记在总量里，逐文件无法归属；"
                    f"各文件差额（{diffs}）合计与总量一致",
                )
            )
        else:
            for ledger, where, diff in imbalances:
                problems.append(
                    Problem(
                        "error",
                        where,
                        "对账不闭合: 原始 {raw} ≠ 移除 {removed} + 交付 {delivered} + 停止后未交付 {left}（差 {diff:+d}）".format(
                            raw=ledger.raw_lines,
                            removed=ledger.removed_total,
                            delivered=ledger.delivered_records,
                            left=ledger.not_delivered,
                            diff=diff,
                        ),
                    )
                )

    # ---- 总量 vs 逐文件 ----
    if manifest.files:
        sum_raw = sum(f.raw_lines for f in manifest.files)
        sum_delivered = sum(f.delivered_records for f in manifest.files)
        sum_tokens = sum(f.delivered_tokens for f in manifest.files)
        sum_removed = sum(f.removed_total for f in manifest.files)
        sum_left = sum(f.not_delivered for f in manifest.files)

        if total.raw_lines and total.raw_lines != sum_raw:
            problems.append(
                Problem("error", "manifest.totals", f"总原始条数 {total.raw_lines} ≠ 逐文件之和 {sum_raw}")
            )
        if total.delivered_records and total.delivered_records != sum_delivered:
            problems.append(
                Problem(
                    "error",
                    "manifest.totals",
                    f"总交付条数 {total.delivered_records} ≠ 逐文件之和 {sum_delivered}"
                    "（交付计数只汇总在顶层、没有逐文件回写）",
                )
            )
        files_have_tokens = any(f.delivered_tokens for f in manifest.files)
        if (
            total.delivered_tokens
            and files_have_tokens
            and strict_totals
            and total.delivered_tokens != sum_tokens
        ):
            problems.append(
                Problem(
                    "error",
                    "manifest.totals",
                    f"总交付 token {total.delivered_tokens} ≠ 逐文件之和 {sum_tokens}"
                    f"（差 {total.delivered_tokens - sum_tokens:+d}）",
                )
            )
        gap_explained = total.not_delivered and sum_left == 0 and sum(
            diff for _, _, diff in imbalances
        ) == total.not_delivered
        if total.not_delivered and total.not_delivered != sum_left and not gap_explained:
            problems.append(
                Problem(
                    "error",
                    "manifest.totals",
                    f"总未交付数 {total.not_delivered} ≠ 逐文件之和 {sum_left}",
                )
            )
        if total.delivered_tokens and not files_have_tokens:
            problems.append(
                Problem("warn", "manifest.totals", "逐文件没有 token 记账，token 只能按总量与分片核对")
            )
        if not total.raw_lines and not total.delivered_records:
            problems.append(Problem("warn", "manifest.totals", "缺少总量记录，只能逐文件对账"))

        # 总量自身也要闭合
        if total.raw_lines:
            if total.removed_total and total.removed_total != sum_removed:
                problems.append(
                    Problem(
                        "error",
                        "manifest.totals",
                        f"总移除量 {total.removed_total} ≠ 逐文件之和 {sum_removed}",
                    )
                )
            if total.raw_lines != total.accounted:
                problems.append(
                    Problem(
                        "error",
                        "manifest.totals",
                        "总量对账不闭合: 原始 {raw} ≠ 移除 {removed} + 交付 {delivered} + 停止后未交付 {left}（差 {diff:+d}）".format(
                            raw=total.raw_lines,
                            removed=total.removed_total,
                            delivered=total.delivered_records,
                            left=total.not_delivered,
                            diff=total.raw_lines - total.accounted,
                        ),
                    )
                )

    # ---- 分片 vs 总量 ----
    if manifest.shards:
        shard_lines = sum(s.records for s in manifest.shards)
        shard_tokens = sum(s.tokens for s in manifest.shards)
        delivered = total.delivered_records or sum(f.delivered_records for f in manifest.files)
        windows = total.window_pieces or sum(f.window_pieces for f in manifest.files)
        samples = total.delivered_samples or sum(f.delivered_samples for f in manifest.files)
        tokens = total.delivered_tokens or sum(f.delivered_tokens for f in manifest.files)
        expected_lines = samples or (delivered + windows)
        if shard_lines and expected_lines and shard_lines != expected_lines:
            problems.append(
                Problem(
                    "error",
                    "manifest.shards",
                    f"分片行数之和 {shard_lines} ≠ 交付行数 {expected_lines}"
                    + ("" if samples else f"（交付源记录 {delivered} + 切窗片段 {windows}）"),
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
    elif total.delivered_records:
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
            delivered_tokens = total.delivered_tokens or sum(f.delivered_tokens for f in manifest.files)
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
            stopped = [
                f for f in manifest.files if f.status in ("stopped", "stop", "partial", "done_partial")
            ]
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
