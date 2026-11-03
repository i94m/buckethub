#!/usr/bin/env python3
"""联调工具：把开发侧真实交付与验收侧独立计算结果摆在一起逐项对比。

用途只有一个——回答"两边到底吻合不吻合"：

* 逐文件对比：扫描数、各过滤原因、两步去重移除量、保留数（开发侧账本 vs 我独立重算）；
* 对比交付集：开发侧分片里的记录内容（切窗的按 `_window` 合并）vs 我独立算出的保留集；
* 对比 token：交付 token 复算 vs manifest 声明。

只读本地目录，不联网、不写任何交付物（除 `--report` 指定的 JSON）。

    python3 -m accept.crosscheck --raw-dir ./itest/raw --delivery ./itest/out
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import dedup, ledger, tokens
from .shard import ShardConfig, discover_shards, window_info

FILTER_LABELS = {
    "parse_errors": "解析失败",
    "filter_no_session_id": "缺 session_id",
    "filter_rs_id": "缺 rs_id",
    "filter_encrypted_content": "缺密文 part",
    "filter_min_messages": "不足 100 条",
}


@dataclass
class SourceRecord:
    key: str
    offset: int
    length: int


@dataclass
class FileStat:
    scanned: int = 0
    parse_errors: int = 0
    filters: dict[str, int] = field(default_factory=dict)
    session_removed: int = 0
    prefix_removed: int = 0

    @property
    def kept(self) -> int:
        return (
            self.scanned
            - self.parse_errors
            - sum(self.filters.values())
            - self.session_removed
            - self.prefix_removed
        )


def _sequence_digest(session_id: str, messages: Sequence[Any]) -> str:
    return f"{dedup.session_group_key(session_id)}:{dedup.sequence_bytes(messages).hex()}"


def scan_raw(raw_dir: Path, min_messages: int = 100) -> tuple[dict[str, FileStat], list[dict[str, Any]], list[dict[str, Any]]]:
    """独立重跑过滤 + 两步去重，返回 (逐文件统计, 过滤后幸存, 最终保留)。"""
    stats: dict[str, FileStat] = {}
    survivors: list[dict[str, Any]] = []
    for path in sorted(raw_dir.glob("*.jsonl")):
        stat = stats.setdefault(path.name, FileStat())
        offset = 0
        with open(path, "rb") as handle:
            for raw in handle:
                stat.scanned += 1
                here = offset
                offset += len(raw)
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                except Exception:
                    stat.parse_errors += 1
                    continue
                messages = dedup.get_messages(record)
                reason = _filter_reason(record, messages, min_messages)
                if reason is not None:
                    stat.filters[reason] = stat.filters.get(reason, 0) + 1
                    continue
                survivors.append(
                    {
                        "key": path.name,
                        "off": here,
                        "session_id": dedup.get_session_id(record),
                        "messages": messages,
                        "digest": _sequence_digest(dedup.get_session_id(record), messages),
                    }
                )

    # dedup 模块吃的是"记录"（内部按 request.messages 取内容），这里包一层。
    wrapped = [
        {"session_id": item["session_id"], "request": {"messages": item["messages"]}}
        for item in survivors
    ]
    session_result = dedup.dedupe_session(wrapped)
    for index in session_result.removed:
        stats[survivors[index]["key"]].session_removed += 1
    after_session = [survivors[index] for index in session_result.kept]
    after_session_wrapped = [wrapped[index] for index in session_result.kept]

    prefix_result = dedup.dedupe_prefix(after_session_wrapped)
    for index in prefix_result.removed:
        stats[after_session[index]["key"]].prefix_removed += 1
    kept = [after_session[index] for index in prefix_result.kept]

    for item in survivors:
        item.pop("messages", None)
    kept_summary = [{k: v for k, v in item.items() if k != "messages"} for item in kept]
    return stats, survivors, kept_summary


def _filter_reason(record: dict, messages: Sequence[Any], min_messages: int) -> str | None:
    if not isinstance(record, dict):
        return "parse_errors"
    if not dedup.session_group_key(dedup.get_session_id(record)):
        return "filter_no_session_id"
    from .shard import has_encrypted_part, rs_ids

    if not rs_ids(record):
        return "filter_rs_id"
    if not has_encrypted_part(record):
        return "filter_encrypted_content"
    if len(messages) < min_messages:
        return "filter_min_messages"
    return None


def read_delivery(delivery_dir: Path, cfg: ShardConfig | None = None) -> dict[str, Any]:
    """读出交付内容：非切窗记录直接取 messages，切窗记录按 source 合并。"""
    cfg = cfg or ShardConfig()
    cfg.max_record_tokens = 10**9
    groups: dict[str, dict[str, Any]] = {}
    plain: list[dict[str, Any]] = []
    lines = 0
    for path in discover_shards(delivery_dir, cfg):
        with open(path, "rb") as handle:
            for lineno, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                lines += 1
                record = json.loads(raw)
                window = window_info(record, cfg.window_marker_keys)
                if window is None:
                    messages = dedup.get_messages(record)
                    plain.append(
                        {
                            "where": f"{path.name}:{lineno}",
                            "session_id": dedup.get_session_id(record),
                            "messages": messages,
                            "digest": _sequence_digest(dedup.get_session_id(record), messages),
                        }
                    )
                    continue
                source = window.get("source") or {}
                fingerprint = str(window.get("fingerprint") or f"{source.get('key')}#{source.get('off')}")
                bucket = groups.setdefault(
                    fingerprint,
                    {
                        "windows": {},
                        "count": window.get("count"),
                        "messages_total": window.get("messages_total"),
                        "session_id": dedup.get_session_id(record),
                    },
                )
                index = window.get("index")
                content = record.get("messages") if isinstance(record.get("messages"), list) else dedup.get_messages(record)
                bucket["windows"][index] = content

    merged: list[dict[str, Any]] = list(plain)
    for fingerprint, bucket in groups.items():
        indices = sorted(bucket["windows"])
        messages: list[Any] = []
        for index in indices:
            messages.extend(bucket["windows"][index])
        merged.append(
            {
                "where": f"window-group {fingerprint}",
                "session_id": bucket["session_id"],
                "messages": messages,
                "digest": _sequence_digest(bucket["session_id"], messages),
                "windows": len(indices),
            }
        )
    return {"lines": lines, "records": merged, "window_groups": groups}


def compare(raw_dir: Path, delivery_dir: Path, *, min_messages: int = 100) -> dict[str, Any]:
    my_stats, _, my_kept = scan_raw(raw_dir, min_messages=min_messages)
    delivery = read_delivery(delivery_dir)
    manifest = ledger.load_manifest(delivery_dir / "manifest.json")

    # ---- 逐文件：开发侧账本 vs 独立重算 ----
    per_file: list[dict[str, Any]] = []
    for file_ledger in manifest.files:
        mine = my_stats.get(file_ledger.source_key)
        if mine is None:
            per_file.append({"file": file_ledger.source_key, "missing_on_my_side": True})
            continue
        theirs_filters = {
            step: file_ledger.removed.get(step, 0)
            for step in FILTER_LABELS
            if file_ledger.removed.get(step, 0) or step in mine.filters
        }
        per_file.append(
            {
                "file": file_ledger.source_key,
                "scanned": [file_ledger.raw_lines, mine.scanned],
                "parse_errors": [file_ledger.removed.get("parse_errors", 0), mine.parse_errors],
                "filters": {
                    step: [theirs_filters.get(step, 0), mine.filters.get(step, 0)] for step in theirs_filters
                },
                "session_removed": [file_ledger.removed.get("dedupe_session", 0), mine.session_removed],
                "prefix_removed": [file_ledger.removed.get("dedupe_prefix", 0), mine.prefix_removed],
                "kept": [
                    file_ledger.raw_lines - file_ledger.removed_total,
                    mine.kept,
                ],
            }
        )

    # ---- 交付集 vs 独立保留集（按内容摘要） ----
    mine_digests = [item["digest"] for item in my_kept]
    theirs_digests = [item["digest"] for item in delivery["records"]]
    missing = sorted(set(mine_digests) - set(theirs_digests))
    extra = sorted(set(theirs_digests) - set(mine_digests))

    # ---- token 复算 ----
    recounted = sum(tokens.count_messages(item["messages"]) for item in delivery["records"])
    manifest_tokens = manifest.total.delivered_tokens

    mismatched_files = [
        item["file"]
        for item in per_file
        if any(
            pair[0] != pair[1]
            for pair in [
                item.get("scanned", [0, 0]),
                item.get("parse_errors", [0, 0]),
                item.get("session_removed", [0, 0]),
                item.get("prefix_removed", [0, 0]),
            ]
        )
        or any(pair[0] != pair[1] for pair in item.get("filters", {}).values())
    ]

    return {
        "raw_dir": str(raw_dir),
        "delivery_dir": str(delivery_dir),
        "per_file": per_file,
        "kept": {
            "mine": len(mine_digests),
            "theirs": len(theirs_digests),
            "missing_in_delivery": len(missing),
            "extra_in_delivery": len(extra),
            "digests_match": not missing and not extra and len(mine_digests) == len(theirs_digests),
        },
        "tokens": {
            "manifest": manifest_tokens,
            "recount": recounted,
            "delta": recounted - manifest_tokens,
            "match": recounted == manifest_tokens,
        },
        "delivery_lines": delivery["lines"],
        "window_groups": len(delivery["window_groups"]),
        "per_file_mismatches": mismatched_files,
        "files_source": manifest.files_source,
    }


def _print(report: dict[str, Any]) -> None:
    print(f"原始目录: {report['raw_dir']}")
    print(f"交付目录: {report['delivery_dir']}")
    print(f"逐文件账本来源: {report['files_source'] or 'manifest.files'}")
    print()
    print("逐文件对比（开发侧 / 独立重算）：")
    for item in report["per_file"]:
        if item.get("missing_on_my_side"):
            print(f"  {item['file']}: 原始目录里找不到这个文件")
            continue
        flags = []
        if item["scanned"][0] != item["scanned"][1]:
            flags.append(f"扫描 {item['scanned']}")
        if item["parse_errors"][0] != item["parse_errors"][1]:
            flags.append(f"解析 {item['parse_errors']}")
        for step, pair in item["filters"].items():
            if pair[0] != pair[1]:
                flags.append(f"{FILTER_LABELS.get(step, step)} {pair}")
        if item["session_removed"][0] != item["session_removed"][1]:
            flags.append(f"session 移除 {item['session_removed']}")
        if item["prefix_removed"][0] != item["prefix_removed"][1]:
            flags.append(f"前缀移除 {item['prefix_removed']}")
        if not flags:
            print(f"  {item['file']}: 一致（扫描 {item['scanned'][0]}，保留 {item['kept'][0]}）")
        else:
            print(f"  {item['file']}: 不一致 → " + "；".join(flags))
    print()
    kept = report["kept"]
    status = "一致" if kept["digests_match"] else "不一致"
    print(
        f"交付集 vs 独立保留集（按内容摘要）: {status}"
        f"（我 {kept['mine']} 条 / 交付 {kept['theirs']} 条；"
        f"交付缺少 {kept['missing_in_delivery']} 条，多出 {kept['extra_in_delivery']} 条）"
    )
    tok = report["tokens"]
    print(
        f"token 复算: manifest {tok['manifest']} / 复算 {tok['recount']}"
        f" → 误差 {tok['delta']:+d}（{'0' if tok['match'] else '不为 0'}）"
    )
    print(f"交付 {report['delivery_lines']} 行，其中切窗组 {report['window_groups']} 个")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="联调：开发侧交付 vs 验收侧独立计算")
    parser.add_argument("--raw-dir", required=True, help="原始 jsonl 所在目录（本地合成数据）")
    parser.add_argument("--delivery", required=True, help="开发侧产出目录（含 manifest.json）")
    parser.add_argument("--min-messages", type=int, default=100)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--report", default=None, help="把对比结果写成 JSON")
    args = parser.parse_args(argv)

    report = compare(Path(args.raw_dir), Path(args.delivery), min_messages=args.min_messages)
    if args.report:
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        _print(report)
    ok = (
        report["kept"]["digests_match"]
        and report["tokens"]["match"]
        and not report["per_file_mismatches"]
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
