#!/usr/bin/env python3
"""验收侧的"参考流水线"（合成数据专用，绝不联网）。

它不是交付流水线，只是为了让验收脚本有个已知正确的对照物：
用 ``accept.dedup`` + ``accept.tokens`` 把合成语料跑一遍，产出一份符合
manifest 契约的交付目录，然后让 ``accept.cli verify`` 去验它——验不过
说明验收脚本自己有问题。

跑真实数据的是开发侧的流水线，这里一行网络代码都没有。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import dedup, ledger, tokens
from .shard import MIB, has_encrypted_part, rs_ids


@dataclass
class ReferenceConfig:
    min_messages: int = 100
    max_record_tokens: int = 128_000
    shard_max_bytes: int = 500 * MIB
    token_target: int = 0
    shard_name_format: str = "deliver-{index:04d}.jsonl"


@dataclass
class _Origin:
    source_key: str
    line_no: int


@dataclass
class _Work:
    record: dict[str, Any]
    origin: _Origin
    tokens: int = 0


@dataclass
class _FileLedger:
    source_key: str
    raw_lines: int = 0
    removed: dict[str, int] = field(default_factory=lambda: {step: 0 for step in ledger.CANONICAL_STEPS})
    delivered_records: int = 0
    delivered_tokens: int = 0
    window_pieces: int = 0
    status: str = "done"


def _read_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    parse_errors = 0
    with open(path, "rb") as handle:
        for raw in handle:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except Exception:
                parse_errors += 1
                continue
            if not isinstance(record, dict):
                parse_errors += 1
                continue
            records.append(record)
    return records, parse_errors


def _split_windows(
    record: dict[str, Any],
    messages: Sequence[Any],
    *,
    counter: Any,
    max_tokens: int,
) -> list[dict[str, Any]]:
    """把超长记录按窗口切开：窗口之间**不重叠**（否则会自造前缀关系）。"""
    pieces: list[list[Any]] = []
    current: list[Any] = []
    current_tokens = 0
    for message in messages:
        size = counter.count_messages([message])
        if current and current_tokens + size > max_tokens:
            pieces.append(current)
            current, current_tokens = [], 0
        current.append(message)
        current_tokens += size
    if current:
        pieces.append(current)
    if len(pieces) <= 1:
        return [record]

    out: list[dict[str, Any]] = []
    fingerprint = hashlib.blake2b(
        json.dumps(messages, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        digest_size=8,
    ).hexdigest()
    for index, chunk in enumerate(pieces):
        clone = json.loads(json.dumps(record))
        clone["request"]["messages"] = chunk
        clone["window"] = {
            "index": index,
            "count": len(pieces),
            "source_fingerprint": fingerprint,
        }
        out.append(clone)
    return out


def run_reference(
    source_files: dict[str, Path] | Iterable[Path],
    out_dir: str | Path,
    *,
    cfg: ReferenceConfig | None = None,
    counter: Any | None = None,
    tokenizer_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """跑参考流水线，写 ``out_dir`` 下的分片 + ``manifest.json``，返回 manifest。"""
    cfg = cfg or ReferenceConfig()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(source_files, dict):
        pairs = list(source_files.items())
    else:
        pairs = [(str(Path(path)), Path(path)) for path in source_files]

    if counter is None:
        counter = _TiktokenCounter()

    ledgers: dict[str, _FileLedger] = {}
    survivors: list[_Work] = []

    # ---- L1 过滤 ----
    for source_key, path in pairs:
        records, parse_errors = _read_jsonl(Path(path))
        file_ledger = _FileLedger(source_key=source_key, raw_lines=len(records) + parse_errors)
        file_ledger.removed["parse_errors"] = parse_errors
        for line_no, record in enumerate(records, start=1):
            if not rs_ids(record):
                file_ledger.removed["filter_rs_id"] += 1
                continue
            if not has_encrypted_part(record):
                file_ledger.removed["filter_encrypted_content"] += 1
                continue
            if len(dedup.get_messages(record)) < cfg.min_messages:
                file_ledger.removed["filter_min_messages"] += 1
                continue
            survivors.append(_Work(record=record, origin=_Origin(source_key, line_no)))
        ledgers[source_key] = file_ledger

    order = {key: position for position, key in enumerate(ledgers)}

    def _file_of(work: _Work) -> _FileLedger:
        return ledgers[work.origin.source_key]

    def _sort_key(work: _Work) -> tuple[int, int]:
        return (order[work.origin.source_key], work.origin.line_no)

    survivors.sort(key=_sort_key)

    # ---- L2a session 去重 ----
    records_only = [work.record for work in survivors]
    session_result = dedup.dedupe_session(records_only)
    for index, reason in session_result.removed.items():
        _file_of(survivors[index]).removed["dedupe_session"] += 1
    after_session = [survivors[index] for index in session_result.kept]

    # ---- L2b 前缀去重 ----
    prefix_result = dedup.dedupe_prefix([work.record for work in after_session])
    for index, _reason in prefix_result.removed.items():
        _file_of(after_session[index]).removed["dedupe_prefix"] += 1
    after_prefix = [after_session[index] for index in prefix_result.kept]

    # ---- L3 计量 + 分片 ----
    shards: list[dict[str, Any]] = []
    state = {
        "index": 1,
        "path": out_dir / cfg.shard_name_format.format(index=1),
        "records": 0,
        "tokens": 0,
        "bytes": 0,
    }
    handle = open(state["path"], "wb")

    def _close_shard() -> None:
        handle.close()
        shards.append(
            {
                "name": state["path"].name,
                "records": state["records"],
                "tokens": state["tokens"],
                "bytes": state["path"].stat().st_size,
            }
        )

    def _roll_shard() -> None:
        nonlocal handle
        _close_shard()
        state["index"] += 1
        state["path"] = out_dir / cfg.shard_name_format.format(index=state["index"])
        state["records"] = 0
        state["tokens"] = 0
        state["bytes"] = 0
        handle = open(state["path"], "wb")

    def _emit(payload: bytes, work: _Work, piece_tokens: int) -> None:
        current = state["path"].stat().st_size if state["records"] else 0
        if state["records"] and current + len(payload) > cfg.shard_max_bytes:
            _roll_shard()
        handle.write(payload)
        handle.flush()
        state["records"] += 1
        state["tokens"] += piece_tokens
        _file_of(work).delivered_tokens += piece_tokens

    stopped = False
    stop_file: str | None = None
    for work in after_prefix:
        if stopped:
            file_ledger = _file_of(work)
            file_ledger.removed["stop_cut"] = file_ledger.removed.get("stop_cut", 0) + 1
            continue

        messages = dedup.get_messages(work.record)
        work.tokens = counter.count_messages(messages)
        pieces = (
            _split_windows(work.record, messages, counter=counter, max_tokens=cfg.max_record_tokens)
            if work.tokens > cfg.max_record_tokens
            else [work.record]
        )
        for piece in pieces:
            payload = json.dumps(piece, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
            piece_tokens = counter.count_messages(dedup.get_messages(piece))
            _emit(payload, work, piece_tokens)
        _file_of(work).delivered_records += 1
        if len(pieces) > 1:
            _file_of(work).window_pieces += len(pieces) - 1

        if cfg.token_target and _total_tokens(ledgers) >= cfg.token_target:
            stopped = True
            stop_file = work.origin.source_key
            _file_of(work).status = "done_partial"

    if state["records"]:
        _close_shard()
    else:
        handle.close()
        state["path"].unlink(missing_ok=True)

    # 到达目标后不再"拉取"后续文件：manifest 里只保留真正处理过的文件，
    # 停止的那个文件用 stop_cut 记账（不能静默丢掉未交付的记录）。
    file_keys = list(ledgers)
    if stop_file is not None:
        file_keys = file_keys[: file_keys.index(stop_file) + 1]
    included = {key: ledgers[key] for key in file_keys}

    total_delivered_tokens = _total_tokens(included)
    manifest = {
        "schema": ledger.MANIFEST_SCHEMA,
        "generated_at": "1970-01-01T00:00:00Z",
        "stop_reason": "token_target_reached" if stopped else "source_exhausted",
        "token_target": cfg.token_target,
        "tokenizer": tokenizer_meta
        or {
            "library": "tiktoken",
            "encoding": tokens.ENCODING_NAME,
            "method": tokens.METHOD_NAME,
            "unit": "sum(per_message_compact_json)",
        },
        "totals": {
            "source_key": "<totals>",
            "raw_lines": sum(f.raw_lines for f in included.values()),
            "removed": {
                step: sum(f.removed.get(step, 0) for f in included.values())
                for step in set().union(*[set(f.removed) for f in included.values()])
            },
            "delivered_records": sum(f.delivered_records for f in included.values()),
            "delivered_tokens": total_delivered_tokens,
            "window_pieces": sum(f.window_pieces for f in included.values()),
        },
        "files": [
            {
                "source_key": f.source_key,
                "raw_lines": f.raw_lines,
                "removed": dict(sorted(f.removed.items())),
                "delivered_records": f.delivered_records,
                "delivered_tokens": f.delivered_tokens,
                "window_pieces": f.window_pieces,
                "status": f.status,
            }
            for f in included.values()
        ],
        "shards": shards,
    }
    if stop_file:
        manifest["stopped_at_file"] = stop_file

    with open(out_dir / "manifest.json", "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return manifest


def _total_tokens(ledgers: dict[str, _FileLedger]) -> int:
    return sum(f.delivered_tokens for f in ledgers.values())


class _TiktokenCounter:
    """把 ``accept.tokens`` 的函数封装成 counter 对象。"""

    def count_text(self, text: str) -> int:
        return tokens.count_text(text)

    def count_messages(self, messages: Sequence[Any]) -> int:
        return tokens.count_messages(messages)
