#!/usr/bin/env python3
"""验收侧：交付分片完整性检查（流式，逐行，不整文件进内存）。

每一项都对应锁定口径里的一条硬要求：

* 每条都能 ``json.loads``，末尾行完整（文件必须以换行结尾）；
* 单包体积不超阈值；
* 单条不超 128k token（超长必须已切窗，切窗记录要带窗口标记）；
* 每条仍满足交付口径：``messages ≥ 100``、有 ``rs_`` 前缀的 rs_id、
  ``request.messages[].content[]`` 里有 ``type=encrypted_content`` 的 part；
* 分片命名连续、无重复记录、交付集内无前缀关系。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from . import dedup
from .ledger import Problem

MIB = 1024 * 1024


@dataclass
class ShardConfig:
    min_messages: int = 100
    max_record_tokens: int = 128_000
    shard_max_bytes: int = 500 * MIB
    #: 允许的超标比例（只对体积，且默认 2%：切分点是"写完才判断"的普遍做法）
    shard_slack: float = 0.02
    glob: str = "deliver-*.jsonl"
    require_trailing_newline: bool = True
    window_marker_keys: tuple[str, ...] = (
        "window",
        "_window",
        "window_index",
        "window_part",
        "chunk",
        "_chunk",
    )
    min_messages_applies_to_windows: bool = False
    #: 开发侧实际输出把窗口内容写在顶层 ``messages``，而 ``request.messages`` 仍是
    #: 未切窗的完整对话。默认判为错误（下游按真实 schema 读会拿到超长记录），
    #: 一旦双方确认改口径，用这个开关放行。
    allow_top_level_window_messages: bool = False
    max_records_for_prefix_check: int = 200_000

    @property
    def shard_hard_limit(self) -> int:
        return int(self.shard_max_bytes * (1 + self.shard_slack))


@dataclass
class ShardReport:
    name: str
    path: str
    bytes: int = 0
    records: int = 0
    tokens: int = 0
    empty_lines: int = 0
    windowed_records: int = 0
    max_record_tokens: int = 0
    problems: list[Problem] = field(default_factory=list)
    fingerprints: list[str] = field(default_factory=list)
    rs_ids: list[str] = field(default_factory=list)
    window_rs_ids: list[str] = field(default_factory=list)
    windowing: list[tuple[str, int, int, int, Any]] = field(default_factory=list)
    #: 交付单元的 token：非切窗记录按行，切窗记录按 source 聚合（整组算一条）
    unit_tokens: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not any(p.level == "error" for p in self.problems)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "bytes": self.bytes,
            "records": self.records,
            "tokens": self.tokens,
            "windowed_records": self.windowed_records,
            "max_record_tokens": self.max_record_tokens,
            "ok": self.ok,
            "problems": [p.as_dict() for p in self.problems],
        }


# --------------------------------------------------------------------------
# 取字段
# --------------------------------------------------------------------------


def get_messages(record: Any) -> list:
    return dedup.get_messages(record)


def has_encrypted_part(record: Any) -> bool:
    """``request.messages[].content[]`` 里是否有 ``type=encrypted_content`` 的 part。"""
    for message in get_messages(record):
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        parts = content if isinstance(content, list) else []
        for part in parts:
            if not isinstance(part, dict):
                continue
            if part.get("type") != "encrypted_content":
                continue
            value = part.get("encrypted_content")
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return True
    return False


def rs_ids(record: Any) -> list[str]:
    """``response.response_data.output[].id`` 里所有 ``rs_`` 前缀的 id。"""
    if not isinstance(record, dict):
        return []
    response = record.get("response")
    if not isinstance(response, dict):
        return []
    data = response.get("response_data")
    if not isinstance(data, dict):
        return []
    output = data.get("output")
    if not isinstance(output, list):
        return []
    found: list[str] = []
    for item in output:
        if isinstance(item, dict):
            value = item.get("id")
            if isinstance(value, str) and value.startswith("rs_"):
                found.append(value)
    return found


def window_info(record: Any, keys: Sequence[str] = ShardConfig().window_marker_keys) -> dict[str, Any] | None:
    """识别并归一化切窗标记。

    两种写法都认：

    * 验收侧参考格式 ``{"window": {"index", "count", "source_fingerprint"}}``；
    * 开发侧实际格式 ``{"_window": {"index", "of", "source": {"key", "off"},
      "messages_total", "oversize_message"}}``。

    归一后统一给出 ``index`` / ``count`` / ``fingerprint`` / ``messages_total``。
    """
    if not isinstance(record, dict):
        return None
    for container in (record, record.get("meta"), record.get("_meta")):
        if not isinstance(container, dict):
            continue
        for key in keys:
            value = container.get(key)
            if value is None:
                continue
            if not isinstance(value, dict):
                return {"value": value}
            count = value.get("count", value.get("of"))
            fingerprint = value.get("source_fingerprint") or value.get("fingerprint")
            if not fingerprint:
                source = value.get("source")
                if isinstance(source, dict):
                    fingerprint = f"{source.get('key')}#{source.get('off')}"
            return {
                **value,
                "index": value.get("index"),
                "count": count,
                "fingerprint": fingerprint or "",
                "messages_total": value.get("messages_total"),
            }
    return None


def window_messages(record: Any, window: dict[str, Any] | None) -> tuple[list, str]:
    """返回 (窗口内容, 内容位置)。

    位置 ``top`` = 顶层 ``messages``（开发侧现状）；``request`` = ``request.messages``。
    """
    if window is not None:
        top = record.get("messages")
        if isinstance(top, list):
            return top, "top"
    return get_messages(record), "request"


# --------------------------------------------------------------------------
# 单个分片
# --------------------------------------------------------------------------


def check_shard(
    path: str | Path,
    cfg: ShardConfig,
    *,
    counter: Any | None = None,
    collect_fingerprints: bool = True,
) -> ShardReport:
    path = Path(path)
    report = ShardReport(name=path.name, path=str(path))
    report.bytes = path.stat().st_size

    if report.bytes > cfg.shard_hard_limit:
        report.problems.append(
            Problem(
                "error",
                path.name,
                f"分片体积 {report.bytes} 字节超过阈值 {cfg.shard_max_bytes}"
                f"（含 {int(cfg.shard_slack * 100)}% 容差后上限 {cfg.shard_hard_limit}）",
            )
        )

    ends_with_newline = True
    if report.bytes:
        with open(path, "rb") as tail_handle:
            tail_handle.seek(-1, 2)
            ends_with_newline = tail_handle.read(1) == b"\n"
    with open(path, "rb") as handle:
        for lineno, raw in enumerate(handle, start=1):
            where = f"{path.name}:{lineno}"
            stripped = raw.strip()
            if not stripped:
                report.empty_lines += 1
                continue

            try:
                record = json.loads(raw)
            except Exception as exc:
                report.problems.append(Problem("error", where, f"无法 json.loads: {exc}（行内容损坏或末尾被截断）"))
                continue

            if not isinstance(record, dict):
                report.problems.append(Problem("error", where, "记录不是 JSON 对象"))
                continue

            report.records += 1
            request_messages = get_messages(record)
            if not isinstance(request_messages, list):
                report.problems.append(Problem("error", where, "缺少 request.messages 数组"))
                request_messages = []

            window = window_info(record, cfg.window_marker_keys)
            messages, content_location = window_messages(record, window)

            if window is not None:
                report.windowed_records += 1
                index = window.get("index")
                count = window.get("count")
                if isinstance(index, int) and isinstance(count, int) and count > 0:
                    if not 0 <= index < count:
                        report.problems.append(
                            Problem("error", where, f"窗口下标越界: index={index}, count={count}")
                        )
                    report.windowing.append(
                        (
                            str(window.get("fingerprint") or ""),
                            index,
                            count,
                            len(messages),
                            window.get("messages_total"),
                        )
                    )
                elif isinstance(window.get("value"), int):
                    # 只写了 window_index 这类裸标记：至少保证非负
                    if window["value"] < 0:
                        report.problems.append(Problem("error", where, "窗口下标为负"))

                if content_location == "top" and len(request_messages) > len(messages):
                    level = "warn" if cfg.allow_top_level_window_messages else "error"
                    report.problems.append(
                        Problem(
                            level,
                            where,
                            f"切窗把窗口内容写在顶层 messages（{len(messages)} 条），"
                            f"而 request.messages 仍是未切窗的 {len(request_messages)} 条——"
                            "下游按真实 schema 读 request.messages 会拿到超长记录，切窗等于没做",
                        )
                    )

            if window is None or cfg.min_messages_applies_to_windows:
                if len(messages) < cfg.min_messages:
                    report.problems.append(
                        Problem(
                            "error",
                            where,
                            f"messages 只有 {len(messages)} 条，少于锁定口径的 {cfg.min_messages}",
                        )
                    )

            if not rs_ids(record):
                report.problems.append(
                    Problem("error", where, "response.response_data.output[] 里没有 rs_ 前缀的 id")
                )
            elif window is not None:
                report.window_rs_ids.extend(rs_ids(record))
            else:
                report.rs_ids.extend(rs_ids(record))

            # 密文 part 是"源记录级"属性：切窗后单片可能一条密文都没有，
            # 逐片要求会把正确交付判死；非切窗记录仍然逐条要求。
            if window is None and not has_encrypted_part(record):
                report.problems.append(
                    Problem(
                        "error",
                        where,
                        "request.messages[].content[] 里没有 type=encrypted_content 的 part",
                    )
                )

            if collect_fingerprints:
                report.fingerprints.append(hashlib.blake2b(stripped, digest_size=16).hexdigest())

            if counter is None:
                continue
            try:
                tokens = counter.count_messages(messages)
            except Exception as exc:  # pragma: no cover - 计数器异常要显式暴露
                report.problems.append(Problem("error", where, f"token 复算失败: {exc!r}"))
                continue
            report.tokens += tokens
            report.max_record_tokens = max(report.max_record_tokens, tokens)
            unit_key = (
                f"window:{window.get('fingerprint')}"
                if window is not None and window.get("fingerprint")
                else f"line:{path.name}:{lineno}"
            )
            report.unit_tokens[unit_key] = report.unit_tokens.get(unit_key, 0) + tokens
            if tokens > cfg.max_record_tokens:
                report.problems.append(
                    Problem(
                        "error",
                        where,
                        f"单条 {tokens} token 超过上限 {cfg.max_record_tokens}"
                        + ("（即使已切窗也超，说明窗口切得太大）" if window is not None else "，且没有窗口标记"),
                    )
                )

    if cfg.require_trailing_newline and report.records and not ends_with_newline:
        report.problems.append(Problem("error", path.name, "末尾行没有换行结尾（文件可能被截断）"))

    return report


# --------------------------------------------------------------------------
# 整个交付目录
# --------------------------------------------------------------------------


@dataclass
class DeliveryReport:
    directory: str
    shards: list[ShardReport] = field(default_factory=list)
    problems: list[Problem] = field(default_factory=list)
    #: 交付单元（源记录粒度）的 token，切窗组已按 source 合并
    unit_tokens: dict[str, int] = field(default_factory=dict)
    #: 切窗指纹 → 已交付的窗口下标
    window_index_map: dict[str, list[int]] = field(default_factory=dict)

    @property
    def max_unit_tokens(self) -> int:
        return max(self.unit_tokens.values()) if self.unit_tokens else 0

    @property
    def records(self) -> int:
        return sum(s.records for s in self.shards)

    @property
    def bytes(self) -> int:
        return sum(s.bytes for s in self.shards)

    @property
    def tokens(self) -> int:
        return sum(s.tokens for s in self.shards)

    @property
    def ok(self) -> bool:
        return not any(p.level == "error" for p in self.all_problems())

    def all_problems(self) -> list[Problem]:
        problems = list(self.problems)
        for shard in self.shards:
            problems.extend(shard.problems)
        return problems

    def as_dict(self) -> dict[str, Any]:
        return {
            "directory": self.directory,
            "shards": [s.as_dict() for s in self.shards],
            "records": self.records,
            "bytes": self.bytes,
            "tokens": self.tokens,
            "max_unit_tokens": self.max_unit_tokens,
            "ok": self.ok,
            "problems": [p.as_dict() for p in self.all_problems()],
        }


def _shard_sort_key(path: Path) -> tuple[int, str]:
    digits = "".join(ch for ch in path.stem if ch.isdigit())
    return (int(digits) if digits else 0, path.name)


def discover_shards(directory: str | Path, cfg: ShardConfig) -> list[Path]:
    return sorted(Path(directory).glob(cfg.glob), key=_shard_sort_key)


def check_delivery(
    directory: str | Path,
    cfg: ShardConfig,
    *,
    counter: Any | None = None,
    check_prefix_invariant: bool = True,
) -> DeliveryReport:
    directory = Path(directory)
    report = DeliveryReport(directory=str(directory))
    shard_paths = discover_shards(directory, cfg)
    if not shard_paths:
        report.problems.append(
            Problem("error", str(directory), f"目录下没有匹配 {cfg.glob} 的分片")
        )
        return report

    # 命名连续性：deliver-0001 / deliver-0002 ... 不能跳号
    numbers = [_shard_sort_key(p)[0] for p in shard_paths]
    expected = list(range(numbers[0], numbers[0] + len(numbers)))
    if numbers != expected:
        report.problems.append(
            Problem(
                "error",
                str(directory),
                f"分片编号不连续: {numbers}（从 {numbers[0]} 开始应为 {expected}）",
            )
        )

    seen_fingerprints: dict[str, str] = {}
    seen_rs: dict[str, str] = {}
    seen_window_rs: dict[str, str] = {}
    window_groups: dict[str, list[tuple[str, int, int, int, Any]]] = {}
    sequences_for_prefix: list[list] = []

    for path in shard_paths:
        shard = check_shard(path, cfg, counter=counter)
        report.shards.append(shard)
        for fingerprint in shard.fingerprints:
            if fingerprint in seen_fingerprints:
                report.problems.append(
                    Problem("error", path.name, f"记录与 {seen_fingerprints[fingerprint]} 完全重复")
                )
            else:
                seen_fingerprints[fingerprint] = path.name
        for rs_id in shard.rs_ids:
            if rs_id in seen_rs:
                report.problems.append(
                    Problem(
                        "error",
                        path.name,
                        f"rs_id {rs_id} 在同一交付集里重复出现（{seen_rs[rs_id]} 已有）",
                    )
                )
            else:
                seen_rs[rs_id] = path.name
        # 切窗片段共享同一个 rs_id 是正常的（一条超长记录切成多片），
        # 所以只对"非切窗记录之间"查重，另外单独校验窗口覆盖完整性。
        for rs_id in shard.window_rs_ids:
            seen_window_rs.setdefault(rs_id, path.name)
        for fingerprint, index, count, n_messages, messages_total in shard.windowing:
            window_groups.setdefault(fingerprint, []).append(
                (path.name, index, count, n_messages, messages_total)
            )
            if fingerprint:
                report.window_index_map.setdefault(fingerprint, []).append(index)
        for unit_key, unit_tokens in shard.unit_tokens.items():
            report.unit_tokens[unit_key] = report.unit_tokens.get(unit_key, 0) + unit_tokens

    for rs_id, where in seen_window_rs.items():
        if rs_id in seen_rs:
            report.problems.append(
                Problem(
                    "error",
                    where,
                    f"rs_id {rs_id} 既出现在切窗片段里又出现在完整记录里（{seen_rs[rs_id]}）",
                )
            )

    for fingerprint, entries in window_groups.items():
        if not fingerprint:
            continue
        counts = {count for _, _, count, _, _ in entries}
        indices = sorted(index for _, index, _, _, _ in entries)
        if len(counts) != 1:
            report.problems.append(
                Problem("error", "delivery", f"同一源记录的窗口片数不一致: {sorted(counts)}")
            )
            continue
        expected = list(range(counts.pop()))
        if indices != expected:
            report.problems.append(
                Problem(
                    "error",
                    "delivery",
                    f"切窗覆盖不完整或重叠（指纹 {fingerprint[:12]}）: 实际 {indices}，应为 {expected}",
                )
            )
            continue
        totals = {total for _, _, _, _, total in entries if isinstance(total, int)}
        n_sum = sum(n for _, _, _, n, _ in entries)
        if len(totals) > 1:
            report.problems.append(
                Problem("error", "delivery", f"同一源记录的 messages_total 不一致: {sorted(totals)}")
            )
        elif totals:
            messages_total = totals.pop()
            if n_sum != messages_total:
                report.problems.append(
                    Problem(
                        "error",
                        "delivery",
                        f"切窗片段的 messages 合计 {n_sum} ≠ 源记录 {messages_total} 条"
                        "（有窗口内容缺失或重叠）",
                    )
                )

    if check_prefix_invariant:
        for path in shard_paths:
            if len(sequences_for_prefix) >= cfg.max_records_for_prefix_check:
                break
            with open(path, "rb") as handle:
                for raw in handle:
                    if not raw.strip():
                        continue
                    try:
                        record = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(record, dict):
                        continue
                    window = window_info(record, cfg.window_marker_keys)
                    content, _location = window_messages(record, window)
                    sequences_for_prefix.append(content if isinstance(content, list) else [])

    if check_prefix_invariant and sequences_for_prefix:
        violations = dedup.find_sequence_violations(sequences_for_prefix)
        for violation in violations[:20]:
            report.problems.append(
                Problem(
                    "error",
                    "delivery",
                    f"交付集内仍有前缀关系（{violation.reason}）："
                    f"第 {violation.short_index} 条 是 第 {violation.long_index} 条 的前缀",
                )
            )
        if len(violations) > 20:
            report.problems.append(
                Problem("error", "delivery", f"另有 {len(violations) - 20} 处前缀违规未逐条列出")
            )

    return report
