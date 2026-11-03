#!/usr/bin/env python3
"""验收侧独立实现：session 去重 + messages 前缀去重。

与开发侧实现互不参考，只按锁定口径从头写一遍，用来当"第二套真相"
交叉验证交付物。口径（与 README 表格一致）：

1. session 同一性 = ``session_id`` 中 ``~`` 之前的 uuid 段（去空白、统一小写）。
   缺 ``session_id`` 的记录各自成一户，永不因 session 去重被删。
2. message 成员同一性 = 该成员完整 JSON 的规范序列化（key 排序、紧凑分隔符、
   UTF-8 原文），取 16 字节内容摘要；一条记录的序列 = 各成员摘要按序拼接。
3. 前缀关系 = 字节前缀。A 是 B 的严格前缀就丢 A；A == B 只留一条。
4. 实现只做"字典序排序 + 相邻比对"，O(n log n)。不建 Trie，不做两两比较。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

# 成员摘要长度（字节）。16 字节 = 128 bit，8~9 万条序列下碰撞概率可忽略。
MEMBER_DIGEST_BYTES = 16


# --------------------------------------------------------------------------
# 取字段
# --------------------------------------------------------------------------


def get_messages(record: Any) -> list:
    """取 ``request.messages``（锁定口径：messages 在 request 下，不在顶层）。"""
    if not isinstance(record, dict):
        return []
    request = record.get("request")
    if not isinstance(request, dict):
        return []
    messages = request.get("messages")
    return messages if isinstance(messages, list) else []


def get_session_id(record: Any) -> str:
    if not isinstance(record, dict):
        return ""
    value = record.get("session_id")
    return value if isinstance(value, str) else ""


def session_group_key(session_id: str) -> str:
    """``session_id`` 里 ``~`` 之前的 uuid 段；空串表示"没有可用的 session"。"""
    text = (session_id or "").strip()
    if not text:
        return ""
    return text.split("~", 1)[0].strip().lower()


def canonical_member_bytes(
    member: Any,
    *,
    fields: Sequence[str] | None = None,
    ensure_ascii: bool = False,
) -> bytes:
    """成员 JSON 的规范序列化（紧凑、key 排序）。

    ``fields`` 非空时只取这些字段（用于口径放宽的对照实验）；默认取全部字段，
    这样 ``encrypted_content`` 这类 part 也参与成员同一性判定。
    """
    if fields is not None and isinstance(member, dict):
        payload = {name: member[name] for name in fields if name in member}
    else:
        payload = member
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii
    ).encode("utf-8")


def member_digest(
    member: Any,
    *,
    fields: Sequence[str] | None = None,
    ensure_ascii: bool = False,
) -> bytes:
    payload = canonical_member_bytes(member, fields=fields, ensure_ascii=ensure_ascii)
    return hashlib.blake2b(payload, digest_size=MEMBER_DIGEST_BYTES).digest()


def sequence_bytes(messages: Iterable[Any], *, fields: Sequence[str] | None = None) -> bytes:
    """一条记录的 messages 序列指纹：定长成员摘要按序拼接。

    ``messages`` 为空 → ``b""``（长度 0 的序列，是任何序列的前缀）。
    """
    out = bytearray()
    for member in messages:
        out += member_digest(member, fields=fields)
    return bytes(out)


# --------------------------------------------------------------------------
# 结果
# --------------------------------------------------------------------------


@dataclass
class DedupeResult:
    """一步去重的结果：保留哪些、删了哪些、每类原因多少。"""

    step: str
    kept: list[int] = field(default_factory=list)
    removed: dict[int, str] = field(default_factory=dict)

    @property
    def removed_count(self) -> int:
        return len(self.removed)

    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for reason in self.removed.values():
            counts[reason] = counts.get(reason, 0) + 1
        return dict(sorted(counts.items()))

    def summary(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "input": len(self.kept) + len(self.removed),
            "kept": len(self.kept),
            "removed": self.removed_count,
            "reasons": self.reason_counts(),
        }


# --------------------------------------------------------------------------
# L2a：session 去重
# --------------------------------------------------------------------------


def dedupe_session(
    records: Sequence[Any],
    *,
    indices: Sequence[int] | None = None,
    keep: str = "longest",
) -> DedupeResult:
    """按 ``session_id`` 的 uuid 段去重，同户保留 ``messages`` 最长那条。

    并列最长时按 (序列指纹, 原下标) 兜底，保证可复现。
    """
    if keep != "longest":
        raise ValueError(f"不支持的保留策略: {keep!r}（锁定口径只有 longest）")
    order = list(range(len(records))) if indices is None else list(indices)
    result = DedupeResult(step="dedupe_session")
    groups: dict[str, list[int]] = {}
    for idx in order:
        key = session_group_key(get_session_id(records[idx]))
        if not key:
            # 没有 session_id：不参与去重，各自保留，但记一笔，方便对账与排查。
            result.kept.append(idx)
            continue
        groups.setdefault(key, []).append(idx)

    for key in sorted(groups):
        members = groups[key]
        if len(members) == 1:
            result.kept.append(members[0])
            continue
        ranked = sorted(
            members,
            key=lambda i: (
                -len(get_messages(records[i])),
                sequence_bytes(get_messages(records[i])),
                i,
            ),
        )
        winner = ranked[0]
        result.kept.append(winner)
        winner_len = len(get_messages(records[winner]))
        for idx in members:
            if idx == winner:
                continue
            loser_len = len(get_messages(records[idx]))
            result.removed[idx] = (
                "session_shorter_snapshot" if loser_len < winner_len else "session_same_length"
            )

    result.kept.sort()
    return result


# --------------------------------------------------------------------------
# L2b：前缀去重
# --------------------------------------------------------------------------


def dedupe_prefix(
    records: Sequence[Any],
    *,
    indices: Sequence[int] | None = None,
    fields: Sequence[str] | None = None,
) -> DedupeResult:
    """messages 序列的前缀去重：严格前缀丢弃短的那条，完全相同只留一条。

    正确性：字典序下"以 A 为前缀的字符串集合"是连续的一段且 A 本身居中最小，
    所以 A 若被任何更长序列覆盖，它后面紧邻的一定也是覆盖它的序列。
    因此只需排序后比对相邻两项，O(n log n)。
    """
    order = list(range(len(records))) if indices is None else list(indices)
    result = DedupeResult(step="dedupe_prefix")
    if not order:
        return result

    seqs = {idx: sequence_bytes(get_messages(records[idx]), fields=fields) for idx in order}

    # 两次稳定排序得到 (序列升序, 原下标降序)：同序列时留下原下标最小的那条，
    # 让"留哪条"确定可复现（锁定口径只要求留最长，等长时取最早的）。
    order.sort(key=lambda i: -i)
    order.sort(key=lambda i: seqs[i])

    kept_flags = {idx: True for idx in order}
    for pos in range(len(order) - 1):
        left, right = order[pos], order[pos + 1]
        left_seq, right_seq = seqs[left], seqs[right]
        if right_seq.startswith(left_seq):
            kept_flags[left] = False
            result.removed[left] = "identical_duplicate" if left_seq == right_seq else "prefix_of_longer"

    result.kept = [idx for idx in order if kept_flags[idx]]
    result.kept.sort()
    return result


# --------------------------------------------------------------------------
# 交付物不变量：交付集里不能存在前缀关系
# --------------------------------------------------------------------------


@dataclass
class PrefixViolation:
    short_index: int
    long_index: int
    reason: str  # identical_duplicate | prefix_of_longer


def find_prefix_violations(
    records: Sequence[Any],
    *,
    indices: Sequence[int] | None = None,
    fields: Sequence[str] | None = None,
) -> list[PrefixViolation]:
    """对已交付集合做同一套排序+相邻比对，返回全部违规对（0 条才是干净）。"""
    order = list(range(len(records))) if indices is None else list(indices)
    return find_sequence_violations(
        [get_messages(records[idx]) for idx in order], fields=fields
    )


def find_sequence_violations(
    sequences: Sequence[Sequence[Any]],
    *,
    fields: Sequence[str] | None = None,
) -> list[PrefixViolation]:
    """直接对一个 messages 序列列表做前缀扫描（切窗后比的是窗口内容）。"""
    order = list(range(len(sequences)))
    if len(order) < 2:
        return []
    records = sequences
    seqs = {idx: sequence_bytes(records[idx], fields=fields) for idx in order}
    order.sort(key=lambda i: -i)
    order.sort(key=lambda i: seqs[i])
    violations: list[PrefixViolation] = []
    for pos in range(len(order) - 1):
        left, right = order[pos], order[pos + 1]
        if seqs[right].startswith(seqs[left]):
            violations.append(
                PrefixViolation(
                    short_index=left,
                    long_index=right,
                    reason="identical_duplicate" if seqs[left] == seqs[right] else "prefix_of_longer",
                )
            )
    return violations


# --------------------------------------------------------------------------
# 一次跑完两步
# --------------------------------------------------------------------------


@dataclass
class FunnelResult:
    session: DedupeResult
    prefix: DedupeResult
    kept_indices: list[int]

    def summary(self) -> dict[str, Any]:
        return {
            "session": self.session.summary(),
            "prefix": self.prefix.summary(),
            "kept": len(self.kept_indices),
        }


def dedupe_funnel(
    records: Sequence[Any],
    *,
    indices: Sequence[int] | None = None,
    fields: Sequence[str] | None = None,
    order: str = "session_then_prefix",
) -> FunnelResult:
    """按锁定顺序跑两步：先 session_id 去重，再对存活集做前缀去重。"""
    if order != "session_then_prefix":
        raise ValueError(f"不支持的顺序: {order!r}（锁定口径：先 session 后前缀）")
    base = list(range(len(records))) if indices is None else list(indices)
    session_result = dedupe_session(records, indices=base)
    prefix_result = dedupe_prefix(records, indices=session_result.kept, fields=fields)
    return FunnelResult(
        session=session_result,
        prefix=prefix_result,
        kept_indices=prefix_result.kept,
    )
