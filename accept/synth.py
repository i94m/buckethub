#!/usr/bin/env python3
"""合成语料（纯本地、零网络）：验收和单测只吃这里造的数据。

覆盖锁定口径里点名的边界：

===============  =========================================================
场景             期望
===============  =========================================================
abcde/abcdefg    跨 session，短的被前缀去重删掉，长的留下
完全相同         两条 messages 一致、session 不同，只留一条
同长不同内容     长度相同但内容不同，两条都留
跨 session 前缀   session uuid 不同但 messages 有前缀关系，仍按前缀删
空 messages      ``messages=[]`` 是任何序列的前缀；全空时只留一条
共享前 k 个元素  前 k 个成员相同后分叉，**不是前缀**，两条都留（负例）
===============  =========================================================

另外造一批"该被过滤掉"的记录（无 rs_id / 无密文 / 不足 100 条），
让对账里每一格移除量都能真正被算到。
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any, Iterable

MIB = 1024 * 1024


# --------------------------------------------------------------------------
# 基础构件
# --------------------------------------------------------------------------


def message(role: str, text: str, *, encrypted: str | None = None) -> dict[str, Any]:
    """造一个 message 成员：``content`` 是 part 数组。"""
    part_type = "input_text" if role in ("user", "system", "developer") else "output_text"
    parts: list[dict[str, Any]] = [{"type": part_type, "text": text}]
    if encrypted is not None:
        parts.append({"type": "encrypted_content", "encrypted_content": encrypted})
    return {"role": role, "content": parts}


def dialog(turns: int, *, prefix: str = "", encrypted_every: int = 7) -> list[dict[str, Any]]:
    """造 ``turns`` 条 message 的对话；每 ``encrypted_every`` 条带一个密文 part。"""
    messages: list[dict[str, Any]] = []
    for turn in range(turns):
        role = "user" if turn % 2 == 0 else "assistant"
        encrypted = None
        if encrypted_every and turn % encrypted_every == 0:
            encrypted = f"gAAAA-{prefix}-{turn}-" + ("x" * 24)
        messages.append(message(role, f"{prefix} 第 {turn} 条内容。", encrypted=encrypted))
    return messages


def record(
    *,
    session_uuid: str,
    messages: list[dict[str, Any]],
    user_hash: str = "u0",
    rs_id: str | None = None,
    request_id: str = "req-0001",
    timestamp: str = "2026-09-29T07:58:38.842Z",
    model: str = "gpt-6-astra",
) -> dict[str, Any]:
    """按真实 schema 的层级造一条记录：messages 在 request 下、rs_id 在 output 里。"""
    output: list[dict[str, Any]] = []
    if rs_id is not None:
        output.append(
            {
                "id": rs_id,
                "type": "reasoning",
                "content": [],
                "encrypted_content": "gAAAA-output-" + rs_id,
                "summary": [],
                "metadata": {"turn_id": "turn-0001"},
            }
        )
    return {
        "session_id": f"{session_uuid}~{user_hash}",
        "request_id": request_id,
        "timestamp": timestamp,
        "request": {
            "model": model,
            "messages": messages,
            "input": [],
            "tools": [],
        },
        "response": {"response_data": {"output": output}},
        "model": model,
    }


# --------------------------------------------------------------------------
# 六类去重边界（小样本，供单测直接断言）
# --------------------------------------------------------------------------


CASES: dict[str, dict[str, Any]] = {
    "abcde_vs_abcdefg": {
        "records": [
            record(session_uuid="aaaaaaaa-0000", messages=dialog(5, prefix="A"), rs_id="rs_short"),
            record(session_uuid="bbbbbbbb-1111", messages=dialog(7, prefix="A"), rs_id="rs_long"),
        ],
        "kept": [1],
        "note": "跨 session：5 条的是 7 条的前缀，删短的",
    },
    "identical": {
        "records": [
            record(session_uuid="cccccccc-0000", messages=dialog(6, prefix="B"), rs_id="rs_dup1"),
            record(session_uuid="dddddddd-1111", messages=dialog(6, prefix="B"), rs_id="rs_dup2"),
        ],
        "kept": [0],
        "note": "messages 完全一致：只留一条（留原下标最小的）",
    },
    "same_length_diff_content": {
        "records": [
            record(session_uuid="eeeeeeee-0000", messages=dialog(5, prefix="C1"), rs_id="rs_c1"),
            record(session_uuid="ffffffff-1111", messages=dialog(5, prefix="C2"), rs_id="rs_c2"),
        ],
        "kept": [0, 1],
        "note": "同长不同内容：两条都留",
    },
    "cross_session_prefix": {
        "records": [
            record(session_uuid="aaaaaaaa-2222", messages=dialog(4, prefix="D"), rs_id="rs_d1"),
            record(session_uuid="bbbbbbbb-3333", messages=dialog(9, prefix="D"), rs_id="rs_d2"),
            record(session_uuid="cccccccc-4444", messages=dialog(6, prefix="D"), rs_id="rs_d3"),
        ],
        "kept": [1],
        "note": "三条同源前缀链，session 各不相同，只留最长的 (9 条)",
    },
    "same_session_prefix": {
        "records": [
            record(session_uuid="99999999-0000", messages=dialog(5, prefix="E"), user_hash="u1"),
            record(session_uuid="99999999-0000", messages=dialog(8, prefix="E"), user_hash="u2"),
        ],
        "kept": [1],
        "note": "同一 session 的两个快照：session 去重就该删掉短的那条",
    },
    "empty_messages": {
        "records": [
            record(session_uuid="77777777-0000", messages=[], rs_id="rs_empty"),
            record(session_uuid="88888888-1111", messages=dialog(5, prefix="F"), rs_id="rs_f"),
        ],
        "kept": [1],
        "note": "空 messages 是任何序列的前缀，被前缀去重删掉",
    },
    "all_empty_messages": {
        "records": [
            record(session_uuid="66666666-0000", messages=[], rs_id="rs_e1"),
            record(session_uuid="55555555-1111", messages=[], rs_id="rs_e2"),
        ],
        "kept": [0],
        "note": "全空时互为完全相同，只留一条",
    },
    "shared_prefix_k_negative": {
        "records": [
            record(
                session_uuid="12121212-0000",
                messages=dialog(3, prefix="G") + [message("user", "分叉点-左")],
                rs_id="rs_g1",
            ),
            record(
                session_uuid="13131313-1111",
                messages=dialog(3, prefix="G") + [message("user", "分叉点-右")],
                rs_id="rs_g2",
            ),
        ],
        "kept": [0, 1],
        "note": "负例：共享前 3 个成员后分叉，不构成前缀关系，两条都留",
    },
}


def dedupe_case(name: str) -> dict[str, Any]:
    return CASES[name]


def dedupe_case_names() -> list[str]:
    return list(CASES)


# --------------------------------------------------------------------------
# 端到端语料：过滤 → session 去重 → 前缀去重 → 分片 → manifest
# --------------------------------------------------------------------------


def _record_pair(index: int, turns: int, *, prefix: str | None = None) -> dict[str, Any]:
    return record(
        session_uuid=f"0000{index:04x}-aaaa-bbbb-cccc-{index:012x}",
        messages=dialog(turns, prefix=prefix or f"S{index}"),
        user_hash=f"u{index}",
        rs_id=f"rs_{index:06d}",
        request_id=f"req-{index:06d}",
    )


def build_source_corpus(*, seed: int = 20261103, group_size: int = 6) -> list[dict[str, Any]]:
    """造一批原始记录（已按真实 schema），混合可交付与必须被过滤的样本。

    结构：若干"会话快照链"（同源 messages 逐级变长、session 各不相同） +
    各类过滤样本 + 一条超长记录（验证切窗）。
    """
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []

    # 1) 前缀链：同一段对话每轮存一个快照，长度 100→100+2k（同组文案必须一致，
    #    否则就成不了前缀关系）
    for group in range(group_size):
        base_turns = 100 + rng.randrange(0, 5)
        for step in range(3):
            index = group * 10 + step
            records.append(_record_pair(index, base_turns + 2 * step, prefix=f"S{group}"))

    # 1b) 同一个 session（~ 前的 uuid 相同）的两次快照：session 去重该删掉短的那条
    for group in range(2):
        uuid = f"0001{group:04x}-aaaa-bbbb-cccc-{group:012x}"
        for step, turns in enumerate((100, 106)):
            records.append(
                record(
                    session_uuid=uuid,
                    messages=dialog(turns, prefix=f"SNAP{group}"),
                    user_hash=f"h{group}{step}",
                    rs_id=f"rs_{700 + group * 2 + step:06d}",
                    request_id=f"req-{700 + group * 2 + step:06d}",
                )
            )

    # 2) 完全相同但 session uuid 不同的重复
    duplicate = _record_pair(900, 104)
    twin = record(
        session_uuid="9999ffff-aaaa-bbbb-cccc-000000000900",
        messages=duplicate["request"]["messages"],
        user_hash="u900b",
        rs_id="rs_000900b",
        request_id="req-000900b",
    )
    records.append(twin)

    # 3) 共享前 k 个成员后分叉（负例，必须都留下来）
    shared = dialog(3, prefix="SHARED")
    for index, tail in ((801, "左支"), (802, "右支")):
        messages = shared + dialog(100, prefix=f"TAIL{index}") + [message("user", tail)]
        records.append(
            record(
                session_uuid=f"7777{index:04x}-aaaa-bbbb-cccc-{index:012x}",
                messages=messages,
                user_hash=f"u{index}",
                rs_id=f"rs_{index:06d}",
            )
        )

    # 4) 该被过滤的：没有 rs_id
    for index in (851, 852):
        records.append(
            record(
                session_uuid=f"8666{index:04x}-aaaa-bbbb-cccc-{index:012x}",
                messages=dialog(105, prefix=f"NOID{index}"),
                user_hash=f"u{index}",
                rs_id=None,
            )
        )

    # 5) 该被过滤的：没有 encrypted_content part
    for index in (861, 862):
        records.append(
            record(
                session_uuid=f"8888{index:04x}-aaaa-bbbb-cccc-{index:012x}",
                messages=dialog(103, prefix=f"NOENC{index}", encrypted_every=0),
                user_hash=f"u{index}",
                rs_id=f"rs_{index:06d}",
            )
        )

    # 6) 该被过滤的：messages 不足 100 条
    for index in (871, 872):
        records.append(
            record(
                session_uuid=f"8999{index:04x}-aaaa-bbbb-cccc-{index:012x}",
                messages=dialog(42, prefix=f"SHORT{index}"),
                user_hash=f"u{index}",
                rs_id=f"rs_{index:06d}",
            )
        )

    # 7) 空 messages（既不足 100 条，也是前缀）
    records.append(
        record(
            session_uuid="8000eeee-aaaa-bbbb-cccc-000000000001",
            messages=[],
            user_hash="u880",
            rs_id="rs_000880",
        )
    )

    # 8) 超长记录：验证 128k token 切窗
    records.append(
        record(
            session_uuid="8001ffff-aaaa-bbbb-cccc-000000000002",
            messages=dialog(260, prefix="HUGE" + ("z" * 1200)),
            user_hash="u881",
            rs_id="rs_000881",
        )
    )

    rng.shuffle(records)
    return records


def split_into_files(
    records: Iterable[dict[str, Any]], *, files: int = 3
) -> dict[str, list[dict[str, Any]]]:
    """按顺序切成若干"源文件"，模拟多文件流式处理。"""
    buckets: dict[str, list[dict[str, Any]]] = {
        f"Gpt-2026-10-18/part-{index:04d}.jsonl": [] for index in range(files)
    }
    names = list(buckets)
    for position, item in enumerate(records):
        buckets[names[position % files]].append(item)
    return buckets


def write_jsonl(path: str | Path, records: Iterable[dict[str, Any]]) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        for item in records:
            handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
            count += 1
    return count


def write_corpus(root: str | Path, records: list[dict[str, Any]] | None = None) -> list[Path]:
    """把语料写成 ``<root>/raw/part-NNNN.jsonl``，返回文件列表。"""
    root = Path(root)
    files = split_into_files(records if records is not None else build_source_corpus())
    written: list[Path] = []
    for name, items in files.items():
        path = root / "raw" / Path(name).name
        write_jsonl(path, items)
        written.append(path)
    return written
