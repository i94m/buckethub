#!/usr/bin/env python3
"""验收侧：1B 到顶即停的原子性与上界检查。

"到达 1B 立刻停"不能以**把一条记录切成两半**为代价。这里校验四件事：

1. **整组原子**：停止点上的记录如果是切窗记录，交付里必须正好有 `0..of-1`
   全部窗口；不许只落前面几片。
2. **停止点可追溯**：`manifest.stop_after_record` 必须存在，且与 `emit.journal`
   最后一笔、与交付 token 总量都能对上。
3. **超出目标的上界**：整组原子必然会让 `delivered_tokens` 略超目标，但超出量
   不得超过**该条记录自身的 token 量**（等价于"≤ 前缀去重后单条最大 token"）。
4. **未交付条数闭合**：`not_delivered = kept_records − delivered_records`。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .ledger import Manifest, Problem
from .shard import DeliveryReport


def _journal_entries(out_dir: Path) -> list[dict[str, Any]] | None:
    path = out_dir / "emit.journal"
    if not path.exists():
        return None
    entries: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                entries.append(json.loads(line))
    return entries


def check_stop(manifest: Manifest, delivery: DeliveryReport, out_dir: str | Path) -> list[Problem]:
    problems: list[Problem] = []
    out_dir = Path(out_dir)
    total = manifest.total

    if manifest.stop_reason != "token_target_reached":
        return problems

    target = manifest.token_target
    delivered_tokens = total.delivered_tokens or delivery.tokens
    stop_record = manifest.stop_after_record
    stop_file = manifest.stop_file

    # ---- 0) 文件边界停：停点之后不允许有任何源文件的记录 ----
    file_tokens = 0
    if stop_file:
        per_file: dict[str, int] = {}
        for shard in manifest.shards:
            if shard.source_key:
                per_file[shard.source_key] = per_file.get(shard.source_key, 0) + shard.tokens
        later = sorted(k for k in per_file if k > stop_file)
        if later:
            problems.append(
                Problem(
                    "error",
                    "manifest.stop_file",
                    f"停在 {stop_file}，但交付里还有更后面的源文件：{later}"
                    "（文件边界停被破坏，后续文件不该被处理）",
                )
            )
        file_tokens = per_file.get(stop_file, 0)
        if not file_tokens:
            problems.append(
                Problem(
                    "error",
                    "manifest.stop_file",
                    f"stop_file={stop_file} 在交付里没有任何记录（停止点指向了没交付的文件）",
                )
            )

    # ---- 1) 停止点必须被记录，且能对上交付 ----
    if not stop_record and not stop_file:
        problems.append(
            Problem(
                "error",
                "manifest.stop_after_record",
                "声称到达 token 目标，但 manifest 既没有 stop_file 也没有 stop_after_record，"
                "无法确认停在哪条记录/哪个文件上",
            )
        )
    elif stop_record:
        where = f"stop_after_record:{stop_record.get('id')}"
        if stop_record.get("tokens_total_after") not in (None, delivered_tokens):
            problems.append(
                Problem(
                    "error",
                    where,
                    f"停止后累计 token {stop_record.get('tokens_total_after')} ≠ 交付 token {delivered_tokens}",
                )
            )
        windows = stop_record.get("windows")
        if isinstance(windows, int) and windows < 1:
            problems.append(Problem("error", where, f"windows={windows}，停止点不可信"))

        # 整组原子：停止点若是切窗记录，窗口必须全在
        if isinstance(windows, int) and windows > 1:
            source = stop_record.get("source") or {}
            key = source.get("key") if isinstance(source, dict) else None
            key = key or stop_record.get("key") or ""
            off = stop_record.get("off")
            if off is None and isinstance(source, dict):
                off = source.get("off")
            fingerprint = f"{key}#{off}"
            indices = sorted(delivery.window_index_map.get(fingerprint, []))
            if not indices:
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"停止点记录是一组 {windows} 个窗口，但交付里一个窗口都没有（停止点判断错位）",
                    )
                )
            elif indices != list(range(windows)):
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"停止点上的窗口组不完整：交付 {indices}，应为 {list(range(windows))}"
                        "（整组原子提交被破坏）",
                    )
                )

        entries = _journal_entries(out_dir)
        if entries is None:
            problems.append(
                Problem("warn", where, "没有 emit.journal，无法把停止点与实际最后一笔交付对上")
            )
        elif entries:
            journal_last = entries[-1]
            if journal_last.get("id") != stop_record.get("id"):
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"stop_after_record.id={stop_record.get('id')} ≠ journal 最后一笔 "
                        f"{journal_last.get('id')}（停止点与实际交付不一致）",
                    )
                )
            if stop_record.get("tokens") not in (None, journal_last.get("tokens")):
                problems.append(
                    Problem(
                        "error",
                        where,
                        f"该记录 token {stop_record.get('tokens')} ≠ journal 记账 {journal_last.get('tokens')}",
                    )
                )

    # ---- 2) 超出目标的上界 ----
    # 文件边界停的口径：上界是"停在的那个源文件自身的产出"，不是单条记录；
    # 记录边界停（无 stop_file）仍按单条记录上界验。
    overshoot = delivered_tokens - target if target else 0
    max_unit = file_tokens if stop_file and file_tokens else delivery.max_unit_tokens
    if target and max_unit and overshoot > max_unit:
        bound = (f"停止文件 {stop_file} 的产出 {file_tokens} token" if stop_file and file_tokens
                 else f"交付集里单条记录的最大 token {max_unit}")
        problems.append(
            Problem(
                "error",
                "manifest.token_target",
                f"超出目标 {overshoot} token，大于{bound}"
                + ("（文件边界停允许超出一个源文件，但不应超过这个上界）"
                   if stop_file else "（整组原子允许超出一条记录，但不应超过这个上界）"),
            )
        )

    # ---- 3) 未交付条数闭合 ----
    if total.kept_records is not None:
        expected_left = max(0, total.kept_records - total.delivered_records)
        if total.not_delivered != expected_left:
            problems.append(
                Problem(
                    "error",
                    "manifest.not_delivered",
                    f"not_delivered={total.not_delivered} ≠ kept {total.kept_records} "
                    f"− delivered {total.delivered_records} = {expected_left}",
                )
            )

    return problems
