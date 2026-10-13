#!/usr/bin/env python3
"""Stream-validate normalized B-class JSONL and emit a JSON quality report."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = {
    "run_id",
    "data_type",
    "language",
    "initial_instruction",
    "model",
    "tools_available",
    "messages",
    "turn_count",
    "tool_calls",
    "final_answer",
}
ALLOWED_ROLES = {"system", "user", "assistant", "tool", "environment"}
ALLOWED_STATUSES = {"success", "failed", "timeout", "cancelled"}
LANGUAGES = {"zh-CN", "en", "mixed"}
CHANGE_MARKERS = re.compile(
    r"\b(?:change|revise|update|instead|remove|add|fix|correct|wrong|"
    r"don't|do not|retry|again)\b|"
    r"改|修改|换成|删除|增加|不对|不是|不要|纠正|修复|重试|重新",
    re.I,
)
REFERENCE_MARKERS = re.compile(
    r"\b(?:it|its|that|this|those|these|them|same|above|previous|earlier|"
    r"instead|also|then|continue|again|change|update|remove|add|fix|keep)\b|"
    r"这个|那个|这些|那些|它|它们|上述|上面|前面|之前|刚才|继续|再|然后|"
    r"改|修改|换成|删除|增加|补充|修复|也要|还是|保留|按照|基于",
    re.I,
)
WRAPPER_BLOCKS = re.compile(
    r"<(?:system-reminder|task-notification|ide_opened_file|ide_selection|"
    r"command-message|local-command-caveat)[^>]*>.*?</(?:system-reminder|"
    r"task-notification|ide_opened_file|ide_selection|command-message|"
    r"local-command-caveat)>",
    re.I | re.S,
)
ERROR_MARKERS = re.compile(
    r"\b(?:error|failed|failure|exception|traceback|timed out|timeout|"
    r"permission denied|not found)\b|错误|失败|异常|超时|拒绝访问|未找到",
    re.I,
)
UNRESOLVED_MARKERS = re.compile(
    r"\b(?:cannot|can't|unable to|could not|failed to|not possible)\b|"
    r"无法|不能|未能|失败|做不到",
    re.I,
)
# A2: 仅当最终答复在收尾处自述"任务失败/未完成"时才算未恢复；
# 描述错误行为（如"该接口会返回 404"）不再命中。
UNRESOLVED_SELF_REPORT = re.compile(
    r"(?:任务|需求|修复|部署|迁移|构建)[^。\n]{0,16}(?:失败|未完成|未能完成|没有完成)"
    r"|(?:failed|unable|couldn'?t|could not|cannot|did not|was not|were not)\s+(?:to\s+)?"
    r"(?:complete|finish|resolve|fix|deploy|build|deliver)"
    r"|(?:未能|无法|不能|做不到)\s*[^。\n]{0,8}\s*(?:完成|解决|实现|交付)"
    r"|\b(?:incomplete|unresolved|not\s+completed)\b",
    re.I,
)
# 需求方 2026-10-11 拍板：以下三类不再作为拒绝项（统计保留）。
#   孤儿调用/调用无返回 (missing_tool_result/orphan_tool_result)
#   停在等工具结果的收尾 (no_closing_statement)
#   未声明工具的历史调用 (used_tool_not_declared, B5 单快照豁免)
STATS_ONLY_CODES = {
    "missing_tool_result",
    "orphan_tool_result",
    "used_tool_not_declared",
    "no_closing_statement",
}
# 内容质量类错误：按需求文档 §7 的比率门（≥95%）批次把关，不进入结构 100% 门
CONTENT_QUALITY_CODES = {
    "fewer_than_2_turns",
    "missing_tool_call",
    "short_final_assistant_answer",
    "unrecovered_tool_failure",
    "no_multiturn_context_dependency",
    "invalid_call_turn_index",
}


def normalize_model_id(model: str) -> str:
    normalized = re.sub(r"[._\s]+", "-", model.strip().lower())
    return re.sub(r"-+", "-", normalized)


def model_is_allowed(model: Any) -> bool:
    if not isinstance(model, str) or not model.strip():
        return False
    normalized = normalize_model_id(model)
    opus = re.search(
        r"(?:^|[-/:])(?:claude-)?opus-(\d+)(?:-(\d+))?(?:$|[-/:])",
        normalized,
    )
    if opus:
        major = int(opus.group(1))
        minor = opus.group(2)
        return major > 4 or (major == 4 and minor is not None and len(minor) <= 2 and int(minor) >= 8)
    fable = re.search(
        r"(?:^|[-/:])(?:claude-)?fable-(\d+)(?:-(\d+))?(?:$|[-/:])",
        normalized,
    )
    if fable:
        return int(fable.group(1)) >= 5
    return bool(
        re.search(
            r"(?:^|[-/:])gpt-(?:5-6-sol|6-sol|6-1-sol|6-astra)(?:$|[-/:])",
            normalized,
        )
    )


def meaningful_text(value: Any) -> bool:
    return isinstance(value, str) and len(re.findall(r"[A-Za-z0-9\u3400-\u9fff]", value)) >= 4


def lexical_units(text: str) -> set[str]:
    latin = {
        token.lower()
        for token in re.findall(r"[A-Za-z0-9_./-]{3,}", text)
        if token.lower() not in {"the", "and", "for", "with", "this", "that"}
    }
    han = "".join(re.findall(r"[\u3400-\u9fff]", text))
    return latin | {han[index : index + 2] for index in range(max(0, len(han) - 1))}


def has_context_dependency(user_texts: list[str]) -> bool:
    if len(user_texts) < 2:
        return False
    prior_units = lexical_units(user_texts[0])
    for text in user_texts[1:]:
        units = lexical_units(text)
        if REFERENCE_MARKERS.search(text) or prior_units.intersection(units):
            return True
        prior_units.update(units)
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument(
        "--passed-output",
        type=Path,
        help="optional JSONL path receiving only rows that pass every check",
    )
    parser.add_argument(
        "--allow-duplicate-run-id",
        action="store_true",
        help="do not fail rows whose run_id also appears elsewhere in the input",
    )
    parser.add_argument("--max-examples", type=int, default=20)
    return parser.parse_args()


def add_error(
    errors: list[str], code: str, detail: str = ""
) -> None:
    errors.append(f"{code}: {detail}" if detail else code)


def validate_row(row: Any) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []
    stats: dict[str, Any] = {"stats_only": []}
    if not isinstance(row, dict):
        return ["row_not_object"], stats

    missing = sorted(REQUIRED_FIELDS - row.keys())
    if missing:
        add_error(errors, "missing_required_fields", ",".join(missing))

    run_id = row.get("run_id")
    if not isinstance(run_id, str) or not run_id.startswith("agent-mt-"):
        add_error(errors, "invalid_run_id")
    if row.get("data_type") != "agent_multiturn":
        add_error(errors, "invalid_data_type")
    if row.get("language") not in LANGUAGES:
        add_error(errors, "invalid_language")
    if not model_is_allowed(row.get("model")):
        add_error(errors, "disallowed_model", repr(row.get("model")))

    messages = row.get("messages")
    calls = row.get("tool_calls")
    available = row.get("tools_available")
    if not isinstance(messages, list):
        add_error(errors, "messages_not_list")
        messages = []
    if not isinstance(calls, list):
        add_error(errors, "tool_calls_not_list")
        calls = []
    if not isinstance(available, list):
        add_error(errors, "tools_available_not_list")
        available = []

    user_texts: list[str] = []
    raw_user_texts: list[str] = []
    assistant_call_positions: dict[str, int] = {}
    assistant_calls: dict[str, dict[str, Any]] = {}
    result_positions: dict[str, int] = {}
    result_ids: list[str] = []
    for position, message in enumerate(messages):
        if not isinstance(message, dict):
            add_error(errors, "message_not_object", str(position))
            continue
        role = message.get("role")
        if role not in ALLOWED_ROLES:
            add_error(errors, "invalid_message_role", f"{position}:{role!r}")
        if role == "user":
            content = message.get("content")
            raw_user_texts.append(content if isinstance(content, str) else "")
            cleaned_content = (
                WRAPPER_BLOCKS.sub(" ", content).strip()
                if isinstance(content, str)
                else ""
            )
            if meaningful_text(cleaned_content):
                user_texts.append(cleaned_content)
        elif role == "assistant":
            embedded = message.get("tool_calls", [])
            if embedded is None:
                embedded = []
            if not isinstance(embedded, list):
                add_error(errors, "embedded_tool_calls_not_list", str(position))
                embedded = []
            for call in embedded:
                if not isinstance(call, dict):
                    add_error(errors, "embedded_tool_call_not_object", str(position))
                    continue
                call_id = call.get("id")
                if not isinstance(call_id, str) or not call_id:
                    add_error(errors, "invalid_embedded_call_id", str(position))
                    continue
                if call_id in assistant_calls:
                    add_error(errors, "duplicate_embedded_call_id", call_id)
                assistant_calls[call_id] = call
                assistant_call_positions[call_id] = position
        elif role in {"tool", "environment"}:
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or not call_id:
                add_error(errors, "invalid_tool_result_id", str(position))
                continue
            if call_id in result_positions:
                add_error(errors, "duplicate_tool_result_id", call_id)
            result_positions[call_id] = position
            result_ids.append(call_id)

    turn_count = row.get("turn_count")
    if not isinstance(turn_count, int) or isinstance(turn_count, bool):
        add_error(errors, "invalid_turn_count")
    else:
        if len(user_texts) < 2:
            add_error(errors, "fewer_than_2_turns")
        if turn_count != len(raw_user_texts):
            add_error(errors, "turn_count_mismatch", f"declared={turn_count},actual={len(raw_user_texts)}")
    if len(user_texts) >= 2 and not has_context_dependency(user_texts):
        add_error(errors, "no_multiturn_context_dependency")
    if not messages or not isinstance(messages[-1], dict) or messages[-1].get("role") != "assistant":
        add_error(errors, "trajectory_not_ending_with_assistant")
        last_content = None
    else:
        last_content = messages[-1].get("content")
        # B4：纯工具调用收尾（停在等工具结果）→ 统计不拒绝（需求方已取消该限制）
        if not isinstance(last_content, str) or not last_content.strip():
            stats["stats_only"].append("no_closing_statement")
    if row.get("final_answer") != last_content:
        add_error(errors, "final_answer_mismatch")
    if raw_user_texts and row.get("initial_instruction") != raw_user_texts[0]:
        add_error(errors, "initial_instruction_mismatch")

    declared_names: set[str] = set()
    for tool in available:
        if not isinstance(tool, dict):
            add_error(errors, "tool_definition_not_object")
            continue
        name = tool.get("name")
        if isinstance(name, str) and name:
            declared_names.add(name)
        else:
            add_error(errors, "invalid_tool_definition_name")
        if not isinstance(tool.get("schema"), dict):
            add_error(errors, "invalid_tool_definition_schema", repr(name))

    top_calls: dict[str, dict[str, Any]] = {}
    services: set[str] = set()
    tool_names: set[str] = set()
    statuses: Counter[str] = Counter()
    used_count = 0
    undeclared_used: set[str] = set()
    for call in calls:
        if not isinstance(call, dict):
            add_error(errors, "top_tool_call_not_object")
            continue
        call_id = call.get("call_id")
        if not isinstance(call_id, str) or not call_id:
            add_error(errors, "invalid_top_call_id")
            continue
        if call_id in top_calls:
            add_error(errors, "duplicate_top_call_id", call_id)
        top_calls[call_id] = call
        name = call.get("tool_name")
        service = call.get("mcp_service")
        status = call.get("status")
        if not isinstance(name, str) or not name:
            add_error(errors, "invalid_tool_name", call_id)
        else:
            tool_names.add(name)
            if name not in declared_names:
                undeclared_used.add(name)
        if not isinstance(service, str) or not service:
            add_error(errors, "invalid_mcp_service", call_id)
        else:
            services.add(service)
        if status is not None and status not in ALLOWED_STATUSES:
            add_error(errors, "invalid_tool_status", f"{call_id}:{status!r}")
        elif status is not None:
            statuses[status] += 1
        call_turn = call.get("turn_index")
        if not isinstance(call_turn, int) or isinstance(call_turn, bool) or not isinstance(turn_count, int) or not 1 <= call_turn <= turn_count:
            add_error(errors, "invalid_call_turn_index", call_id)
        if "result_summary" not in call:
            add_error(errors, "missing_result_summary", call_id)
        if call.get("used_by_later_steps") is True:
            used_count += 1

    # B5 豁免：历史调用的声明在其各自原始请求中，单快照格式无法归因——只统计不拒绝。
    if undeclared_used:
        stats["stats_only"].append("used_tool_not_declared:" + ",".join(sorted(undeclared_used)))

    if not top_calls:
        add_error(errors, "missing_tool_call")
    top_ids = set(top_calls)
    embedded_ids = set(assistant_calls)
    result_id_set = set(result_ids)
    if top_ids != embedded_ids:
        add_error(errors, "top_vs_embedded_call_id_mismatch", f"top_only={len(top_ids-embedded_ids)},embedded_only={len(embedded_ids-top_ids)}")
    # 需求方已取消"孤儿调用/调用无返回"拒绝限制——只统计不拒绝。
    missing_results = top_ids - result_id_set
    orphan_results = result_id_set - top_ids
    if missing_results:
        stats["stats_only"].append(f"missing_tool_result:count={len(missing_results)}")
    if orphan_results:
        stats["stats_only"].append(f"orphan_tool_result:count={len(orphan_results)}")
    for call_id in top_ids & embedded_ids:
        top = top_calls[call_id]
        embedded = assistant_calls[call_id]
        if top.get("tool_name") != embedded.get("name"):
            add_error(errors, "tool_name_mismatch", call_id)
        if top.get("arguments") != embedded.get("arguments"):
            add_error(errors, "tool_arguments_mismatch", call_id)
    for call_id in top_ids & result_id_set & embedded_ids:
        call_position = assistant_call_positions[call_id]
        result_position = result_positions[call_id]
        if result_position <= call_position:
            add_error(errors, "tool_result_before_call", call_id)
    state_updates = row.get("state_updates")
    if state_updates is not None and not isinstance(state_updates, list):
        add_error(errors, "invalid_state_updates")
    elif isinstance(state_updates, list):
        for update in state_updates:
            update_turn = update.get("turn") if isinstance(update, dict) else None
            if not isinstance(update_turn, int) or not isinstance(turn_count, int) or not 2 <= update_turn <= turn_count:
                add_error(errors, "invalid_state_update_turn", repr(update_turn))

    failed_result_ids = {
        call_id
        for call_id, call in top_calls.items()
        if call.get("status") == "failed"
    }
    failed_result_ids.update(
        call_id
        for call_id, position in result_positions.items()
        if ERROR_MARKERS.search(str(messages[position].get("content", "")))
    )
    # A2：仅在最终答复收尾处自述"任务失败/未完成"才算未恢复；
    # 描述错误行为（如"该接口会返回 404"）不命中。
    if failed_result_ids and isinstance(last_content, str) and UNRESOLVED_SELF_REPORT.search(last_content[-400:]):
        add_error(errors, "unrecovered_tool_failure", f"count={len(failed_result_ids)}")

    key_stats = row.get("key_stats")
    if not isinstance(key_stats, dict):
        add_error(errors, "missing_or_invalid_key_stats")
        key_stats = {}
    else:
        expected = {
            "turn_count": len(raw_user_texts),
            "tool_call_count": len(top_calls),
            "successful_tool_call_count": statuses["success"],
            "mcp_service_count": len(services),
        }
        for name, value in expected.items():
            if key_stats.get(name) != value:
                add_error(errors, "key_stats_mismatch", f"{name}:{key_stats.get(name)!r}!={value!r}")

    stats.update(
        {
            "run_id": run_id,
            "model": row.get("model"),
            "turn_count": len(raw_user_texts),
            "tool_call_count": len(top_calls),
            "statuses": statuses,
            "services": services,
            "tool_names": tool_names,
            "used_count": used_count,
            "context_dependency": has_context_dependency(user_texts),
            "change_or_correction": any(CHANGE_MARKERS.search(text) for text in user_texts[1:]),
            "failed_recovery": any(statuses[name] for name in ("failed", "timeout")) and used_count == len(top_calls),
            "stop_reason": key_stats.get("source_stop_reason"),
        }
    )
    return errors, stats


def main() -> int:
    args = parse_args()
    input_path = args.input.resolve()
    report_path = args.report.resolve()
    passed_output = args.passed_output.resolve() if args.passed_output else None
    if passed_output == input_path:
        raise SystemExit("--passed-output must differ from the input path")
    if report_path == input_path:
        raise SystemExit("--report must differ from the input path")

    error_counts: Counter[str] = Counter()
    stats_only_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    model_distribution: Counter[str] = Counter()
    turn_distribution: Counter[str] = Counter()
    status_distribution: Counter[str] = Counter()
    stop_reason_distribution: Counter[str] = Counter()
    single_service_distribution: Counter[str] = Counter()
    unique_tools: set[str] = set()
    unique_services: set[str] = set()
    seen_ids: set[str] = set()
    total = parsed = structurally_valid = 0
    rows_multiturn_fail = rows_tool_missing = rows_ending_fail = rows_lt2 = 0
    p_multiturn_ok = p_ctx_ok = p_tool_ok = True
    passed_rows = 0
    total_calls = used_calls = 0
    multi_service = context_dependency = change_or_correction = failed_recovery = 0
    # 交付集（passed）聚合：比率门只量交付集可控项
    p_tools: Counter[str] = Counter()
    p_services: Counter[str] = Counter()
    p_statuses: Counter[str] = Counter()
    p_total_calls = p_used_calls = p_correction = 0

    passed_temporary = (
        passed_output.with_name(passed_output.name + ".tmp")
        if passed_output is not None
        else None
    )
    passed_handle = None
    try:
        if passed_temporary is not None:
            passed_output.parent.mkdir(parents=True, exist_ok=True)
            passed_handle = passed_temporary.open("w", encoding="utf-8", newline="\n")
        with args.input.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                total += 1
                try:
                    row = json.loads(line)
                    parsed += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    error_counts["json_parse_error"] += 1
                    if len(examples) < args.max_examples:
                        examples.append({"line": line_number, "errors": [f"json_parse_error: {exc}"]})
                    continue
                errors, stats = validate_row(row)
                run_id = stats.get("run_id")
                if isinstance(run_id, str):
                    if run_id in seen_ids and not args.allow_duplicate_run_id:
                        errors.append("duplicate_run_id")
                    seen_ids.add(run_id)
                # 统计类发现（需求方已取消拒绝限制的三类等）单独计数，不影响通过
                stats_only = stats.get("stats_only", [])
                if stats_only:
                    stats_only_counts.update({s.split(":", 1)[0] for s in stats_only})
                if errors:
                    # 计数按受影响行数（行内重复原因只计一次）；区分结构错误与内容质量错误
                    codes = {error.split(":", 1)[0] for error in errors}
                    error_counts.update(codes)
                    structural_codes = codes - CONTENT_QUALITY_CODES
                    if not structural_codes:
                        structurally_valid += 1
                    if "fewer_than_2_turns" in codes:
                        rows_lt2 += 1
                    if "missing_tool_call" in codes:
                        rows_tool_missing += 1
                    if "short_final_assistant_answer" in codes or "trajectory_not_ending_with_assistant" in codes:
                        rows_ending_fail += 1
                    if len(examples) < args.max_examples:
                        examples.append({"line": line_number, "run_id": run_id, "errors": errors[:20]})
                else:
                    structurally_valid += 1
                    passed_rows += 1
                    p_tools.update(stats.get("tool_names", set()))
                    p_services.update(stats.get("services", set()))
                    p_statuses.update(stats.get("statuses", Counter()))
                    p_total_calls += stats.get("tool_call_count", 0)
                    p_used_calls += stats.get("used_count", 0)
                    if stats.get("change_or_correction"):
                        p_correction += 1
                    if stats.get("turn_count", 0) < 2:
                        p_multiturn_ok = False
                    if not stats.get("context_dependency"):
                        p_ctx_ok = False
                    if stats.get("tool_call_count", 0) < 1:
                        p_tool_ok = False
                    if passed_handle is not None:
                        passed_handle.write(line if line.endswith("\n") else line + "\n")

                model_distribution[str(stats.get("model"))] += 1
                turn_distribution[str(stats.get("turn_count"))] += 1
                statuses: Counter[str] = stats.get("statuses", Counter())
                status_distribution.update(statuses)
                total_calls += stats.get("tool_call_count", 0)
                used_calls += stats.get("used_count", 0)
                services: set[str] = stats.get("services", set())
                unique_services.update(services)
                unique_tools.update(stats.get("tool_names", set()))
                if len(services) >= 2:
                    multi_service += 1
                elif len(services) == 1:
                    single_service_distribution[next(iter(services))] += 1
                context_dependency += int(bool(stats.get("context_dependency")))
                change_or_correction += int(bool(stats.get("change_or_correction")))
                failed_recovery += int(bool(stats.get("failed_recovery")))
                stop_reason_distribution[str(stats.get("stop_reason"))] += 1
        if passed_handle is not None:
            passed_handle.close()
            passed_handle = None
            passed_temporary.replace(passed_output)
    finally:
        if passed_handle is not None:
            passed_handle.close()
        if passed_temporary is not None:
            passed_temporary.unlink(missing_ok=True)

    success_calls = p_statuses["success"]
    total_calls = p_total_calls
    used_calls = p_used_calls
    largest_single_service = single_service_distribution.most_common(1)
    max_single_service_count = largest_single_service[0][1] if largest_single_service else 0
    max_single_service_name = largest_single_service[0][0] if largest_single_service else None
    pct = lambda numerator, denominator: numerator / denominator if denominator else 0.0
    # 门模型（方案 A 延续）：逐条标准决定进交付；比率门只量交付集可控项
    #（成功率/利用率/工具多样性/修改场景，均在交付集上计算）；
    # 源批次构成（多轮率/依赖率/单服务集中度）为信息性指标，双口径上报不设门。
    checks = {
        "jsonl_parse_rate_100pct": parsed == total,
        "schema_structural_conformance_rate_100pct": structurally_valid == total,
        "unique_run_id_rate_100pct": (
            True if args.allow_duplicate_run_id else len(seen_ids) == total
        ),
        "model_allowlist_rate_100pct": error_counts["disallowed_model"] == 0,
        "delivered_multiturn_rate_100pct": p_multiturn_ok,
        "delivered_context_dependency_rate_100pct": p_ctx_ok,
        "delivered_tool_call_presence_100pct": p_tool_ok,
        "tool_result_use_rate_at_least_90pct": pct(used_calls, total_calls) >= 0.90,
        "tool_call_success_rate_at_least_90pct": pct(success_calls, total_calls) >= 0.90,
        "at_least_15_tool_names": len(p_tools) >= 15,
        "change_or_correction_trajectory_rate_at_least_5pct": pct(p_correction, passed_rows) >= 0.05,
    }
    report = {
        "validator": "normalized B-class JSONL structural validator",
        "input": str(args.input.resolve()),
        "passed_output": str(passed_output) if passed_output is not None else None,
        "duplicate_run_ids_ignored": args.allow_duplicate_run_id,
        "scope_note": "Automated structural and aggregate checks only; semantic correctness, artifact existence, sensitive-information safety, and human quality review are not assessed.",
        "total_rows": total,
        "parsed_rows": parsed,
        "structurally_valid_rows": structurally_valid,
        "structural_pass_rate": pct(structurally_valid, total),
        "unique_run_ids": len(seen_ids),
        "error_counts": dict(error_counts),
        "stats_only_counts": dict(stats_only_counts),
        "content_quality_rate_gates": {
            "multiturn_valid_pct": round(pct(total - rows_lt2, total) * 100, 2),
            "tool_presence_pct": round(pct(total - rows_tool_missing, total) * 100, 2),
            "final_answer_complete_pct": round(pct(total - rows_ending_fail, total) * 100, 2),
            "context_dependency_pct_among_multiturn": round(pct(context_dependency, total - rows_lt2) * 100, 2),
        },
        "error_examples": examples,
        "model_distribution": dict(model_distribution),
        "turn_count_distribution": dict(sorted(turn_distribution.items(), key=lambda item: int(item[0]))),
        "source_stop_reason_distribution": dict(stop_reason_distribution),
        "tool_metrics": {
            "unique_tool_names": len(unique_tools),
            "unique_services": len(unique_services),
            "total_calls": total_calls,
            "status_distribution": dict(status_distribution),
            "success_rate": pct(success_calls, total_calls),
            "used_by_later_steps_true_calls": used_calls,
            "used_by_later_steps_rate": pct(used_calls, total_calls),
        },
        "trajectory_metrics": {
            "multi_service_trajectories": multi_service,
            "multi_service_rate": pct(multi_service, total),
            "single_service_trajectories_by_service": dict(single_service_distribution),
            "largest_single_service_as_only_source": max_single_service_name,
            "largest_single_service_as_only_source_count": max_single_service_count,
            "largest_single_service_as_only_source_rate": pct(max_single_service_count, total),
            "informational_note": "源批次构成指标（多轮率/依赖率/单服务集中度）为信息性统计，不设门；交付集逐条已满足 B 类多轮标准",
            "context_dependency_proxy_trajectories": context_dependency,
            "context_dependency_proxy_rate": pct(context_dependency, total),
            "change_or_correction_trajectories": change_or_correction,
            "change_or_correction_rate": pct(change_or_correction, total),
            "failed_or_timeout_with_later_use_trajectories": failed_recovery,
            "failed_or_timeout_with_later_use_rate": pct(failed_recovery, total),
        },
        "automated_requirement_checks": checks,
        "all_automated_checks_passed": all(checks.values()),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
