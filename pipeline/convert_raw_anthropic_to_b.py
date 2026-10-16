#!/usr/bin/env python3
"""Format-convert raw Anthropic request/response JSONL to normalized B JSONL.

This module performs field mapping only. It does not enforce B-class quality or
compliance rules. Missing results, incomplete endings, undeclared tools, model
versions, turn counts, and context dependency do not cause rows to be filtered
out. Conversion warnings are written only to the separate report, keeping each
JSONL row aligned with the supplied demo schema. Only malformed JSON and
non-object JSON rows are unconvertible.

Input rows are expected to look like::

    {"session_id": "...", "request": {...}, "response": {"response_data": {...}}}

Output rows follow the normalized B-class schema used by the supplied reference
sample (run_id, data_type, messages, tool_calls, final_answer, and so on).

No sensitive-information filtering, redaction, or machine-injection filtering
is performed. The source files are never modified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator


MODEL_POLICY_DESCRIPTION = (
    "Opus >= 4.8; Fable >= 5; GPT-5.6 Sol; GPT-6 Sol; "
    "GPT-6.1 Sol; GPT-6 Astra"
)

REFERENCE_MARKERS = re.compile(
    r"\b(?:it|its|that|this|those|these|them|same|above|previous|earlier|"
    r"instead|also|then|continue|again|change|update|remove|add|fix|keep)\b|"
    r"这个|那个|这些|那些|它|它们|上述|上面|前面|之前|刚才|继续|再|然后|"
    r"改|修改|换成|删除|增加|补充|修复|也要|还是|保留|按照|基于",
    re.I,
)
CHANGE_MARKERS = re.compile(
    r"\b(?:change|revise|update|instead|remove|add|fix|correct|wrong|"
    r"don't|do not|retry|again)\b|"
    r"改|修改|换成|删除|增加|不对|不是|不要|纠正|修复|重试|重新",
    re.I,
)


@dataclass(frozen=True)
class Locator:
    raw_length: int
    file_index: int
    byte_offset: int
    ordinal: int
    whole_file: bool = False


class ConversionError(Exception):
    """A source row is valid JSON but cannot become a valid normalized row."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def normalize_model_id(model: str) -> str:
    normalized = model.strip().lower()
    normalized = re.sub(r"[._\s]+", "-", normalized)
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
        if major > 4:
            return True
        if major < 4 or minor is None:
            return False
        return len(minor) <= 2 and int(minor) >= 8

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


def raw_blocks(message: Any) -> list[dict[str, Any]]:
    if not isinstance(message, dict):
        return []
    content = message.get("content")
    if isinstance(content, list):
        return [item for item in content if isinstance(item, dict)]
    if isinstance(content, dict):
        return [content]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def text_from_blocks(message: Any) -> str:
    parts = [
        str(block.get("text", ""))
        for block in raw_blocks(message)
        if block.get("type") == "text" and str(block.get("text", "")).strip()
    ]
    return "\n".join(parts).strip()


def meaningful_text(text: str) -> bool:
    return len(re.findall(r"[A-Za-z0-9\u3400-\u9fff]", text)) >= 4


def lexical_units(text: str) -> set[str]:
    latin = {
        token.lower()
        for token in re.findall(r"[A-Za-z0-9_./-]{3,}", text)
        if token.lower() not in {"the", "and", "for", "with", "this", "that"}
    }
    han = "".join(re.findall(r"[\u3400-\u9fff]", text))
    bigrams = {han[index : index + 2] for index in range(max(0, len(han) - 1))}
    return latin | bigrams


def dependency_turns(user_texts: list[str]) -> list[int]:
    if len(user_texts) < 2:
        return []
    dependent: list[int] = []
    prior_units = lexical_units(user_texts[0])
    for turn, text in enumerate(user_texts[1:], 2):
        units = lexical_units(text)
        if REFERENCE_MARKERS.search(text) or prior_units.intersection(units):
            dependent.append(turn)
        prior_units.update(units)
    return dependent


def infer_language(messages: list[dict[str, Any]]) -> str:
    text = " ".join(
        str(message.get("content", ""))
        for message in messages
        if message.get("role") in {"user", "assistant"}
    )
    cjk = len(re.findall(r"[\u3400-\u9fff]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    if cjk and latin and min(cjk, latin) / max(cjk, latin) >= 0.2:
        return "mixed"
    return "zh-CN" if cjk >= latin * 0.1 else "en"


def infer_service(tool_name: str) -> str:
    if tool_name.startswith("mcp__"):
        parts = tool_name.split("__", 2)
        return parts[1] if len(parts) > 2 and parts[1] else "mcp"
    if "__" in tool_name:
        prefix = tool_name.split("__", 1)[0]
        return prefix or "unknown"
    if "." in tool_name:
        prefix = tool_name.split(".", 1)[0]
        return prefix or "unknown"
    lower = tool_name.lower()
    if lower in {"read", "grep", "glob", "ls", "read_file", "list_directory"}:
        return "filesystem"
    if "web" in lower or "search" in lower or "fetch" in lower:
        return "web"
    if lower in {"bash", "shell", "terminal", "exec", "exec_command"}:
        return "terminal"
    if "browser" in lower or lower in {"navigate", "screenshot"}:
        return "browser"
    if "agent" in lower or "task" in lower:
        return "agent"
    return tool_name.split("_", 1)[0].lower() or "unknown"


def result_as_text(block: dict[str, Any]) -> str:
    content = block.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts = [
            str(item.get("text", ""))
            for item in content
            if isinstance(item, dict)
            and item.get("type") == "text"
            and str(item.get("text", "")).strip()
        ]
        if text_parts:
            return "\n".join(text_parts)
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False, separators=(",", ":"))


def system_prompt_text(request: dict[str, Any]) -> str | None:
    system = request.get("system")
    if isinstance(system, str):
        return system or None
    if isinstance(system, list):
        parts = [
            str(item.get("text", ""))
            for item in system
            if isinstance(item, dict) and str(item.get("text", "")).strip()
        ]
        return "\n".join(parts) or None
    return None


def response_data(record: dict[str, Any]) -> dict[str, Any]:
    response = record.get("response")
    if not isinstance(response, dict):
        return {}
    nested = response.get("response_data")
    return nested if isinstance(nested, dict) else response


def stable_run_id(session_id: str) -> str:
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:16]
    return f"agent-mt-{digest}"


def normalize_tool_definitions(
    request: dict[str, Any], used_names: set[str]
) -> list[dict[str, Any]]:
    source_tools = request.get("tools")
    if not isinstance(source_tools, list):
        source_tools = []
    available: list[dict[str, Any]] = []
    declared: set[str] = set()
    for tool in source_tools:
        if not isinstance(tool, dict) or not isinstance(tool.get("name"), str):
            continue
        name = tool["name"]
        declared.add(name)
        schema = tool.get("input_schema")
        if not isinstance(schema, dict):
            schema = tool.get("schema") if isinstance(tool.get("schema"), dict) else {}
        available.append(
            {
                "service": infer_service(name),
                "name": name,
                "description": str(tool.get("description", "")),
                "schema": schema,
            }
        )
    return available


def convert_record(
    record: dict[str, Any], max_result_summary_chars: int, source_identity: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Map one source object without applying B-class compliance filters.

    Missing or inconsistent source information is represented honestly with
    null/empty values. Conversion warnings go only to the separate report and
    are never added to output JSONL rows. Nothing is invented merely to make a
    row pass downstream validation.
    """
    warnings: list[str] = []
    session_id = record.get("session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        session_id = source_identity
        warnings.append("missing_session_id_generated_from_source_location")

    request = record.get("request")
    if not isinstance(request, dict):
        request = {}
        warnings.append("missing_or_invalid_request")
    history = request.get("messages")
    if not isinstance(history, list):
        history = []
        warnings.append("missing_or_invalid_messages")
    final_response = response_data(record)
    if not final_response:
        warnings.append("missing_or_invalid_response")

    model = final_response.get("model") or request.get("model")

    messages: list[dict[str, Any]] = []
    user_texts: list[str] = []
    active_turn = 0
    sequence = 0
    calls: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    assistant_sequences: list[int] = []

    def append_assistant(message: dict[str, Any], is_final: bool = False) -> None:
        nonlocal sequence
        text = text_from_blocks(message)
        normalized_calls: list[dict[str, Any]] = []
        for block in raw_blocks(message):
            if block.get("type") != "tool_use":
                continue
            call_id = block.get("id")
            name = block.get("name")
            if not isinstance(call_id, str) or not call_id:
                warnings.append("tool_call_missing_id")
                call_id = None
            elif any(item["id"] == call_id for item in calls):
                warnings.append(f"duplicate_tool_call_id:{call_id}")
            if not isinstance(name, str) or not name:
                warnings.append(f"tool_call_missing_name:{call_id}")
                name = None
            sequence += 1
            calls.append({
                "id": call_id,
                "name": name,
                "arguments": block.get("input", {}),
                "turn": active_turn,
                "sequence": sequence,
                "is_final_response": is_final,
            })
            normalized_calls.append(
                {
                    "id": call_id,
                    "name": name,
                    "arguments": block.get("input", {}),
                }
            )
        sequence += 1
        entry: dict[str, Any] = {"role": "assistant", "content": text}
        if normalized_calls:
            entry["tool_calls"] = normalized_calls
        elif not text:
            warnings.append("assistant_message_without_text_or_tool_call")
        messages.append(entry)
        assistant_sequences.append(sequence)

    for raw_message in history:
        if not isinstance(raw_message, dict):
            warnings.append("non_object_message_skipped")
            continue
        role = raw_message.get("role")
        if role == "system":
            text = text_from_blocks(raw_message)
            messages.append({"role": "system", "content": text})
            continue
        if role == "user":
            text = text_from_blocks(raw_message)
            blocks = raw_blocks(raw_message)
            has_tool_result = any(block.get("type") == "tool_result" for block in blocks)
            if text or not has_tool_result:
                active_turn += 1
                user_texts.append(text)
                messages.append({"role": "user", "content": text})
                sequence += 1
                if not text:
                    warnings.append("empty_user_message")
            for block in blocks:
                if block.get("type") != "tool_result":
                    continue
                call_id = block.get("tool_use_id")
                if not isinstance(call_id, str) or not call_id:
                    warnings.append("tool_result_missing_call_id")
                    call_id = None
                elif any(item["id"] == call_id for item in results):
                    warnings.append(f"duplicate_tool_result_id:{call_id}")
                if active_turn == 0:
                    warnings.append(f"tool_result_before_first_user_turn:{call_id}")
                sequence += 1
                content = result_as_text(block)
                results.append({
                    "id": call_id,
                    "block": block,
                    "content": content,
                    "sequence": sequence,
                })
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": content,
                    }
                )
            continue
        if role == "assistant":
            append_assistant(raw_message)
            continue
        warnings.append(f"unknown_message_role:{role}")
        messages.append({"role": role, "content": raw_message.get("content")})

    if final_response:
        append_assistant(final_response, is_final=True)

    final_text = text_from_blocks(final_response) if final_response else ""
    used_names = {
        item["name"] for item in calls if isinstance(item.get("name"), str)
    }
    tools_available = normalize_tool_definitions(request, used_names)
    declared_names = {
        item["name"] for item in tools_available if isinstance(item.get("name"), str)
    }
    for undeclared in sorted(used_names - declared_names):
        warnings.append(f"used_tool_not_declared:{undeclared}")

    results_by_id: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        result_id = result.get("id")
        if isinstance(result_id, str):
            results_by_id.setdefault(result_id, []).append(result)
    consumed_results: set[int] = set()
    tool_rows: list[dict[str, Any]] = []
    for call in sorted(calls, key=lambda item: item["sequence"]):
        call_id = call.get("id")
        candidates = results_by_id.get(call_id, []) if isinstance(call_id, str) else []
        result = next((item for item in candidates if id(item) not in consumed_results), None)
        if result is None:
            summary = None
            later_assistant = False
            status = None
            error_message = None
            warnings.append(f"missing_tool_result:{call_id}")
        else:
            consumed_results.add(id(result))
            result_text = result["content"]
            summary = result_text
            if max_result_summary_chars and len(summary) > max_result_summary_chars:
                summary = summary[:max_result_summary_chars] + "\n...[truncated in result_summary]"
            later_assistant = any(
                assistant_sequence > result["sequence"]
                for assistant_sequence in assistant_sequences
            )
            is_error = result["block"].get("is_error") is True
            status = "failed" if is_error else "success"
            error_message = summary if is_error else None
        tool_rows.append(
            {
                "call_id": call_id,
                "turn_index": call["turn"],
                "mcp_service": (
                    infer_service(call["name"])
                    if isinstance(call.get("name"), str)
                    else None
                ),
                "tool_name": call["name"],
                "arguments": call["arguments"],
                "status": status,
                "result_summary": summary,
                "error_message": error_message,
                "used_by_later_steps": later_assistant,
                "latency_ms": None,
            }
        )
    for result in results:
        if id(result) not in consumed_results:
            warnings.append(f"orphan_tool_result:{result.get('id')}")

    state_updates: list[dict[str, Any]] = []
    successful_calls = sum(row["status"] == "success" for row in tool_rows)
    normalized = {
        "run_id": stable_run_id(session_id),
        "data_type": "agent_multiturn",
        "language": infer_language(messages),
        "initial_instruction": user_texts[0] if user_texts else "",
        "model": model,
        "system_prompt": system_prompt_text(request),
        "tools_available": tools_available,
        "messages": messages,
        "turn_count": active_turn,
        "tool_calls": tool_rows,
        "state_updates": state_updates,
        "final_answer": final_text,
        "execution_mode": "real",
        "key_stats": {
            "turn_count": active_turn,
            "tool_call_count": len(tool_rows),
            "total_characters": len(
                json.dumps(messages, ensure_ascii=False, separators=(",", ":"))
            ),
            "successful_tool_call_count": successful_calls,
            "mcp_service_count": len(
                {row["mcp_service"] for row in tool_rows}
            ),
        },
    }
    unique_warnings = list(dict.fromkeys(warnings))
    stats = {
        "model": model,
        "tool_names": sorted(used_names),
        "services": sorted({row["mcp_service"] for row in tool_rows}),
        "tool_calls": len(tool_rows),
        "successful_tool_calls": successful_calls,
        "has_change": any(CHANGE_MARKERS.search(text) for text in user_texts[1:]),
        "has_failed_recovered_call": any(
            row["status"] == "failed" and row["used_by_later_steps"]
            for row in tool_rows
        ),
        "source_stop_reason": final_response.get("stop_reason"),
        "warning_count": len(unique_warnings),
        "warnings": unique_warnings,
    }
    return normalized, stats


def resolve_inputs(values: list[str]) -> list[Path]:
    files: list[Path] = []
    for value in values:
        path = Path(value).expanduser().resolve()
        if path.is_dir():
            files.extend(
                sorted([*path.glob("*.jsonl"), *path.glob("*.json")])
            )
        elif path.is_file():
            files.append(path)
        else:
            raise FileNotFoundError(f"input not found: {path}")
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in files:
        if path not in seen:
            unique.append(path)
            seen.add(path)
    if not unique:
        raise FileNotFoundError("no JSONL or JSON input files found")
    return unique


def scan_records(
    paths: list[Path], report: dict[str, Any], dedupe_session_id: bool
) -> list[tuple[str, Locator]]:
    records: list[tuple[str, Locator]] = []
    best: dict[str, Locator] = {}
    input_rows = 0
    malformed = 0
    non_object = 0
    missing_session = 0
    replacements = 0
    def register(row: Any, candidate: Locator, path: Path) -> None:
        nonlocal non_object, missing_session, replacements
        if not isinstance(row, dict):
            non_object += 1
            return
        session_id = row.get("session_id")
        if not isinstance(session_id, str) or not session_id.strip():
            missing_session += 1
            session_id = f"source:{path}:{candidate.byte_offset}"
        if not dedupe_session_id:
            records.append((session_id, candidate))
            return
        previous = best.get(session_id)
        if previous is None or (candidate.raw_length, candidate.ordinal) >= (
            previous.raw_length,
            previous.ordinal,
        ):
            if previous is not None:
                replacements += 1
            best[session_id] = candidate

    for file_index, path in enumerate(paths):
        if path.suffix.lower() == ".json":
            ordinal = input_rows
            input_rows += 1
            try:
                with path.open("rb") as source:
                    row = json.load(source)
            except (json.JSONDecodeError, UnicodeDecodeError):
                malformed += 1
                continue
            register(
                row,
                Locator(path.stat().st_size, file_index, 0, ordinal, True),
                path,
            )
            continue
        with path.open("rb") as source:
            while True:
                offset = source.tell()
                raw_line = source.readline()
                if not raw_line:
                    break
                if not raw_line.strip():
                    continue
                ordinal = input_rows
                input_rows += 1
                raw = raw_line.rstrip(b"\r\n")
                try:
                    row = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    malformed += 1
                    continue
                candidate = Locator(len(raw), file_index, offset, ordinal)
                register(row, candidate, path)
    selected = (
        sorted(best.items(), key=lambda item: item[1].ordinal)
        if dedupe_session_id
        else records
    )
    report.update(
        {
            "input_rows": input_rows,
            "format_error_rows": malformed + non_object,
            "malformed_json_rows": malformed,
            "non_object_json_rows": non_object,
            "missing_session_id_rows": missing_session,
            "dedupe_session_id": dedupe_session_id,
            "selected_rows": len(selected),
            "duplicates_removed": (
                input_rows - malformed - non_object - len(selected)
                if dedupe_session_id
                else 0
            ),
            "longest_record_replacements": replacements,
        }
    )
    return selected


class _FileHandleCache:
    """Bounded LRU of open input file handles, keyed by file index.

    ``convert_files`` used to open *every* input file at once
    (``handles = [path.open("rb") for path in paths]``). On large source
    directories that exhausts ``RLIMIT_NOFILE`` / ``kern.maxfiles`` and aborts
    with ``OSError: [Errno 24] Too many open files`` (observed on a 119,769-file
    batch). ``selected`` is consumed in ascending ``locator.ordinal`` order,
    i.e. mostly file-index order, so a small LRU keeps the output byte-for-byte
    identical while capping concurrent descriptors at ``max_open``.
    """

    def __init__(self, paths: list[Path], max_open: int = 512) -> None:
        self._paths = paths
        self._max_open = max(1, max_open)
        self._handles: "OrderedDict[int, Any]" = OrderedDict()

    def get(self, index: int) -> Any:
        handle = self._handles.get(index)
        if handle is not None:
            self._handles.move_to_end(index)
            return handle
        if len(self._handles) >= self._max_open:
            _, evicted = self._handles.popitem(last=False)
            evicted.close()
        handle = self._paths[index].open("rb")
        self._handles[index] = handle
        return handle

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()


def read_locator(handles: "_FileHandleCache", locator: Locator) -> dict[str, Any]:
    handle = handles.get(locator.file_index)
    handle.seek(locator.byte_offset)
    row = json.load(handle) if locator.whole_file else json.loads(handle.readline())
    if not isinstance(row, dict):
        raise ConversionError("non_object_row")
    return row


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def convert_files(args: argparse.Namespace) -> dict[str, Any]:
    paths = resolve_inputs(args.inputs)
    output = Path(args.output).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve() if args.report else None
    if output in paths:
        raise ValueError("output path must be different from every input path")

    report: dict[str, Any] = {
        "converter": "raw Anthropic JSONL to normalized B-class JSONL (format-only)",
        "compliance_filtering": False,
        "model_filtering": False,
        "model_policy_reference_only": MODEL_POLICY_DESCRIPTION,
        "sensitive_information_filtering": False,
        "machine_injection_filtering": False,
        "inputs": [str(path) for path in paths],
        "output": str(output),
    }
    selected = scan_records(paths, report, args.dedupe_session_id)
    rejection_counts: Counter[str] = Counter()
    rejection_examples: list[dict[str, Any]] = []
    warning_counts: Counter[str] = Counter()
    model_distribution: Counter[str] = Counter()
    stop_reason_distribution: Counter[str] = Counter()
    tool_names: set[str] = set()
    sole_service: Counter[str] = Counter()
    converted_stats: list[dict[str, Any]] = []

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    handles = _FileHandleCache(paths, getattr(args, "max_open_files", 512))
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as destination:
            for session_id, locator in selected:
                try:
                    source_row = read_locator(handles, locator)
                    normalized, stats = convert_record(
                        source_row,
                        args.max_result_summary_chars,
                        f"source:{paths[locator.file_index]}:{locator.byte_offset}",
                    )
                except ConversionError as exc:
                    rejection_counts[exc.code] += 1
                    if len(rejection_examples) < args.max_examples:
                        rejection_examples.append(
                            {
                                "session_id": session_id,
                                "source_file": str(paths[locator.file_index]),
                                "source_line_ordinal": locator.ordinal + 1,
                                "code": exc.code,
                                "detail": exc.detail,
                            }
                        )
                    continue
                destination.write(
                    json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )
                converted_stats.append(stats)
                warning_counts.update(
                    warning.split(":", 1)[0] for warning in stats["warnings"]
                )
                model_distribution[str(stats["model"])] += 1
                stop_reason_distribution[str(stats["source_stop_reason"])] += 1
                tool_names.update(stats["tool_names"])
                services = stats["services"]
                if len(services) == 1:
                    sole_service[services[0]] += 1
        temporary.replace(output)
    finally:
        handles.close()
        temporary.unlink(missing_ok=True)

    converted = len(converted_stats)
    total_calls = sum(item["tool_calls"] for item in converted_stats)
    successful_calls = sum(item["successful_tool_calls"] for item in converted_stats)
    report.update(
        {
            "converted_records": converted,
            "rejected_records": len(selected) - converted,
            "conversion_rate_of_selected_rows": (
                converted / len(selected) if selected else 0.0
            ),
            "conversion_rate_of_input_rows": (
                converted / report["input_rows"] if report["input_rows"] else 0.0
            ),
            "rejection_reason_counts": dict(rejection_counts.most_common()),
            "rejection_examples": rejection_examples,
            "records_with_conversion_warnings": sum(
                bool(item["warning_count"]) for item in converted_stats
            ),
            "conversion_warning_counts": dict(warning_counts.most_common()),
            "model_distribution_converted": dict(model_distribution.most_common()),
            "source_stop_reason_distribution_converted": dict(
                stop_reason_distribution.most_common()
            ),
            "unique_tool_names": len(tool_names),
            "total_tool_calls": total_calls,
            "successful_tool_calls": successful_calls,
            "tool_success_rate": (
                successful_calls / total_calls if total_calls else 0.0
            ),
            "multi_service_trajectories": sum(
                len(item["services"]) >= 2 for item in converted_stats
            ),
            "single_service_trajectories_by_service": dict(
                sole_service.most_common()
            ),
            "change_or_correction_trajectories": sum(
                bool(item["has_change"]) for item in converted_stats
            ),
            "recovered_failure_trajectories": sum(
                bool(item["has_failed_recovered_call"])
                for item in converted_stats
            ),
            "status": "completed",
        }
    )
    if report_path is not None:
        write_report(report_path, report)
        report["report"] = str(report_path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert raw Anthropic JSONL files to normalized B-class JSONL."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="one or more raw JSONL/JSON files, or directories containing them",
    )
    parser.add_argument("-o", "--output", required=True, help="normalized output JSONL")
    parser.add_argument(
        "--report",
        help="optional JSON report path; omitted by default so only the output JSONL is created",
    )
    parser.add_argument(
        "--dedupe-session-id",
        action="store_true",
        help="optionally keep only the longest row per session_id (default: preserve every input row)",
    )
    parser.add_argument(
        "--max-result-summary-chars",
        type=int,
        default=0,
        help="maximum characters in top-level result_summary; full tool message is preserved (default: 0, unlimited)",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=30,
        help="maximum rejection examples in report (default: 30)",
    )
    parser.add_argument(
        "--max-open-files",
        type=int,
        default=512,
        help=(
            "cap on simultaneously open input files; large source directories are "
            "read through a bounded LRU so the FD limit is never exhausted "
            "(default: 512)"
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_result_summary_chars < 0 or args.max_examples < 0:
        print("numeric options must be non-negative", file=sys.stderr)
        return 2
    if args.max_open_files < 1:
        print("--max-open-files must be >= 1", file=sys.stderr)
        return 2
    try:
        report = convert_files(args)
    except (OSError, ValueError) as exc:
        print(f"convert_raw_anthropic_to_b.py: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
