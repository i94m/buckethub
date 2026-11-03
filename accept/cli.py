#!/usr/bin/env python3
"""验收侧命令行入口。

**默认 dry-run**：不加 ``--execute`` 只打印"将要做什么"，不读交付物、不写文件、
不联网。加了 ``--execute`` 也只做本地读取与可选报告落盘——验收侧永远不下载、
不上传、不改 B2。

退出码：0 全部通过 / 1 验收不通过 / 2 参数或配置错误 / 3 分词器不可用。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence

from . import dedup, ledger, reference, shard, stopcheck, synth, tokens
from .ledger import Problem

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_CONFIG = 2
EXIT_TOKENIZER = 3


# --------------------------------------------------------------------------
# 公共
# --------------------------------------------------------------------------


class _Counter:
    """统一计数器接口：既能真算 tiktoken，也能是显式标注的占位实现。"""

    def __init__(self, *, fake: bool = False, allow_fake: bool = False) -> None:
        self.fake = fake
        if fake:
            self._impl = tokens.CharCounter(allow=allow_fake)
        else:
            self._impl = None
            tokens.get_encoder(tokens.ENCODING_NAME)  # 提前失败，别跑一半才发现

    def count_text(self, text: str) -> int:
        if self.fake:
            return self._impl.count_text(text)
        return tokens.count_text(text)

    def count_messages(self, messages: Sequence[Any]) -> int:
        if self.fake:
            return self._impl.count_messages(messages)
        return tokens.count_messages(messages)

    def meta(self) -> dict[str, Any]:
        if self.fake:
            return {
                "library": "chars(placeholder)",
                "encoding": "-",
                "method": "len(text)",
                "unit": "sum(per_message_compact_json)",
                "acceptance_valid": False,
            }
        return {
            "library": "tiktoken",
            "encoding": tokens.ENCODING_NAME,
            "method": tokens.METHOD_NAME,
            "unit": "sum(per_message_compact_json)",
            "includes_encrypted_content": True,
            "acceptance_valid": True,
        }


def _resolve_counter(args: argparse.Namespace) -> tuple[_Counter | None, int | None]:
    """返回 (计数器, 退出码)。计数器为 None 时退出码说明为什么不能用。"""
    fake = bool(getattr(args, "allow_fake_tokenizer", False)) or getattr(args, "tokenizer", "auto") == "chars"
    if fake and not getattr(args, "allow_fake_tokenizer", False):
        print(
            "[error] cli: 占位分词器(--tokenizer chars)必须同时给 --allow-fake-tokenizer",
            file=sys.stderr,
        )
        return None, EXIT_CONFIG
    try:
        return _Counter(fake=fake, allow_fake=True), None
    except tokens.TokenizerUnavailable as exc:
        print(f"[error] tokenizer: {exc}", file=sys.stderr)
        return None, EXIT_TOKENIZER


def _shard_config(args: argparse.Namespace, fake_tokens: bool) -> shard.ShardConfig:
    cfg = shard.ShardConfig(
        min_messages=args.min_messages,
        max_record_tokens=args.max_record_tokens,
        shard_max_bytes=int(args.shard_max_mb * shard.MIB),
        shard_slack=args.shard_slack,
        glob=args.shard_glob,
        allow_top_level_window_messages=bool(
            getattr(args, "allow_top_level_window_messages", False)
        ),
    )
    if fake_tokens:
        # 占位计数器下 token 阈值没有物理意义，放宽到字符量级避免误判。
        cfg.max_record_tokens = max(cfg.max_record_tokens, 10_000_000)
    return cfg


def _emit(report: dict[str, Any], *, as_json: bool, report_path: str | None) -> None:
    if report_path:
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(_human(report))


def _human(report: dict[str, Any]) -> str:
    lines = [f"验收结论：{'通过' if report.get('ok') else '不通过'}"]
    tokenizer = report.get("tokenizer", {})
    lines.append(
        f"分词器：{tokenizer.get('library')} / {tokenizer.get('encoding')} / {tokenizer.get('method')}"
        + ("" if tokenizer.get("acceptance_valid", True) else "  ⚠ 占位实现，不可用于验收")
    )
    delivery = report.get("delivery") or {}
    if delivery:
        lines.append(
            f"交付：{len(delivery.get('shards', []))} 个分片 / {delivery.get('records', 0)} 条 / "
            f"{delivery.get('bytes', 0)} 字节 / {delivery.get('tokens', 0)} token"
        )
        for item in delivery.get("shards", []):
            flag = "OK " if item.get("ok") else "ERR"
            lines.append(
                f"  [{flag}] {item['name']}: {item['records']} 条, {item['bytes']} 字节, "
                f"{item['tokens']} token, 最大单条 {item.get('max_record_tokens', 0)}, "
                f"切窗 {item.get('windowed_records', 0)}"
            )
    for name, result in (report.get("checks") or {}).items():
        mark = "OK " if result.get("ok") else "ERR"
        lines.append(f"  [{mark}] {name}: {result.get('detail', '')}")
    problems = report.get("problems") or []
    if problems:
        lines.append("问题清单：")
        for item in problems:
            lines.append(f"  - [{item['level']}] {item['where']}: {item['message']}")
    return "\n".join(lines)


def _problems_ok(problems: Sequence[Problem]) -> bool:
    return not any(p.level == "error" for p in problems)


# --------------------------------------------------------------------------
# verify
# --------------------------------------------------------------------------


def run_verify(
    delivery_dir: str | Path,
    *,
    manifest_path: str | Path | None,
    counter: _Counter,
    cfg: shard.ShardConfig,
    check_prefix_invariant: bool = True,
    required_steps: Sequence[str] = ledger.CORE_STEPS,
    allow_missing_steps: bool = False,
) -> dict[str, Any]:
    delivery_dir = Path(delivery_dir)
    problems: list[Problem] = []
    checks: dict[str, Any] = {}

    manifest_file = Path(manifest_path) if manifest_path else delivery_dir / "manifest.json"
    manifest: ledger.Manifest | None = None
    if not manifest_file.exists():
        problems.append(Problem("error", "manifest", f"找不到 manifest: {manifest_file}"))
        checks["reconcile"] = {"ok": False, "detail": "manifest 缺失"}
    else:
        manifest = ledger.load_manifest(manifest_file)
        reconcile_problems = ledger.reconcile(
            manifest, required_steps=required_steps, allow_missing_steps=allow_missing_steps
        )
        problems.extend(reconcile_problems)
        checks["reconcile"] = {
            "ok": _problems_ok(reconcile_problems),
            "detail": "逐文件恒等式 原始 = 移除 + 交付",
        }
        tokenizer_problems = [] if counter.fake else _check_tokenizer_meta(manifest.tokenizer)
        problems.extend(tokenizer_problems)
        checks["tokenizer_meta"] = {
            "ok": _problems_ok(tokenizer_problems),
            "detail": (
                "占位分词器模式：跳过 tokenizer 口径校验（仅验证记账管道）"
                if counter.fake
                else "manifest 里写的 tokenizer 口径与锁定口径一致"
            ),
        }

    delivery = shard.check_delivery(
        delivery_dir, cfg, counter=counter, check_prefix_invariant=check_prefix_invariant
    )
    problems.extend(delivery.all_problems())
    checks["shards"] = {
        "ok": delivery.ok,
        "detail": "逐行 json.loads / 末尾完整 / 体积 / 单条 token / rs_id / 密文 / 条数",
    }
    checks["prefix_invariant"] = {
        "ok": _problems_ok([p for p in delivery.problems if "前缀" in p.message]),
        "detail": "交付集内不存在前缀关系" if check_prefix_invariant else "已按参数跳过",
    }

    if manifest is not None:
        stop_problems = stopcheck.check_stop(manifest, delivery, delivery_dir)
        problems.extend(stop_problems)
        checks["stop_atomic"] = {
            "ok": _problems_ok(stop_problems),
            "detail": "到顶即停：整组窗口原子、停止点可追溯、超出量 ≤ 单条记录 token、未交付条数闭合",
        }

        cross_problems = _cross_check_shards(manifest, delivery)
        problems.extend(cross_problems)
        checks["shard_vs_manifest"] = {
            "ok": _problems_ok(cross_problems),
            "detail": "分片实际条数/字节/token 与 manifest 逐片一致",
        }

    ok = _problems_ok(problems) and all(check.get("ok") for check in checks.values())
    return {
        "ok": ok,
        "mode": "verify",
        "tokenizer": counter.meta(),
        "delivery": delivery.as_dict(),
        "checks": checks,
        "problems": [p.as_dict() for p in problems],
        "manifest": manifest.as_dict() if manifest is not None else None,
    }


def _check_tokenizer_meta(meta: dict[str, Any]) -> list[Problem]:
    problems: list[Problem] = []
    if not isinstance(meta, dict) or not meta:
        return [Problem("warn", "manifest.tokenizer", "manifest 没写 tokenizer 口径，无法确认与锁定口径一致")]
    encoding = str(meta.get("encoding") or meta.get("tokenizer") or "")
    method = str(meta.get("method") or meta.get("mode") or "")
    if encoding and encoding != tokens.ENCODING_NAME:
        problems.append(
            Problem(
                "error",
                "manifest.tokenizer",
                f"编码器是 {encoding!r}，锁定口径要求 {tokens.ENCODING_NAME}",
            )
        )
    if method and method != tokens.METHOD_NAME:
        problems.append(
            Problem(
                "error",
                "manifest.tokenizer",
                f"编码方法是 {method!r}，锁定口径要求 {tokens.METHOD_NAME}",
            )
        )
    return problems


def _cross_check_shards(manifest: ledger.Manifest, delivery: shard.DeliveryReport) -> list[Problem]:
    problems: list[Problem] = []
    actual = {item.name: item for item in delivery.shards}
    for declared in manifest.shards:
        found = actual.get(declared.name)
        if found is None:
            problems.append(Problem("error", "manifest.shards", f"manifest 里的分片 {declared.name} 实际不存在"))
            continue
        if declared.records and declared.records != found.records:
            problems.append(
                Problem(
                    "error",
                    declared.name,
                    f"manifest 记 {declared.records} 条，实际 {found.records} 条",
                )
            )
        if declared.bytes and declared.bytes != found.bytes:
            problems.append(
                Problem(
                    "error",
                    declared.name,
                    f"manifest 记 {declared.bytes} 字节，实际 {found.bytes} 字节",
                )
            )
        if declared.tokens and declared.tokens != found.tokens:
            problems.append(
                Problem(
                    "error",
                    declared.name,
                    f"token 复算不一致：manifest {declared.tokens}，复算 {found.tokens}"
                    f"（差 {found.tokens - declared.tokens:+d}）",
                )
            )
    declared_names = {item.name for item in manifest.shards}
    for name in actual:
        if name not in declared_names:
            problems.append(Problem("error", "manifest.shards", f"分片 {name} 没在 manifest 里登记"))
    return problems


# --------------------------------------------------------------------------
# recount
# --------------------------------------------------------------------------


def run_recount(
    delivery_dir: str | Path,
    *,
    counter: _Counter,
    cfg: shard.ShardConfig,
    diagnose: int = 0,
    sample: int = 200,
) -> dict[str, Any]:
    delivery = shard.check_delivery(delivery_dir, cfg, counter=counter, check_prefix_invariant=False)
    problems = list(delivery.all_problems())

    sample_problems: list[Problem] = []
    sampled = 0
    if sample and not counter.fake:
        sampled, sample_problems = _sample_recompute(
            shard.discover_shards(delivery_dir, cfg), counter, sample
        )
        problems.extend(sample_problems)

    diagnostics: list[dict[str, Any]] = []
    if diagnose:
        paths = shard.discover_shards(delivery_dir, cfg)
        for path in paths:
            with open(path, "rb") as handle:
                for lineno, raw in enumerate(handle, start=1):
                    if len(diagnostics) >= diagnose:
                        break
                    if not raw.strip():
                        continue
                    try:
                        record = json.loads(raw)
                    except Exception:
                        continue
                    if counter.fake:
                        continue
                    diagnostics.append(
                        {
                            "shard": path.name,
                            "line": lineno,
                            "variants": tokens.variant_counts(record, line=raw),
                        }
                    )
            if len(diagnostics) >= diagnose:
                break

    return {
        "ok": _problems_ok(problems) and delivery.ok,
        "mode": "recount",
        "tokenizer": counter.meta(),
        "delivery": delivery.as_dict(),
        "checks": {
            "recount": {
                "ok": delivery.ok,
                "detail": "逐条按锁定口径重算 token（误差要求 0）",
            },
            "sample_recompute": {
                "ok": _problems_ok(sample_problems),
                "detail": (
                    "占位分词器模式：跳过抽样复算"
                    if counter.fake
                    else f"抽样 {sampled} 条，两套独立实现误差为 0"
                ),
            }
        },
        "problems": [p.as_dict() for p in problems],
        "diagnostics": diagnostics,
    }


def _sample_recompute(
    paths: Sequence[Path], counter: _Counter, sample: int
) -> tuple[int, list[Problem]]:
    """均匀抽取 sample 条，用两套独立序列化各算一次，误差必须为 0。

    同时比较"文件里的原始字节"口径，用于发现交付方按别的序列化写的记录。
    """
    problems: list[Problem] = []
    total = 0
    for path in paths:
        with open(path, "rb") as handle:
            total += sum(1 for raw in handle if raw.strip())
    if not total:
        return 0, [Problem("error", "sample", "交付集里没有记录，无法抽样")]

    step = max(1, total // sample)
    sampled = 0
    index = 0
    for path in paths:
        with open(path, "rb") as handle:
            for lineno, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                take = index % step == 0
                index += 1
                if not take:
                    continue
                if sampled >= sample:
                    break
                try:
                    record = json.loads(raw)
                except Exception:
                    continue
                left = tokens.count_messages(dedup.get_messages(record))
                right = tokens.count_messages(
                    dedup.get_messages(record), serializer=tokens.compact_json_manual
                )
                sampled += 1
                if left != right:
                    problems.append(
                        Problem(
                            "error",
                            f"{path.name}:{lineno}",
                            f"两套独立实现计数不一致：json.dumps={left}，手写={right}",
                        )
                    )
                try:
                    raw_tokens = tokens.count_raw_line(raw)
                except Exception:
                    raw_tokens = -1
                if raw_tokens != left:
                    problems.append(
                        Problem(
                            "warn",
                            f"{path.name}:{lineno}",
                            f"文件里的原始字节口径 {raw_tokens} 与紧凑重排口径 {left} 不同，"
                            "说明交付记录不是紧凑 JSON（token 数会因此偏离口径）",
                        )
                    )
        if sampled >= sample:
            break
    return sampled, problems


# --------------------------------------------------------------------------
# fixture：把合成语料 + 参考交付写到目录
# --------------------------------------------------------------------------


def build_fixture(root: str | Path, *, counter: _Counter, cfg: shard.ShardConfig) -> dict[str, Any]:
    root = Path(root)
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    paths = synth.write_corpus(raw_dir)

    delivery_dir = root / "delivery"
    delivery_dir.mkdir(parents=True, exist_ok=True)
    ref_cfg = reference.ReferenceConfig(
        min_messages=cfg.min_messages,
        max_record_tokens=cfg.max_record_tokens,
        shard_max_bytes=cfg.shard_max_bytes,
    )
    manifest = reference.run_reference(
        paths, delivery_dir, cfg=ref_cfg, counter=counter, tokenizer_meta=counter.meta()
    )
    return {
        "root": str(root),
        "raw_dir": str(raw_dir),
        "delivery_dir": str(delivery_dir),
        "raw_files": [str(p) for p in paths],
        "manifest": manifest,
    }


# --------------------------------------------------------------------------
# selftest：在本机临时目录里跑"合成 → 参考交付 → 验收 → 破坏后必须报错"
# --------------------------------------------------------------------------


def run_selftest(*, counter: _Counter, cfg: shard.ShardConfig, keep: bool = False) -> dict[str, Any]:
    root = Path(tempfile.mkdtemp(prefix="buckethub-accept-"))
    results: list[dict[str, Any]] = []
    problems: list[Problem] = []
    try:
        fixture = build_fixture(root, counter=counter, cfg=cfg)
        delivery_dir = Path(fixture["delivery_dir"])

        good = run_verify(delivery_dir, manifest_path=None, counter=counter, cfg=cfg)
        results.append(
            {
                "case": "clean_delivery_passes",
                "expect": "通过",
                "ok": bool(good["ok"]),
                "problems": good["problems"],
            }
        )
        if not good["ok"]:
            problems.extend(Problem("error", "selftest", m["message"]) for m in good["problems"])

        cases = _selftest_mutations(delivery_dir, counter=counter, cfg=cfg, root=root)
        results.extend(cases)
        for case in cases:
            if not case["ok"]:
                problems.append(
                    Problem("error", f"selftest:{case['case']}", "破坏后的交付没有被验收脚本抓到")
                )

        return {
            "ok": _problems_ok(problems) and all(r["ok"] for r in results),
            "mode": "selftest",
            "tokenizer": counter.meta(),
            "results": results,
            "checks": {
                "selftest": {
                    "ok": all(r["ok"] for r in results),
                    "detail": "干净交付必须通过；每一类破坏必须被抓到",
                }
            },
            "problems": [p.as_dict() for p in problems],
        }
    finally:
        if keep:
            print(f"[selftest] 现场保留在 {root}", file=sys.stderr)
        else:
            shutil.rmtree(root, ignore_errors=True)


def _selftest_mutations(
    delivery_dir: Path, *, counter: _Counter, cfg: shard.ShardConfig, root: Path
) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []

    def _copy(name: str) -> Path:
        target = root / "mutations" / name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(delivery_dir, target)
        return target

    def _must_fail(name: str, expect_keyword: str, **kwargs: Any) -> None:
        target = _copy(name)
        kwargs["mutate"](target)
        report = run_verify(target, manifest_path=None, counter=counter, cfg=cfg)
        matched = any(expect_keyword in p["message"] for p in report["problems"])
        cases.append(
            {
                "case": name,
                "expect": f"报错且命中 {expect_keyword!r}",
                "ok": (not report["ok"]) and matched,
                "problems": report["problems"][:5],
            }
        )

    def _first_shard(path: Path) -> Path:
        return sorted(path.glob(cfg.glob))[0]

    def _drop_record(path: Path) -> None:
        target = _first_shard(path)
        lines = target.read_bytes().splitlines(keepends=True)
        target.write_bytes(b"".join(lines[1:]))

    def _duplicate_record(path: Path) -> None:
        target = _first_shard(path)
        lines = target.read_bytes().splitlines(keepends=True)
        target.write_bytes(b"".join(lines) + lines[0])

    def _truncate_tail(path: Path) -> None:
        target = _first_shard(path)
        data = target.read_bytes()
        target.write_bytes(data[:-40])

    def _break_reconcile(path: Path) -> None:
        manifest_file = path / "manifest.json"
        raw = json.loads(manifest_file.read_text(encoding="utf-8"))
        raw["files"][0]["revomd"] = raw["files"][0]["removed"]
        raw["files"][0].pop("removed")
        raw["files"][0]["removed"] = {"dedupe_prefix": 0}
        manifest_file.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")

    def _unknown_step(path: Path) -> None:
        manifest_file = path / "manifest.json"
        raw = json.loads(manifest_file.read_text(encoding="utf-8"))
        raw["files"][0]["removed"]["mystery_stage"] = 3
        manifest_file.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")

    def _token_mismatch(path: Path) -> None:
        manifest_file = path / "manifest.json"
        raw = json.loads(manifest_file.read_text(encoding="utf-8"))
        raw["shards"][0]["tokens"] = int(raw["shards"][0].get("tokens", 0)) + 7
        manifest_file.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")

    def _prefix_pollution(path: Path) -> None:
        """把某条记录的前缀版本硬塞进交付：交付集内出现前缀关系。"""
        target = _first_shard(path)
        first = json.loads(target.read_bytes().splitlines()[0])
        shortened = json.loads(json.dumps(first))
        messages = shortened["request"]["messages"]
        shortened["request"]["messages"] = messages[: max(1, len(messages) // 2)]
        shortened["response"]["response_data"]["output"][0]["id"] = "rs_pollute"
        with open(target, "ab") as handle:
            handle.write(json.dumps(shortened, ensure_ascii=False, separators=(",", ":")).encode())
            handle.write(b"\n")

    mutc = shard.ShardConfig(
        min_messages=cfg.min_messages,
        max_record_tokens=max(2000, cfg.max_record_tokens // 64),
        shard_max_bytes=cfg.shard_max_bytes,
        shard_slack=cfg.shard_slack,
        glob=cfg.glob,
    )

    def _oversize_record(path: Path) -> None:
        target = _first_shard(path)
        first = json.loads(target.read_bytes().splitlines()[0])
        first["request"]["messages"].append(
            {"role": "user", "content": [{"type": "input_text", "text": "x" * 40_000}]}
        )
        first["response"]["response_data"]["output"][0]["id"] = "rs_oversize"
        with open(target, "ab") as handle:
            handle.write(json.dumps(first, ensure_ascii=False, separators=(",", ":")).encode())
            handle.write(b"\n")

    def _missing_newline(path: Path) -> None:
        target = _first_shard(path)
        data = target.read_bytes()
        target.write_bytes(data.rstrip(b"\n"))

    _must_fail("shard_dropped_record", "实际", mutate=_drop_record)
    _must_fail("shard_duplicate_record", "完全重复", mutate=_duplicate_record)
    _must_fail("shard_truncated_tail", "json.loads", mutate=_truncate_tail)
    _must_fail("manifest_missing_step", "缺少移除步骤", mutate=_break_reconcile)
    _must_fail("manifest_unknown_step", "认不出的移除步骤", mutate=_unknown_step)
    _must_fail("manifest_token_mismatch", "token 复算不一致", mutate=_token_mismatch)
    _must_fail("delivery_prefix_pollution", "前缀", mutate=_prefix_pollution)

    # 单条超 token：用更严格的阈值单独跑一次
    target = _copy("record_over_token")
    _oversize_record(target)
    report = run_verify(target, manifest_path=None, counter=counter, cfg=mutc)
    cases.append(
        {
            "case": "record_over_token_limit",
            "expect": "报错（单条超 128k 未切窗）",
            "ok": any("token 超过上限" in p["message"] for p in report["problems"]),
            "problems": report["problems"][:5],
        }
    )

    target = _copy("missing_newline")
    _missing_newline(target)
    report = run_verify(target, manifest_path=None, counter=counter, cfg=cfg)
    cases.append(
        {
            "case": "shard_missing_trailing_newline",
            "expect": "报错（末尾行不完整）",
            "ok": any("末尾行" in p["message"] for p in report["problems"]),
            "problems": report["problems"][:5],
        }
    )

    return cases


# --------------------------------------------------------------------------
# 参数解析
# --------------------------------------------------------------------------


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--min-messages", type=int, default=100, help="「100 轮」= messages 条数 ≥ 该值")
    parser.add_argument("--max-record-tokens", type=int, default=128_000, help="单条 token 上限")
    parser.add_argument("--shard-max-mb", type=float, default=500.0, help="单包体积上限（MB）")
    parser.add_argument("--shard-slack", type=float, default=0.02, help="体积容差比例")
    parser.add_argument("--shard-glob", default="deliver-*.jsonl", help="分片文件名模式")
    parser.add_argument(
        "--tokenizer",
        choices=("auto", "tiktoken", "chars"),
        default="auto",
        help="auto/tiktoken = 真算；chars = 占位实现，必须配 --allow-fake-tokenizer",
    )
    parser.add_argument(
        "--allow-top-level-window-messages",
        action="store_true",
        help="放行「窗口内容写在顶层 messages」的切窗形态（默认判错，需双方确认口径后再开）",
    )
    parser.add_argument(
        "--allow-fake-tokenizer",
        action="store_true",
        help="允许占位分词器（只用于离线跑通管道，报告会标 acceptance_valid=false）",
    )


def _cmd_verify(args: argparse.Namespace) -> int:
    cfg = _shard_config(args, args.tokenizer == "chars" or args.allow_fake_tokenizer)
    if not args.execute:
        print("[dry-run] 未加 --execute：只打印计划，不读交付物、不写任何文件、不联网。")
        print(f"  交付目录: {args.delivery}")
        print(f"  manifest : {args.manifest or Path(args.delivery) / 'manifest.json'}")
        print(f"  分片模式 : {cfg.glob}，单包上限 {cfg.shard_max_bytes} 字节（容差 {cfg.shard_slack:.0%}）")
        print(f"  单条上限 : {cfg.max_record_tokens} token，messages ≥ {cfg.min_messages}")
        print("  将执行   : 逐文件对账 / 分片完整性 / token 复算 / 交付集前缀不变量 / 分片与 manifest 交叉核对")
        info = tokens.tokenizer_info()
        print(f"  分词器   : tiktoken {info['encoding']} 可用={info['available']}")
        print("  加 --execute 才会真的跑。")
        return EXIT_OK
    counter, exit_code = _resolve_counter(args)
    if counter is None:
        return int(exit_code or EXIT_TOKENIZER)
    report = run_verify(
        args.delivery,
        manifest_path=args.manifest,
        counter=counter,
        cfg=cfg,
        check_prefix_invariant=not args.skip_prefix_invariant,
        allow_missing_steps=args.allow_missing_steps,
    )
    _emit(report, as_json=args.json, report_path=args.report)
    return EXIT_OK if report["ok"] else EXIT_FAIL


def _cmd_recount(args: argparse.Namespace) -> int:
    cfg = _shard_config(args, args.tokenizer == "chars" or args.allow_fake_tokenizer)
    if not args.execute:
        print("[dry-run] 未加 --execute：不会读任何交付文件。")
        print(f"  将逐条重算 {args.delivery} 下 {cfg.glob} 的 token，误差要求 0")
        if args.diagnose:
            print(f"  并对前 {args.diagnose} 条输出 4 种序列化变体的计数，用于反推口径")
        return EXIT_OK
    counter, exit_code = _resolve_counter(args)
    if counter is None:
        return int(exit_code or EXIT_TOKENIZER)
    report = run_recount(
        args.delivery, counter=counter, cfg=cfg, diagnose=args.diagnose, sample=args.sample
    )
    _emit(report, as_json=args.json, report_path=args.report)
    return EXIT_OK if report["ok"] else EXIT_FAIL


def _cmd_fixture(args: argparse.Namespace) -> int:
    cfg = _shard_config(args, args.tokenizer == "chars" or args.allow_fake_tokenizer)
    if not args.execute:
        print("[dry-run] 未加 --execute：不会写任何文件。")
        print(f"  将在 {args.out} 下写合成语料 raw/part-NNNN.jsonl 和参考交付 delivery/")
        return EXIT_OK
    counter, exit_code = _resolve_counter(args)
    if counter is None:
        return int(exit_code or EXIT_TOKENIZER)
    fixture = build_fixture(args.out, counter=counter, cfg=cfg)
    print(json.dumps(fixture, ensure_ascii=False, indent=2))
    return EXIT_OK


def _cmd_selftest(args: argparse.Namespace) -> int:
    cfg = _shard_config(args, args.tokenizer == "chars" or args.allow_fake_tokenizer)
    if not args.execute:
        print("[dry-run] 未加 --execute：不会写任何文件。")
        print("  将在临时目录里：造合成语料 → 跑参考流水线 → 验收干净交付 → 8 类破坏逐一必须报错")
        return EXIT_OK
    counter, exit_code = _resolve_counter(args)
    if counter is None:
        return int(exit_code or EXIT_TOKENIZER)
    report = run_selftest(counter=counter, cfg=cfg, keep=args.keep)
    _emit(report, as_json=args.json, report_path=args.report)
    if not args.json:
        for case in report["results"]:
            print(f"  [{'PASS' if case['ok'] else 'FAIL'}] {case['case']}: {case['expect']}")
    return EXIT_OK if report["ok"] else EXIT_FAIL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m accept.cli",
        description="Buckethub 数据集交付的独立验收脚本（默认 dry-run，只跑本地合成数据）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--execute", action="store_true", help="真正执行（默认只打印计划）")
    common.add_argument("--json", action="store_true", help="输出机器可读 JSON")
    common.add_argument("--report", default=None, help="把报告写到该路径（需 --execute）")

    verify = sub.add_parser("verify", parents=[common], help="全量验收：对账 + 分片完整性 + token 复算")
    verify.add_argument("--delivery", required=True, help="交付目录（含 manifest.json 和 deliver-*.jsonl）")
    verify.add_argument("--manifest", default=None, help="manifest 路径，默认 <delivery>/manifest.json")
    verify.add_argument("--skip-prefix-invariant", action="store_true", help="跳过交付集前缀不变量")
    verify.add_argument("--allow-missing-steps", action="store_true", help="允许 manifest 缺某一步的记账")
    _add_config_args(verify)
    verify.set_defaults(func=_cmd_verify)

    recount = sub.add_parser("recount", parents=[common], help="只做 token 复算（误差要求 0）")
    recount.add_argument("--delivery", required=True)
    recount.add_argument("--diagnose", type=int, default=0, help="对前 N 条输出 4 种序列化变体计数")
    recount.add_argument("--sample", type=int, default=200, help="抽样复算条数（0 = 关闭）")
    _add_config_args(recount)
    recount.set_defaults(func=_cmd_recount)

    fixture = sub.add_parser("fixture", parents=[common], help="生成合成语料 + 参考交付（仅本地）")
    fixture.add_argument("--out", required=True)
    _add_config_args(fixture)
    fixture.set_defaults(func=_cmd_fixture)

    selftest = sub.add_parser("selftest", parents=[common], help="自检：合成 → 交付 → 验收 → 破坏必须报错")
    selftest.add_argument("--keep", action="store_true", help="保留临时目录")
    _add_config_args(selftest)
    selftest.set_defaults(func=_cmd_selftest)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("[abort] 用户中断", file=sys.stderr)
        return EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
