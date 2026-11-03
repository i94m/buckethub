#!/usr/bin/env python3
"""验收侧独立实现的 token 计数 + 序列化交叉校验。

锁定口径：tiktoken ``o200k_base`` / ``encode_ordinary``，按每条 message 的紧凑
JSON 单独计数后求和，密文（``encrypted_content``）计入。

这里刻意写了两套互相独立的序列化实现：

* ``compact_json``      —— 用标准库 ``json.dumps``；
* ``compact_json_manual`` —— 手写递归序列化，自己处理排序、转义、浮点。

两者对任意 JSON 必须逐字节相同，否则 token 数没有可比性（这是最容易同源出错
的地方）。真实计数只认 tiktoken；tiktoken 不可用时不会静默降级成别的分词器，
而是直接报错，避免"看起来通过了"。

另有两个辅助能力，专供对账排错：

* ``variant_counts`` —— 同时算 4 种序列化变体，用于反推对方用了哪种口径；
* ``raw_message_texts`` —— 从交付行的**原始字节**里切出每条 message 的原文，
  完全不重新序列化，得到"文件里到底是什么"的计数。
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Iterable, Sequence

ENCODING_NAME = "o200k_base"
METHOD_NAME = "encode_ordinary"

#: 单条记录允许的最大 token 数；超过必须已切窗（锁定口径 #6）。
MAX_RECORD_TOKENS = 128_000


class TokenizerUnavailable(RuntimeError):
    """tiktoken / o200k_base 不可用——验收不能降级，必须显式失败。"""


# --------------------------------------------------------------------------
# tiktoken 入口
# --------------------------------------------------------------------------


@lru_cache(maxsize=4)
def get_encoder(encoding: str = ENCODING_NAME):
    try:
        import tiktoken  # type: ignore
    except Exception as exc:  # pragma: no cover - 取决于运行环境
        raise TokenizerUnavailable(
            "缺少 tiktoken：验收要求 tiktoken "
            f"{encoding} / {METHOD_NAME}，不接受其它分词器。"
            "请先离线装好（uv pip install tiktoken）并保证 BPE 文件已在 "
            "TIKTOKEN_CACHE_DIR 里，再重跑。原始错误：" + repr(exc)
        ) from exc
    try:
        return tiktoken.get_encoding(encoding)
    except Exception as exc:  # pragma: no cover - 取决于缓存/网络
        raise TokenizerUnavailable(
            f"tiktoken 无法加载 {encoding}（通常是 BPE 文件没缓存）。"
            "设置 TIKTOKEN_CACHE_DIR 指向已缓存的目录后重跑；"
            "验收侧不发网络请求。原始错误：" + repr(exc)
        ) from exc


def encode_ordinary(text: str, *, encoding: str = ENCODING_NAME) -> list[int]:
    """锁定口径的编码入口：``encode_ordinary``（特殊 token 按普通文本处理）。"""
    return get_encoder(encoding).encode_ordinary(text)


def count_text(text: str, *, encoding: str = ENCODING_NAME) -> int:
    return len(encode_ordinary(text, encoding=encoding))


def tokenizer_info() -> dict[str, Any]:
    """报告里要写清的分词器口径（含是否真的可用）。"""
    info: dict[str, Any] = {
        "library": "tiktoken",
        "encoding": ENCODING_NAME,
        "method": METHOD_NAME,
        "unit": "sum(per_message_compact_json)",
        "includes_encrypted_content": True,
        "available": False,
    }
    try:
        get_encoder(ENCODING_NAME)
        info["available"] = True
    except TokenizerUnavailable as exc:
        info["error"] = str(exc)
    return info


# --------------------------------------------------------------------------
# 两套独立序列化
# --------------------------------------------------------------------------

_ESCAPES = {
    '"': '\\"',
    "\\": "\\\\",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
    "\b": "\\b",
    "\f": "\\f",
}


def compact_json(obj: Any, *, sort_keys: bool = True, ensure_ascii: bool = False) -> str:
    """实现 A：标准库 ``json.dumps`` 的紧凑形式。"""
    return json.dumps(obj, sort_keys=sort_keys, separators=(",", ":"), ensure_ascii=ensure_ascii)


def _float_repr(value: float) -> str:
    import math

    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return float.__repr__(value)


def _escape_string(text: str, ensure_ascii: bool) -> str:
    out = ['"']
    for ch in text:
        code = ord(ch)
        escaped = _ESCAPES.get(ch)
        if escaped is not None:
            out.append(escaped)
        elif code < 0x20:
            out.append(f"\\u{code:04x}")
        elif ensure_ascii and code > 0x7E:
            if code <= 0xFFFF:
                out.append(f"\\u{code:04x}")
            else:
                code -= 0x10000
                out.append(f"\\u{0xD800 + (code >> 10):04x}\\u{0xDC00 + (code & 0x3FF):04x}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _key_str(key: Any) -> str:
    if isinstance(key, str):
        return key
    if key is True:
        return "true"
    if key is False:
        return "false"
    if key is None:
        return "null"
    if isinstance(key, int):
        return int.__repr__(key)
    if isinstance(key, float):
        return _float_repr(key)
    return str(key)


def _dump(obj: Any, sort_keys: bool, ensure_ascii: bool, out: list[str]) -> None:
    if obj is None:
        out.append("null")
    elif obj is True:
        out.append("true")
    elif obj is False:
        out.append("false")
    elif isinstance(obj, str):
        out.append(_escape_string(obj, ensure_ascii))
    elif isinstance(obj, int):
        out.append(int.__repr__(obj))
    elif isinstance(obj, float):
        out.append(_float_repr(obj))
    elif isinstance(obj, dict):
        items = list(obj.items())
        if sort_keys:
            items.sort(key=lambda kv: _key_str(kv[0]))
        out.append("{")
        for pos, (key, value) in enumerate(items):
            if pos:
                out.append(",")
            out.append(_escape_string(_key_str(key), ensure_ascii))
            out.append(":")
            _dump(value, sort_keys, ensure_ascii, out)
        out.append("}")
    elif isinstance(obj, (list, tuple)):
        out.append("[")
        for pos, value in enumerate(obj):
            if pos:
                out.append(",")
            _dump(value, sort_keys, ensure_ascii, out)
        out.append("]")
    else:  # pragma: no cover - 非 JSON 类型
        raise TypeError(f"不支持序列化的类型: {type(obj)!r}")


def compact_json_manual(obj: Any, *, sort_keys: bool = True, ensure_ascii: bool = False) -> str:
    """实现 B：手写序列化，用于和实现 A 逐字节互验。"""
    out: list[str] = []
    _dump(obj, sort_keys, ensure_ascii, out)
    return "".join(out)


SERIALIZER_VARIANTS = {
    # 锁定口径本身
    "spec(compact,sort_keys,utf8)": dict(sort_keys=True, ensure_ascii=False),
    # 常见的实现偏差，用于反推对方口径
    "compact,sort_keys,ascii": dict(sort_keys=True, ensure_ascii=True),
    "compact,no_sort,utf8": dict(sort_keys=False, ensure_ascii=False),
    "compact,no_sort,ascii": dict(sort_keys=False, ensure_ascii=True),
}


# --------------------------------------------------------------------------
# 计数
# --------------------------------------------------------------------------


def message_texts(messages: Iterable[Any], *, serializer=compact_json) -> list[str]:
    return [serializer(message) for message in messages]


def count_messages(
    messages: Sequence[Any],
    *,
    serializer=compact_json,
    encoding: str = ENCODING_NAME,
) -> int:
    """按每条 message 的紧凑 JSON 单独计数后求和（锁定口径 #5）。"""
    encoder = get_encoder(encoding)
    total = 0
    for message in messages:
        total += len(encoder.encode_ordinary(serializer(message)))
    return total


def get_messages(record: Any) -> list:
    if not isinstance(record, dict):
        return []
    request = record.get("request")
    if not isinstance(request, dict):
        return []
    messages = request.get("messages")
    return messages if isinstance(messages, list) else []


def count_record(record: Any, *, encoding: str = ENCODING_NAME) -> int:
    return count_messages(get_messages(record), encoding=encoding)


def variant_counts(record: Any, *, encoding: str = ENCODING_NAME) -> dict[str, int]:
    """同一份 messages 在 4 种序列化口径下的 token 数，用于口径反推。"""
    messages = get_messages(record)
    result: dict[str, int] = {}
    for name, kwargs in SERIALIZER_VARIANTS.items():
        result[name] = count_messages(
            messages,
            serializer=lambda m, _kw=kwargs: compact_json(m, **_kw),
            encoding=encoding,
        )
    result["raw_bytes_in_file"] = count_messages(
        raw_message_texts(record), encoding=encoding
    )
    return result


# --------------------------------------------------------------------------
# 从原始字节里切 messages（不重新序列化）
# --------------------------------------------------------------------------


class RawScanError(ValueError):
    pass


def _skip_ws(buf: bytes, pos: int, end: int) -> int:
    while pos < end and buf[pos] in b" \t\r\n":
        pos += 1
    return pos


def _scan_string(buf: bytes, pos: int, end: int) -> int:
    """pos 指向起始引号，返回结束引号之后的位置。"""
    pos += 1
    while pos < end:
        ch = buf[pos]
        if ch == 0x5C:  # backslash
            pos += 2
            continue
        if ch == 0x22:  # quote
            return pos + 1
        pos += 1
    raise RawScanError("字符串未闭合（文件可能被截断）")


def _scan_value(buf: bytes, pos: int, end: int) -> int:
    ch = buf[pos]
    if ch == 0x22:
        return _scan_string(buf, pos, end)
    if ch in b"{[":
        depth = 0
        while pos < end:
            ch = buf[pos]
            if ch == 0x22:
                pos = _scan_string(buf, pos, end)
                continue
            if ch in b"{[":
                depth += 1
            elif ch in b"}]":
                depth -= 1
                if depth == 0:
                    return pos + 1
            pos += 1
        raise RawScanError("括号未闭合（文件可能被截断）")
    while pos < end and buf[pos] not in b",}] \t\r\n":
        pos += 1
    return pos


def object_key_spans(buf: bytes, start: int, end: int) -> dict[str, tuple[int, int]]:
    """解析 [start, end) 的 JSON 对象，返回 key → (值起始, 值结束) 的字节区间。"""
    pos = _skip_ws(buf, start, end)
    if pos >= end or buf[pos] != 0x7B:
        raise RawScanError(f"位置 {start} 不是 JSON 对象")
    pos += 1
    spans: dict[str, tuple[int, int]] = {}
    while True:
        pos = _skip_ws(buf, pos, end)
        if pos >= end:
            raise RawScanError("对象未闭合")
        if buf[pos] == 0x7D:
            return spans
        if buf[pos] != 0x22:
            raise RawScanError(f"位置 {pos} 期望 key")
        key_end = _scan_string(buf, pos, end)
        key = json.loads(buf[pos:key_end].decode("utf-8"))
        pos = _skip_ws(buf, key_end, end)
        if pos >= end or buf[pos] != 0x3A:  # ':'
            raise RawScanError(f"位置 {pos} 期望 ':'")
        pos = _skip_ws(buf, pos + 1, end)
        value_end = _scan_value(buf, pos, end)
        spans[key] = (pos, value_end)
        pos = _skip_ws(buf, value_end, end)
        if pos < end and buf[pos] == 0x2C:  # ','
            pos += 1
            continue
        if pos < end and buf[pos] == 0x7D:
            return spans
        raise RawScanError(f"位置 {pos} 期望 ',' 或 '}}'")


def array_element_spans(buf: bytes, start: int, end: int) -> list[tuple[int, int]]:
    """解析 [start, end) 的 JSON 数组，返回每个元素的值字节区间。"""
    pos = _skip_ws(buf, start, end)
    if pos >= end or buf[pos] != 0x5B:
        raise RawScanError(f"位置 {start} 不是 JSON 数组")
    pos += 1
    spans: list[tuple[int, int]] = []
    while True:
        pos = _skip_ws(buf, pos, end)
        if pos >= end:
            raise RawScanError("数组未闭合")
        if buf[pos] == 0x5D:
            return spans
        value_end = _scan_value(buf, pos, end)
        spans.append((pos, value_end))
        pos = _skip_ws(buf, value_end, end)
        if pos < end and buf[pos] == 0x2C:
            pos += 1
            continue
        if pos < end and buf[pos] == 0x5D:
            return spans
        raise RawScanError(f"位置 {pos} 期望 ',' 或 ']'")


def raw_messages_array_span(line: bytes) -> tuple[int, int]:
    """从交付行原始字节里定位 ``request.messages`` 数组的区间。"""
    top = object_key_spans(line, 0, len(line))
    if "request" not in top:
        raise RawScanError("缺少 request")
    req_start, req_end = top["request"]
    request_spans = object_key_spans(line, req_start, req_end)
    if "messages" not in request_spans:
        raise RawScanError("request 里缺少 messages")
    return request_spans["messages"]


def raw_message_texts(line: bytes) -> list[str]:
    """交付行里每条 message 的**原文**（不重新序列化）。"""
    start, end = raw_messages_array_span(line)
    return [
        line[span_start:span_end].decode("utf-8")
        for span_start, span_end in array_element_spans(line, start, end)
    ]


def count_raw_line(line: bytes, *, encoding: str = ENCODING_NAME) -> int:
    encoder = get_encoder(encoding)
    return sum(len(encoder.encode_ordinary(text)) for text in raw_message_texts(line))


# --------------------------------------------------------------------------
# 测试用占位分词器（绝不用于验收）
# --------------------------------------------------------------------------


class CharCounter:
    """仅用于离线跑通"记账管道"的占位计数器：token 数 = 字符数。

    ``allow=True`` 才能构造，且报告里必须打上 ``acceptance_valid=false``。
    """

    name = "chars(placeholder)"

    def __init__(self, *, allow: bool = False) -> None:
        if not allow:
            raise TokenizerUnavailable(
                "CharCounter 是占位计数器，不能用于验收；确需跑通管道请显式 "
                "allow=True 并在报告里标注 acceptance_valid=false"
            )

    def count_text(self, text: str) -> int:
        return len(text)

    def count_messages(self, messages: Sequence[Any]) -> int:
        return sum(len(compact_json(message)) for message in messages)

    def count_record(self, record: Any) -> int:
        return self.count_messages(get_messages(record))
