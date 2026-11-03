"""token 计数单测：两套独立序列化必须逐字节一致，计数必须与锁定口径一致。

tiktoken 没装时，涉及真分词的用例走 skip；序列化/切片/记账相关的用例
不依赖 tiktoken，永远会跑。
"""

from __future__ import annotations

import json
import unittest

from accept import synth, tokens

TRICKY_VALUES = [
    None,
    True,
    False,
    0,
    -17,
    3.5,
    1e16,
    1.0,
    -0.0,
    "",
    "plain",
    'quote " and backslash \\',
    "换行\n回车\r制表\t退格\b换页\f",
    "\x00\x01\x1f\x7f",
    "中文内容，需要转义吗",
    "emoji 😀🙂‍↔️ 组合",
    "surrogate pair \U0001f600",
    [],
    {},
    [1, "two", None, {"k": [True, 2.5]}],
    {"b": 1, "a": 2, "c": {"z": [1, 2, 3], "y": "中文"}},
    {"10": "numeric-string-key", "2": "another"},
    [{"role": "assistant", "content": [{"type": "encrypted_content", "encrypted_content": "gAAAA"}]}],
]


def _tiktoken_available() -> bool:
    try:
        tokens.get_encoder(tokens.ENCODING_NAME)
        return True
    except tokens.TokenizerUnavailable:
        return False


TIKTOKEN_AVAILABLE = _tiktoken_available()


class SerializerEqualityTest(unittest.TestCase):
    def test_manual_serializer_matches_json_dumps(self):
        for value in TRICKY_VALUES:
            for sort_keys in (True, False):
                for ensure_ascii in (True, False):
                    with self.subTest(value=value, sort_keys=sort_keys, ensure_ascii=ensure_ascii):
                        expected = tokens.compact_json(
                            value, sort_keys=sort_keys, ensure_ascii=ensure_ascii
                        )
                        actual = tokens.compact_json_manual(
                            value, sort_keys=sort_keys, ensure_ascii=ensure_ascii
                        )
                        self.assertEqual(expected, actual)

    def test_manual_serializer_matches_on_synthetic_messages(self):
        for turns in (1, 3, 12):
            messages = synth.dialog(turns, prefix="SER")
            for message in messages:
                self.assertEqual(
                    tokens.compact_json(message), tokens.compact_json_manual(message)
                )

    def test_compact_and_roundtrip(self):
        message = synth.message("user", "内容 with \"quotes\"")
        self.assertEqual(json.loads(tokens.compact_json(message)), message)
        self.assertEqual(json.loads(tokens.compact_json_manual(message)), message)


class RawSliceTest(unittest.TestCase):
    def test_raw_slices_equal_messages(self):
        messages = synth.dialog(5, prefix="RAW")
        record = synth.record(session_uuid="raw", messages=messages, rs_id="rs_raw")
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertEqual(tokens.raw_message_texts(line), [
            json.dumps(message, ensure_ascii=False, separators=(",", ":"))
            for message in messages
        ])

    def test_raw_slices_survive_pretty_print(self):
        messages = synth.dialog(3, prefix="PRETTY")
        record = synth.record(session_uuid="pretty", messages=messages, rs_id="rs_pretty")
        line = json.dumps(record, ensure_ascii=False, indent=2).encode()
        sliced = tokens.raw_message_texts(line)
        self.assertEqual([json.loads(item) for item in sliced], messages)

    def test_raw_slices_handle_braces_inside_strings(self):
        message = synth.message("user", '文本里有 {"a": [1,2,3]} 和 ] } 字符')
        record = synth.record(session_uuid="brace", messages=[message], rs_id="rs_brace")
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertEqual(json.loads(tokens.raw_message_texts(line)[0]), message)

    def test_truncated_line_raises(self):
        record = synth.record(session_uuid="cut", messages=synth.dialog(3), rs_id="rs_cut")
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
        with self.assertRaises(tokens.RawScanError):
            tokens.raw_message_texts(line[:-20])


class FakeCounterTest(unittest.TestCase):
    def test_requires_explicit_allow(self):
        with self.assertRaises(tokens.TokenizerUnavailable):
            tokens.CharCounter()
        counter = tokens.CharCounter(allow=True)
        messages = synth.dialog(2)
        self.assertEqual(
            counter.count_messages(messages),
            sum(len(tokens.compact_json(message)) for message in messages),
        )


@unittest.skipUnless(TIKTOKEN_AVAILABLE, "本机没有 tiktoken/o200k_base，跳过真分词用例")
class TiktokenCountTest(unittest.TestCase):
    def test_two_serializers_give_identical_counts(self):
        """两套独立实现（json.dumps vs 手写）计数必须误差 0。"""
        for turns in (1, 5, 30):
            messages = synth.dialog(turns, prefix="CNT")
            left = tokens.count_messages(messages, serializer=tokens.compact_json)
            right = tokens.count_messages(messages, serializer=tokens.compact_json_manual)
            self.assertEqual(left, right)

    def test_count_equals_sum_of_individual_messages(self):
        messages = synth.dialog(9, prefix="SUM")
        expected = sum(tokens.count_text(tokens.compact_json(m)) for m in messages)
        self.assertEqual(tokens.count_messages(messages), expected)

    def test_encrypted_content_is_counted(self):
        plain = synth.dialog(8, prefix="ENC", encrypted_every=0)
        with_cipher = synth.dialog(8, prefix="ENC", encrypted_every=1)
        self.assertGreater(
            tokens.count_messages(with_cipher), tokens.count_messages(plain)
        )

    def test_raw_line_count_matches_reserialized_count(self):
        messages = synth.dialog(6, prefix="RAW2")
        record = synth.record(session_uuid="raw2", messages=messages, rs_id="rs_raw2")
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertEqual(tokens.count_raw_line(line), tokens.count_record(record))

    def test_variant_counts_expose_caliber_differences(self):
        record = synth.record(
            session_uuid="var",
            messages=[synth.message("user", "中文内容需要转义")],
            rs_id="rs_var",
        )
        counts = tokens.variant_counts(record)
        self.assertEqual(set(counts), set(tokens.SERIALIZER_VARIANTS) | {"raw_bytes_in_file"})
        self.assertNotEqual(
            counts["spec(compact,sort_keys,utf8)"], counts["compact,sort_keys,ascii"]
        )

    def test_empty_messages_count_zero(self):
        self.assertEqual(tokens.count_messages([]), 0)


if __name__ == "__main__":
    unittest.main()
