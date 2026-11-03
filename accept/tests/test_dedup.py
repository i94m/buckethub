"""去重逻辑单测：全部跑本地合成数据，不联网、不读真实数据。

覆盖锁定口径点名的五类边界 + 一个负例，另外补了 session 归一化、确定性、
规模（O(n log n) 不能退化成两两比较）等容易踩的坑。
"""

from __future__ import annotations

import time
import unittest

from accept import dedup, synth


class MemberIdentityTest(unittest.TestCase):
    def test_key_order_does_not_matter(self):
        left = {"role": "user", "content": [{"type": "text", "text": "hi"}]}
        right = {"content": [{"type": "text", "text": "hi"}], "role": "user"}
        self.assertEqual(
            dedup.canonical_member_bytes(left), dedup.canonical_member_bytes(right)
        )

    def test_content_order_matters(self):
        left = {"role": "user", "content": [{"text": "a", "type": "text"}, {"text": "b", "type": "text"}]}
        right = {"role": "user", "content": [{"text": "b", "type": "text"}, {"text": "a", "type": "text"}]}
        self.assertNotEqual(
            dedup.canonical_member_bytes(left), dedup.canonical_member_bytes(right)
        )

    def test_encrypted_part_participates(self):
        plain = {"role": "assistant", "content": [{"type": "output_text", "text": "x"}]}
        cipher = {
            "role": "assistant",
            "content": [
                {"type": "output_text", "text": "x"},
                {"type": "encrypted_content", "encrypted_content": "gAAAA"},
            ],
        }
        self.assertNotEqual(
            dedup.canonical_member_bytes(plain), dedup.canonical_member_bytes(cipher)
        )

    def test_session_group_key(self):
        self.assertEqual(dedup.session_group_key("ABCD~u1"), "abcd")
        self.assertEqual(dedup.session_group_key("  abc-123 ~ u9 "), "abc-123")
        self.assertEqual(dedup.session_group_key("no-tilde"), "no-tilde")
        self.assertEqual(dedup.session_group_key(""), "")
        self.assertEqual(dedup.session_group_key(None), "")  # type: ignore[arg-type]


class PrefixBoundaryTest(unittest.TestCase):
    """锁定口径点名的边界，逐条断言，期望值是手写的、不由被测实现推导。"""

    def _run(self, name: str) -> list[int]:
        case = synth.dedupe_case(name)
        result = dedup.dedupe_funnel(case["records"])
        return result.kept_indices

    def test_abcde_vs_abcdefg(self):
        self.assertEqual(self._run("abcde_vs_abcdefg"), [1])

    def test_identical(self):
        self.assertEqual(self._run("identical"), [0])

    def test_same_length_different_content(self):
        self.assertEqual(self._run("same_length_diff_content"), [0, 1])

    def test_cross_session_prefix(self):
        self.assertEqual(self._run("cross_session_prefix"), [1])

    def test_same_session_prefix_caught_by_session_step(self):
        case = synth.dedupe_case("same_session_prefix")
        result = dedup.dedupe_funnel(case["records"])
        self.assertEqual(result.kept_indices, [1])
        self.assertEqual(result.session.removed, {0: "session_shorter_snapshot"})
        self.assertEqual(result.prefix.removed, {})

    def test_empty_messages_is_prefix_of_everything(self):
        self.assertEqual(self._run("empty_messages"), [1])

    def test_all_empty_messages_keeps_one(self):
        self.assertEqual(self._run("all_empty_messages"), [0])

    def test_shared_prefix_k_is_not_prefix(self):
        """负例：前 k 个成员相同后分叉，不是前缀关系，必须都留下。"""
        self.assertEqual(self._run("shared_prefix_k_negative"), [0, 1])


class PrefixChainTest(unittest.TestCase):
    def test_three_level_chain_keeps_longest(self):
        short = synth.record(
            session_uuid="aaaa", messages=synth.dialog(3, prefix="X"), rs_id="rs_1"
        )
        mid = synth.record(
            session_uuid="bbbb", messages=synth.dialog(5, prefix="X"), rs_id="rs_2"
        )
        long = synth.record(
            session_uuid="cccc", messages=synth.dialog(9, prefix="X"), rs_id="rs_3"
        )
        result = dedup.dedupe_funnel([short, mid, long])
        self.assertEqual(result.kept_indices, [2])
        self.assertEqual(
            result.prefix.reason_counts(), {"prefix_of_longer": 2}
        )

    def test_shuffled_input_same_result(self):
        records = [
            synth.record(session_uuid=f"u{i}", messages=synth.dialog(n, prefix="Y"), rs_id=f"rs_{i}")
            for i, n in enumerate([12, 3, 9, 5, 7])
        ]
        forward = dedup.dedupe_funnel(records).kept_indices
        backward = dedup.dedupe_funnel(list(reversed(records))).kept_indices
        self.assertEqual([records[i]["session_id"] for i in forward], [
            list(reversed(records))[i]["session_id"] for i in backward
        ])

    def test_identical_keeps_lowest_index(self):
        base = synth.dialog(4, prefix="Z")
        records = [
            synth.record(session_uuid=f"s{i}", messages=base, rs_id=f"rs_{i}") for i in range(4)
        ]
        result = dedup.dedupe_funnel(records)
        self.assertEqual(result.kept_indices, [0])


class SessionDedupeTest(unittest.TestCase):
    def test_keeps_longest_per_uuid(self):
        records = [
            synth.record(session_uuid="same-uuid", user_hash="a", messages=synth.dialog(3), rs_id="rs_1"),
            synth.record(session_uuid="same-uuid", user_hash="b", messages=synth.dialog(9), rs_id="rs_2"),
            synth.record(session_uuid="same-uuid", user_hash="c", messages=synth.dialog(5), rs_id="rs_3"),
            synth.record(session_uuid="other-uuid", user_hash="d", messages=synth.dialog(2), rs_id="rs_4"),
        ]
        result = dedup.dedupe_session(records)
        self.assertEqual(result.kept, [1, 3])
        self.assertEqual(result.removed_count, 2)

    def test_missing_session_id_never_removed(self):
        records = [
            synth.record(session_uuid="", user_hash="a", messages=synth.dialog(3), rs_id="rs_1"),
            synth.record(session_uuid="", user_hash="b", messages=synth.dialog(3), rs_id="rs_2"),
        ]
        for record in records:
            record["session_id"] = ""
        result = dedup.dedupe_session(records)
        self.assertEqual(result.kept, [0, 1])

    def test_tie_break_is_deterministic(self):
        records = [
            synth.record(session_uuid="tie", user_hash="a", messages=synth.dialog(4, prefix="T1"), rs_id="a"),
            synth.record(session_uuid="tie", user_hash="b", messages=synth.dialog(4, prefix="T2"), rs_id="b"),
        ]
        first = dedup.dedupe_session(records).kept
        second = dedup.dedupe_session(list(reversed(records))).kept
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)


class ViolationScanTest(unittest.TestCase):
    def test_clean_set_has_no_violation(self):
        records = [
            synth.record(session_uuid="a", messages=synth.dialog(5, prefix="V"), rs_id="rs_a"),
            synth.record(session_uuid="b", messages=synth.dialog(5, prefix="W"), rs_id="rs_b"),
        ]
        self.assertEqual(dedup.find_prefix_violations(records), [])

    def test_prefix_and_identical_are_reported(self):
        longer = synth.dialog(6, prefix="V")
        records = [
            synth.record(session_uuid="a", messages=longer, rs_id="rs_a"),
            synth.record(session_uuid="b", messages=longer[:3], rs_id="rs_b"),
            synth.record(session_uuid="c", messages=longer, rs_id="rs_c"),
            synth.record(session_uuid="d", messages=synth.dialog(6, prefix="W"), rs_id="rs_d"),
        ]
        violations = dedup.find_prefix_violations(records)
        reasons = sorted(v.reason for v in violations)
        self.assertEqual(reasons, ["identical_duplicate", "prefix_of_longer"])


class ScaleTest(unittest.TestCase):
    def test_no_quadratic_blowup(self):
        """8000 条记录必须在秒级完成；写成 O(n²) 这里是分钟级。"""
        base = synth.dialog(20, prefix="P")
        records = []
        for index in range(8000):
            messages = base if index % 4 else base[: 3 + index % 7]
            records.append(
                synth.record(session_uuid=f"s{index}", messages=list(messages), rs_id=f"rs_{index}")
            )
        start = time.perf_counter()
        result = dedup.dedupe_funnel(records)
        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 10.0, f"去重耗时 {elapsed:.1f}s，疑似退化成两两比较")
        self.assertGreater(len(result.kept_indices), 0)


if __name__ == "__main__":
    unittest.main()


class SessionTieBreakTest(unittest.TestCase):
    """并列全序：messages 最长 → timestamp 较新 → 原下标较小。

    开发侧与验收侧必须是同一份规则，否则 1B 规模下 crosscheck 会持续误报。
    """

    def _rec(self, ts: str, seed: str, turns: int = 100) -> dict:
        return synth.record(
            session_uuid="tie",
            messages=synth.dialog(turns, prefix=seed),
            user_hash=f"u{seed}",
            rs_id=f"rs_{seed}",
            timestamp=ts,
        )

    def test_same_length_prefers_newer_timestamp(self):
        records = [self._rec("2026-01-01T00:00:00Z", "aaa"),
                   self._rec("2026-02-01T00:00:00Z", "bbb")]
        result = dedup.dedupe_session(records)
        self.assertEqual(result.kept, [1], "同长度必须选 timestamp 较新的那条")

    def test_sequence_hash_must_not_decide(self):
        # 这组数据上"序列指纹"排序会选到 timestamp 更旧的那条（旧实现就是这么错的）
        records = [self._rec("2026-01-01T00:00:00Z", "AAA"),
                   self._rec("2026-02-01T00:00:00Z", "DDD")]
        self.assertEqual(dedup.dedupe_session(records).kept, [1],
                         "并列的第二关键字是 timestamp，不是序列指纹")

    def test_full_tie_keeps_lower_index(self):
        records = [self._rec("2026-01-01T00:00:00Z", "aaa"),
                   self._rec("2026-01-01T00:00:00Z", "bbb")]
        result = dedup.dedupe_session(records)
        self.assertEqual(result.kept, [0], "同长同时间必须取原下标小的那条")

    def test_longer_wins_over_newer(self):
        records = [self._rec("2026-01-01T00:00:00Z", "aaa", turns=120),
                   self._rec("2026-12-01T00:00:00Z", "bbb", turns=100)]
        result = dedup.dedupe_session(records)
        self.assertEqual(result.kept, [0])

    def test_tie_is_deterministic(self):
        records = [self._rec("2026-02-01T00:00:00Z", "aaa"),
                   self._rec("2026-02-01T00:00:00Z", "bbb"),
                   self._rec("2026-02-01T00:00:00Z", "ccc")]
        first = dedup.dedupe_session(records).kept
        second = dedup.dedupe_session(list(records)).kept
        self.assertEqual(first, second)
        self.assertEqual(first, [0])
