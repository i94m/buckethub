"""数据集交付的独立验收工具（合成数据专用，零网络）。

对外的命令入口是 ``python -m accept.cli --help``；口径表与运行步骤见
``accept/README.md``。
"""

from __future__ import annotations

__all__ = ["cli", "dedup", "ledger", "reference", "shard", "synth", "tokens"]

__version__ = "0.1.0"
