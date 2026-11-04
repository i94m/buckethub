# 文件台账（file-list.tsv）· 唯一权威副本

全桶 `tvk-data-1012-a` / 前缀 `Gpt-2026-10-18/` 的 **30 个 key**，一行一个。
**从此只读这一份**：任何人不再自建副本，需要改状态就改这里并注明原因（commit message）。

数据来源：一次真实 `ListObjectsV2`（`prefix=Gpt-2026-10-18/`，30 objects / `791,029,691,821` bytes = 736.70 GiB）
+ HK 下发的全局 key 序（字典序）rank。

## 列

| 列 | 含义 |
| --- | --- |
| `rank` | 全局 key 序位 1..30（字典序），`order = rank × 2⁴⁰ + offset` 里的 rank 基 |
| `key` | 源对象 key（`Gpt-2026-10-18/` 前缀省略写全） |
| `bytes` | 对象字节数（ListObjectsV2 实测，非估算） |
| `owner` | 该 key 的唯一 owner 机（**每个 key 只有一个 owner**，不允许两家同时读） |
| `local_copy` | 整对象本地副本绝对路径；`-` = 该机未落盘 |
| `download_status` | `pending` / `downloading` / `partial N%` / `done` / `failed` |
| `index_status` | `pending` / `done` / `-`（未开始，因为副本还没落盘） |
| `delivery_status` | `pending` / `delivered` / `not_delivered`（文件边界停）/ `zero_kept` |

状态只写真值：`local_copy` 指向的文件**实际存在且字节数与 `bytes` 一致**才允许写 `done`。

## 四段切分（≈184 GiB/段，连续、字典序）

| 段 | owner | rank | key 数 | 体量 |
| --- | --- | --- | --- | --- |
| 1 | HK-202.6.205.37 | 1–7 | 7 | 183.80 GiB |
| 2 | Mac Mini M4 | 8–14 | 7 | 179.21 GiB |
| 3 | Macbook Pro | 15–22 | 8 | 183.38 GiB |
| 4 | Alienware | 23–30 | 8 | 190.31 GiB |

切分规则（与 leader 指令一致）：30 个 key 按字典序连续、每段 ≈ 总字节/4；**已经整对象落盘的 key 归落盘那台，不重下**；
其余按四段重新分配。边界由 `rank` 唯一定义，改边界必须在本表上改并注明原因。

> 本表 owner 列按上面的规则算出（HK 终版认领表贴出后以该表为准，届时在此改表并注明原因）。
> 四段制下与上一版三段草案的差异：HK 1–10 → 1–7；M4 11–21 → 8–14；Macbook Pro 22–30 → 15–22；
> 新第四段 23–30 归 Alienware。**已完整落盘的 key 不重下**，按落盘机归属优先。

## 交付单元（2026-11-04 起）：一个 key 一件

合并层与 `rank_base` 已退役，`order` 的全局归一不再需要。每个已完整落盘的 key 在本机就地走
`index → plan → emit → verify → pack`（同一 out-dir，`--local-dir` 零 egress），产出该 key 自己的
`<源文件>.jsonl(.zst)` + `delivery-manifest.json` + `README.md`。台账的 `delivery_status`
只在 `manifest.STOP_REASON=corpus_exhausted` 且 `verify ok=true` 时写 `delivered`。

## 机器本地根目录

| 机器 | 本地根目录 |
| --- | --- |
| Alienware | `/data/corpus-mirror/Gpt-2026-10-18/` |
| Mac Mini M4 | 待 owner 回填 |
| Macbook Pro | 待 owner 回填 |
| HK-202.6.205.37 | 待 owner 回填 |

## 改这张表的规矩

1. 只改自己 owner 的行；别人的行要改，先在 issue 里说清原因。
2. 每次改完在 commit message 里写「who / which rows / why」。
3. 下载状态从 `partial` 到 `done` 必须实测字节数；index 状态必须指向真实的 `index.jsonl` 行。
