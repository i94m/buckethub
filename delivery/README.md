# 交付打包 + 上传（`delivery/delivery_pack.py`）

把 `pipeline/corpus_pipeline.py` 的交付目录（`--out-dir`）压成 `<分片名>.jsonl.zst`，
生成 `delivery-manifest.json` + `README.md`，并上传到桶内一个**全新日期目录**。

- **只读**交付目录：不改 `pipeline/`、`accept/` 一行，也不改交付目录里的任何文件。
- 默认 **dry-run**：不压缩、不写盘、不连网，只打印计划。
- 凭证只从环境变量读：`B2_KEY_ID` / `B2_APP_KEY`（兼容 `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY`）；不进命令行、不落盘、不回显。
- 源对象只 `GET`/`HEAD`，**永不删除、永不覆盖**。

## 一、T14 收尾时的确切命令

```bash
# 0) 依赖（只需要 zstd；python 包缺失会自动退回 zstd CLI）
uv pip install boto3 zstandard tiktoken      # 或 pip install ...

# 1) 先看计划（不连网、不写盘，确认分片数/行数/token 对得上）
python delivery/delivery_pack.py <OUT>/full --date 2026-11-04

# 2) 打包（不连网）：产物落在 <OUT>/full/_delivery_pack/
python delivery/delivery_pack.py <OUT>/full --execute --phase pack --date 2026-11-04

# 3) 上传 + 校验（凭证只走环境变量）
export B2_KEY_ID=...
export B2_APP_KEY=...
python delivery/delivery_pack.py <OUT>/full --execute --date 2026-11-04
```

一条命令走完（打包 + 上传 + 校验）：

```bash
export B2_KEY_ID=... B2_APP_KEY=...
python delivery/delivery_pack.py <OUT>/full --execute --date 2026-11-04
```

> `--date` 建议显式写死交付日期（默认取本机 UTC 当天）。目录被占用时自动换成
> `2026-11-04-delivery-1/`、`-delivery-2/` …

## 二、产物

```
<OUT>/full/_delivery_pack/       # 暂存目录（--staging-dir 可改到别处）
  gpt-6-astra-0009-20261001.part-0001.jsonl.zst   # 分片名 + .zst
  ...
  delivery-manifest.json         # 清单（每文件/每片的字节数、两份 sha256、条数、token、状态）
  README.md                      # A1 口径说明 + 文件清单 + 复算命令
  pack-state.json                # 压缩缓存（续跑不重压）
  upload.journal.jsonl           # 上传流水（哪些对象开始传/传完/跳过/冲突）
  upload-report.json             # 本次上传校验报告（回读结果、对象数）
```

`delivery-manifest.json` 关键字段：

| 字段 | 含义 |
| --- | --- |
| `delivery_id` | 本次交付的内容指纹（对象名 + 压缩后 sha256 + 字节数） |
| `dataset.target_prefix` | 桶内落点（如 `2026-11-04/`） |
| `caliber` | A1 口径：整条丢弃含图记录 / `o200k_base` + `encode_ordinary` / 切窗 / 分片 |
| `totals` | 文件数、分片数、源记录数、行数（窗口数）、token、未压缩/压缩字节、压缩比 |
| `files[]` | 每个源文件：`source_key`、`status`、`records`、`windows`、`tokens`、两种字节数、两种 sha256、`parts[]` |
| `files[].parts[]` | 每片：`name`、`windows`、`tokens`、`bytes_raw`/`bytes_zst`、`sha256_raw`/`sha256_zst` |
| `objects[]` | 桶内每个分片对象：`key`、`size`、`sha256`（压缩后）、`sha256_raw`（未压缩） |
| `provenance` | 多机合并时的来源：`machines`、`shards_per_machine`、`merged_from`、`multi_machine` |
| `object_count` | 期望对象总数 = 分片数 + 2（清单 + README） |
| `pipeline` | 上游 `manifest.json` 的停止原因/停止文件/token/`manifest.json` 自身 sha256 |

单源多片时，`files[].sha256_*` 是**各分片哈希按 part 顺序拼接后的 sha256**
（`sha256_scope=aggregate-of-parts`），逐片真实哈希见 `parts[]`。

## 三、幂等 / 续跑 / 冲突

| 远端状态 | 行为 |
| --- | --- |
| 对象不存在 | 上传 |
| 同名 + `x-amz-meta-sha256` 相同 + 字节数相同 | **跳过**（已完成的直接过） |
| 同名 + sha256 元数据不同 | **整批停**，退出码 3，一个字节都不写 |
| 同名 + 无 sha256 元数据，但上传流水证明是本次交付自己写了一半的对象 | **重传**（只在 `start` 之后没 `put` 的 key 上生效；`--no-repair` 可关掉） |
| 同名 + 无 sha256 元数据、且不是本次交付写的 | **整批停**，退出码 3 |
| 目标目录里有 manifest 之外的别人的对象 | **整批停**，退出码 3（`--allow-extra-objects` 可只警告） |

- 目标目录固定记录在 `pack-state.json`：续跑沿用同一个目录，不会每跑一次新建一个。
  要换新目录用 `--new-target`；换桶会自动不沿用。
- 压缩产物按「源文件 sha256 + 级别」复用，重跑不重压。
- `delivery-manifest.json` / `README.md` 在内容不变时**逐字节复用**（不重写 `generated_at`），
  否则每次重跑的哈希都会变，续跑会被自己判成「同名不同内容」。

## 四、多机多片合并后的交付目录

打包这一层不假设"一个源文件只产一片、只有一台机器在跑"。合并目录（多源 key、
同源多 `part`、分片住在 `machine-a/` 这类子目录里、`STOP_REASON=merged`、
可选的 `machine` / `host` 来源字段）可以直接喂进来：

- **对象名扁平化**：`machine-a/gpt-….part-0001.jsonl` → `gpt-….part-0001.jsonl.zst`，
  桶里不会多出一层机器目录；不同子目录里同名的分片会当场报错（否则上传会互相覆盖）。
- **形状体检（硬失败，退出码 2）**：同一源文件的 `part` 必须从 1 连续、不许重号；
  分片重名；`delivered_files[].parts` 与实际片数不符；`delivered_records` /
  `delivered_samples` 与逐片之和对不上（合并后 totals 没重算的典型症状）；
  `status=delivered` 但一片都没有。分片对应的 `source_key` 不在 `delivered_files`
  里只警告（records/status 会留空）。
- **totals 重算**：打包侧永远按实际分片重算 `totals`（文件数/片数/行数/token/两种字节），
  再与合并清单的 `delivered_tokens` / `delivered_samples` 交叉核对，不一致直接停。
- **来源可追溯**：清单 `provenance`（`machines`、`shards_per_machine`、`merged_from`）、
  逐文件 `files[].machines`、逐对象 `objects[].machine`；桶上另有 `x-amz-meta-machine`。
  README 在多机时自动加"来源机"一列。
- **停因**：默认接受 `corpus_exhausted` / `token_target_reached` / `merged`；
  合并层若用别的终态名，用 `--allow-stop-reason <REASON>` 显式放行。

## 五、滚动上传（`--rolling`）

同一个日期目录分多轮上传（每跑完一个 key 就追加一批）：

```bash
python delivery/delivery_pack.py <OUT> --execute --date 2026-11-04 --rolling
```

规则（全部由代码强制，不靠自律）：

| 对象 | 行为 |
| --- | --- |
| 分片 `*.jsonl.zst` | **只追加，永不覆盖**；同名不同 sha256 仍然整批停（退出码 3） |
| `delivery-manifest.json` / `README.md` | 允许整份替换，**但仅当上一轮清单里的对象集合 ⊆ 本轮**（且每个对象 sha256/字节数没变）；否则整批停 |
| `manifests/<utc>-<sha12>.json` | 替换前把旧清单原样快照到桶上，可回溯每一轮的清单 |
| 对象数核对 | 交付对象数 == 桶内对象数（`manifests/` 快照不计入） |

**多机各写各的 key（串行写入同一目录）**：每台的本地清单只含自己那几件，
所以 rolling 会把**桶上上一轮清单并进本轮清单**——按 `source_key` / `name` 取并集、
只增不减，重算 `totals` / `delivery_id` / `object_count`，并在 `rolling.rounds` 里记轮次。
两台机先后写同一目录（A 写 astra-0009、B 写 sol-0001）是支持的：B 的清单会自动包含 A 的条目。
同一对象在两轮里有两份不同内容 → 冲突整批停（不静默覆盖）。**同一时刻仍只允许一个写者**：
并发写会互相覆盖清单（后者并的是"上上轮"），虽然不会丢分片，但清单会缺中间那轮。

其它不变：`head_object` 核 `ContentLength` + `sha256` 元数据、最大分片与两个文档全文回读、同一目录续跑沿用上次目标、自己写了一半的对象可重传。`--rolling` 只是把「清单/README 可替换」这条口子打开，分片的不可变契约一字未放宽。

## 六、上传后校验

1. 每个对象上传后立刻 `head_object`：`ContentLength` == 本地字节数，且
   `x-amz-meta-sha256` == 本地压缩后 sha256（不符 → 退出码 4）。
2. 全部传完再列一次目标前缀：对象集合必须与清单**完全一致**（缺一个/多一个都失败）。
3. 回读：**最大的分片**整段读回算 sha256 比对，另外 `README.md`、`delivery-manifest.json`
   也全文回读比对。
4. 结果写 `upload-report.json`；命令行打印「上传 N / 重传 N / 跳过 N / 回读 N 一致」。

## 七、退出码

| 码 | 含义 |
| --- | --- |
| 0 | 成功（含 dry-run、全部跳过） |
| 2 | 前置校验失败：交付目录不完整、分段暂停未收尾、账不闭合、行数不符、缺凭证 |
| 3 | 远端同名对象内容不同 / 目录被占：**整批停**，绝不覆盖 |
| 4 | 上传后校验失败（大小、元数据、对象集合、回读 sha256） |
| 137 | 自测注入中断（`--simulate-fail-after`） |

## 八、前置校验（不通过直接退出码 2）

- `manifest.json` 存在且 `STOP_REASON ∈ {corpus_exhausted, token_target_reached}`；
  `segment_paused` 明确拒绝（那是分段中间态，不是终态）。
- `run.lock` 心跳新鲜 → 拒绝打包正在跑的目录（`--ignore-lock` 可跳）。
- 过滤段/去重段账本闭合（`filter_closure_ok`、`dedup_closure_ok`）。
- 清单里的每个分片都真实存在、非空、以换行结尾；**行数与 `shards[].samples` 一致**。
- 磁盘上没有清单未列出的 `*.jsonl`（`index.jsonl` / `keep.jsonl` 除外）——防 manifest 过期。
- 分片 token 之和 == `manifest.delivered_tokens`；逐文件与 `ledger.json` 的
  `delivered_windows` / `delivered_tokens` 对得上。

## 九、自测（离线，不需要凭证）

```bash
python -m unittest discover -s delivery/tests -v     # 24 例
```

合成交付目录由 `pipeline/corpus_pipeline.py --local-dir` 真跑产生（不是手写 manifest），
覆盖：A1 含图丢弃、dry-run 不写盘、清单/README 内容与字节一致、压缩回读、
重跑不重压、幂等跳过、目录占用自动递增、半截上传续跑、同名冲突整批停。
上传路径用 `--simulate-local-s3 <dir>`（本地假 S3，不联网）；
`--simulate-fail-after N` 模拟第 N 个 put 时进程被杀。

滚动上传另有一组用例：两轮（第二轮多一个 key）→ 不加 `--rolling` 必须整批停、加了则追加分片+替换文档+留快照、第三轮全跳过、用「少一个 key 的旧目录」回滚必须停。

另有一组**合并目录**用例：两台"机器"各自真跑一遍 pipeline，再按
`<machine>/<分片>` 合成 3 源 key / 4 分片 / 2 机的合并目录，验 dry-run、
清单自洽（合并 totals、同源多片的聚合 sha256）、上传校验，以及断号/重号/
totals 没重算三种合并事故都会被拦下。

## 十、磁盘与耗时预算

- 额外磁盘 ≈ 压缩后体积（JSONL 文本 zstd level 10 约 1/8~1/12），默认就写在交付目录的
  `_delivery_pack/` 下；磁盘紧就用 `--staging-dir` 指到别处。
- 压缩一遍 ≈ 读一遍分片（level 10 在 4 核上约 20~40 MB/s 输入），压缩后立即做一次解压回读校验。
- 上传后回读只抽最大的一个分片，不整批下载。
