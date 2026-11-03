# accept —— 数据集交付的独立验收侧

这一套脚本**不看开发侧流水线的实现**，只按锁定口径从头写一遍，用来交叉验证
交付物。目的很明确：同一个口径被两个人各实现一次，如果两边都错，错的概率远低于
一个人写两遍。

**范围**：只处理本地文件，零网络。不连 B2、不下载、不上传、不改任何远端对象。
本轮只交付脚本与本地合成数据单测，跑批等指令。

---

## 一、锁定的口径（验收判定依据）

| # | 口径 | 验收侧怎么验 |
| --- | --- | --- |
| 1 | 「100 轮」= `request.messages` 条数 ≥ 100 | 逐条数 `request.messages` 长度（切窗片段除外） |
| 2 | `encrypted_content` 卡 `request.messages[].content[]` 里 `type=encrypted_content` 的 part | 逐条检查该 part 存在且值非空 |
| 3 | `rs_id` 取 `response.response_data.output[].id`（`rs_` 前缀） | 逐条要求至少一个 `rs_` 前缀 id |
| 4 | session 去重按 `session_id` 中 `~` 之前的 uuid 段，保留 `messages` 最长那条 | 独立重算一遍去重，比对移除量与交付集 |
| 5 | token 计入密文，tiktoken `o200k_base` / `encode_ordinary`，按每条 message 紧凑 JSON 单独计数求和 | 两套独立序列化 + tiktoken 重算，误差必须 0 |
| 6 | 分片按体积，每包 ~500 MB；单条超 128k token 按窗口切分 | 逐片量体积、逐条量 token、检查窗口标记与覆盖 |

约定细节（口径表没写死、验收侧按最严格解释执行）：

- **`encrypted_content` 非空**：值不是 `None`、不是空白字符串才算命中。
- **`rs_id` 非空**：`output[].id` 必须以 `rs_` 开头，空串不算。
- **session 归一化**：`session_id` 去首尾空白、`~` 前段统一小写；没有 `session_id`
  的记录各自成户，不因 session 去重被删。
- **成员同一性**：一条 message 成员的**完整 JSON**（key 排序、紧凑分隔符）算同一性，
  所以 `encrypted_content`、`tool_calls` 等字段都参与判定。
- **前缀关系**：`messages` 序列的严格前缀就丢弃短的那条；完全相同只留一条
  （留原下标最小的那条，保证可复现）。
- **前缀去重顺序**：先 `session_id` 去重，再对存活集做前缀去重，两步移除量分开记。
- **切窗不重叠**：超长记录切成不重叠的窗口，否则窗口之间自己构成前缀关系，
  会被交付集前缀不变量判为违规。

---

## 二、交付契约

**以开发侧 `pipeline/corpus_pipeline.py` 的实际输出为准**（联调后对齐，见第八节）。
下面先写"验收侧认什么"，再写具体字段；字段名两侧不一致的地方已经做了归一，
不需要开发侧迁就本文件里的别名。

### 2.1 分片

- 文件名 `deliver-0001.jsonl`、`deliver-0002.jsonl` …，**编号连续不跳号**。
- 每行一条记录，是一个 JSON 对象；文件以换行结尾（末行必须完整）。
- 每条记录仍是原始 schema：`session_id` / `request.messages` / `response.response_data.output`。
- 单包体积 ≤ 500 MB（默认容差 2%，因为常见实现是"写完才判断是否切"）。
- 单条记录 ≤ 128k token；超长必须先切窗。

### 2.2 切窗标记

切窗片段需要在记录里带一个窗口标记，验收侧认这几个键名（顶层或 `meta`/`_meta` 下），
两种写法等价：

```json
{"window": {"index": 0, "count": 3, "source_fingerprint": "<源记录指纹>"}}
{"_window": {"index": 0, "of": 3, "source": {"key": "x.jsonl", "off": 123},
             "messages_total": 1234, "oversize_message": 0}}
```

窗口内容**必须放在 `request.messages`**（与未切窗记录同一条读取路径）。
开发侧目前把窗口内容写在顶层 `messages`、而 `request.messages` 仍是完整对话，
验收侧默认判为错误——因为下游按真实 schema 读到的还是超长记录，切窗等于没做。
双方确认改口径前，可用 `--allow-top-level-window-messages` 放行（降级为 warn）。

- `index` / `count` 用于校验覆盖：同一 `source_fingerprint` 的片段必须正好覆盖
  `0..count-1`，不能缺、不能重。
- 带标记的记录**豁免**「messages ≥ 100」这条（切成窗口后条数自然不足）。
- 同一 `rs_id` 出现在多个切窗片段里是允许的；但同一条 `rs_id` 不能既有
  完整记录又有切窗片段。

### 2.3 manifest.json

对账恒等式：

```
原始条数 = parse_errors + filter_rs_id + filter_encrypted_content
         + filter_min_messages + dedupe_session + dedupe_prefix + 交付条数
```

**逐文件**成立，且各文件之和等于总量。某一步一条都没删，也必须写 `0`——
写 0 才能证明这个阶段真的跑过。

```json
{
  "schema": "buckethub.dataset.manifest/v1",
  "stop_reason": "token_target_reached | source_exhausted",
  "token_target": 1000000000,
  "tokenizer": {"encoding": "o200k_base", "method": "encode_ordinary"},
  "totals": {
    "raw_lines": 7295,
    "removed": {"parse_errors": 0, "filter_rs_id": 0, "...": 0},
    "delivered_records": 76,
    "delivered_tokens": 27060000,
    "window_pieces": 3
  },
  "files": [
    {
      "source_key": "Gpt-2026-10-18/gpt-6-astra-0009-20261001.jsonl",
      "raw_lines": 7295,
      "removed": {"parse_errors": 0, "filter_rs_id": 0, "filter_encrypted_content": 0,
                  "filter_min_messages": 0, "dedupe_session": 0, "dedupe_prefix": 0,
                  "stop_cut": 0},
      "delivered_records": 76,
      "delivered_tokens": 27060000,
      "window_pieces": 3,
      "status": "done"
    }
  ],
  "shards": [{"name": "deliver-0001.jsonl", "records": 79, "bytes": 524288000, "tokens": 27060000}]
}
```

字段命名不强制完全一致：验收侧会做别名归一（`files`/`per_file`/`sources`、
`removed`/`removals`/`dropped`、`raw_lines`/`input_lines` 等），移除步骤按关键词归类
（含 `session` → session 去重，含 `prefix` → 前缀去重，含 `encrypt` → 密文过滤…）。
**认不出的步骤名直接报错**，不会悄悄漏算。

口径约定：

- `delivered_records` ＝ **源记录条数**（对账用），不是分片行数。
- 分片行数 = `delivered_records + window_pieces`（切窗片段是额外行）。
- `stop_cut` 只在"1B 到达即停"时出现，记录停止后没交付的那部分，否则对账不闭合。

### 2.4 到达 1B 即停：整组原子

- `stop_reason = "token_target_reached"`，且 `total.delivered_tokens ≥ token_target`
  （小于目标就声称到达，判错）。
- **最小提交单位是一条记录的全部窗口**：过线时这一组要么全落盘、要么全不落，
  不允许 `window 0 of 2` 已交付、`1 of 2` 被丢掉。交付里任何窗口组的
  `index` 都必须正好覆盖 `0..of-1`（分片检查统一校验）。
- `manifest.stop_after_record` 必须存在，且与 `emit.journal` 最后一笔、
  与 `delivered_tokens` 对得上：

```json
"stop_after_record": {"id": "Gpt-…/x.jsonl#123603", "key": "Gpt-…/x.jsonl", "off": 123603,
                      "n_msgs": 260, "windows": 2, "tokens": 186022,
                      "tokens_total_after": 215506}
```

- **允许超出目标，但有上界**：整组原子提交会让 `delivered_tokens` 最多超出
  "这一条记录自身的 token 量"。验收按 `delivered_tokens − token_target ≤
  交付集里单条记录的最大 token` 校验（等价于"≤ 前缀去重后单条最大 token"），
  超出这个上界说明不是整组原子造成的，判错。
- 未交付部分：`not_delivered = kept_records − delivered_records` 必须闭合。
  逐文件记不了可以只记总量，此时各文件差额合计必须等于 `not_delivered`（降级为 warn）。
- 最多一个文件标 `status: "done_partial"` 且必须是 `files[]` 最后一个
  （没拉取的文件不要写进 manifest）。

---

## 三、验收跑哪几项

| 检查 | 实现 | 失败即不可交付 |
| --- | --- | --- |
| 去重正确性 | `accept/dedup.py` 独立实现（排序 + 相邻比对，O(n log n)） | 是 |
| 交付集前缀不变量 | 对交付集整体重扫，不允许存在任何前缀/完全相同 | 是 |
| token 复算 | `accept/tokens.py` 两套独立序列化 + tiktoken，逐片比对 manifest | 是（误差必须 0） |
| 对账 | `accept/ledger.py` 逐文件恒等式 + 总量/分片交叉核对 | 是 |
| 分片完整性 | `accept/shard.py` 逐行 `json.loads`、末尾完整、体积、单条 token、rs_id、密文、切窗覆盖 | 是 |
| 1B 即停原子性 | `accept/stopcheck.py` 整组窗口完整性、停止点可追溯、超出量上界、未交付闭合 | 是 |
| 两侧交叉验证 | `accept/crosscheck.py` 用独立实现从原始 jsonl 重算，对比开发侧账本与交付内容 | 是 |

---

## 四、怎么跑

所有命令**默认 dry-run**：不加 `--execute` 只打印计划，不读交付物、不写文件、不联网。

```bash
# 1) 看计划（安全，什么都不碰）
python3 -m accept.cli verify --delivery ./out --manifest ./out/manifest.json

# 2) 真跑验收
python3 -m accept.cli verify --delivery ./out --execute
python3 -m accept.cli verify --delivery ./out --execute --json --report ./accept-report.json

# 3) 只重算 token：默认抽样 200 条用两套独立实现互验（误差必须 0），
#    再加 --diagnose 3 时对前 3 条打印 4 种序列化变体计数，用于反推口径
python3 -m accept.cli recount --delivery ./out --execute --sample 200 --diagnose 3

# 4) 生成一份合成样本 + 参考交付（本地测试用）
python3 -m accept.cli fixture --out ./accept-fixture --execute

# 5) 自检：合成 → 参考交付 → 验收 → 8 类破坏必须报错
python3 -m accept.cli selftest --execute

# 6) 联调：开发侧产出目录 vs 验收侧独立重算
python3 -m accept.crosscheck --raw-dir ./itest/raw --delivery ./itest/out
```

窗口内容写在顶层 `messages` 的交付（开发侧当前形态）默认会有 error；
确认口径后用 `--allow-top-level-window-messages` 放行。

退出码：`0` 通过 / `1` 验收不通过 / `2` 参数或配置错误 / `3` 分词器不可用。

### token 复算对不上时怎么办

`recount --diagnose N` 会把同一份 messages 在四种序列化口径下的计数并排打出来
（`sort_keys` 有无、`ensure_ascii` 有无，外加"文件里原始字节"口径）。哪个数字与
manifest 对上，就说明对方用的是哪一种口径——比"差 3%"这种报错有用得多。

### 分词器供给（离线）

验收只认 tiktoken `o200k_base` / `encode_ordinary`。本机没装时脚本会以退出码 `3`
**显式失败**，不会偷偷换成别的分词器（换了就不是同一个口径，误差为 0 也就无从谈起）。

```bash
uv pip install tiktoken                     # 需要能联网时一次装好
export TIKTOKEN_CACHE_DIR=/path/to/cache    # 之后可完全离线（BPE 文件已缓存）
python3 -c "import tiktoken; print(len(tiktoken.get_encoding('o200k_base').encode_ordinary('你好')))"
```

单测里用 `CharCounter`（token 数 = 字符数）只为跑通记账管道，报告会标
`acceptance_valid: false`，**不能用于验收结论**。

---

## 五、单测

只用本地合成数据（`accept/synth.py`），不读真实文件、不联网：

```bash
python3 -m unittest discover -s accept/tests -t .
```

| 文件 | 覆盖 |
| --- | --- |
| `tests/test_dedup.py` | abcde/abcdefg、完全相同、同长不同内容、跨 session 前缀、同 session 前缀、空 messages、全空、**共享前 k 个元素后分叉（负例）**、三级前缀链、乱序稳定、确定性、8000 条不退化 |
| `tests/test_tokens.py` | 两套序列化逐字节互验（含 emoji/控制字符/浮点/转义）、原文切片、截断行报错、token 复算误差 0（需 tiktoken）、密文计入、口径变体 |
| `tests/test_ledger.py` | 别名归一、嵌套计数摊平、逐文件闭合、未知步骤报错、缺步骤报错、总量/分片交叉、到达目标的记账 |
| `tests/test_shard.py` | 末尾无换行、坏行、体积超限、单条超 token、切窗豁免与覆盖、rs_id/密文缺失、重复记录/重复 rs_id、编号跳号、交付集前缀违规 |
| `tests/test_e2e.py` | 合成 → 参考交付 → 全量验收通过、逐文件对账闭合、可复现、体积切分、1B 即停记账、**dry-run 不读不写** |

另有 `tests/test_dev_format.py`（11 例）专门锁住开发侧实际格式：`ledger.json` sidecar、
`_window` 的 `of`/`source`、窗口内容位置、窗口覆盖完整性、停止后未交付的归属；
`tests/test_stopcheck.py`（11 例）锁住 1B 即停的四条判据（缺 stop_after_record、
与 journal 不一致、窗口组不完整、超出上界、未交付不闭合都必须报错）。

`tests/test_dev_integration.py`（4 例）是**真联调**：直接 subprocess 跑开发侧
`pipeline/corpus_pipeline.py`（合并进本分支后自动生效，也可用环境变量
`CORPUS_PIPELINE=/path/to/corpus_pipeline.py` 指过去），断言窗口写回
`request.messages` 且可还原、逐文件交付计数闭合、连跑三次幂等、窗口组整组原子
与超出上界。这组用例在修复前的 `2b954a1` 上 **4 条全红**，修复后全绿。

`tiktoken` 缺失时，依赖真分词的用例自动 skip，其余全跑——这是刻意的：
记账/切片/去重这些逻辑不该被"没装包"挡住。装好后全绿：不带开发侧流水线时
103 例，带上联调用例共 **114 例、0 skip**。

---

## 六、跑批中断与恢复（给开发侧的约定）

验收侧本身无状态，重跑幂等。跑批侧如果要能断点续跑，manifest 必须满足：

1. `files[]` 按处理顺序追加，每个文件处理完就落一次 manifest（原子写：临时文件 + rename）。
2. `status` 只有 `done` / `done_partial`（中途停止）/ `pending` 三种语义；
   恢复时只重跑 `pending`，`done` 一律跳过。
3. 分片按编号连续追加；中断时最后一片可能不完整，恢复前先按验收的分片检查
   把尾部残行裁掉（保留到最后一个完整换行），再继续追加。
4. `stop_cut` 只对"到达 1B 之后没交付"的量记账，不要在恢复时重复计入。

---

## 七、和开发侧实际输出的对齐（联调结论）

用 `pipeline/corpus_pipeline.py --local-dir <合成目录> --execute` 实跑后，两边差异如下，
**验收侧已全部适配，开发侧不需要改格式**：

| 项 | 开发侧实际 | 验收侧处理 |
| --- | --- | --- |
| manifest 顶层 | `STOP_REASON` / `delivered_tokens` / `delivered_samples` / `kept_records` / `delivered_records` / `not_delivered` / `shards` / `config` / `ledger` / `stop_after_record` | 别名归一 |
| 逐文件明细 | 不在 manifest 里，在同目录 `ledger.json` 的 `files{}`，字段扁平 | 自动读 sidecar |
| 过滤步骤 | 多一个 `no_session_id` | 作为可选步骤 `filter_no_session_id` 纳入对账 |
| 停止原因 | `corpus_exhausted` | 归一为 `source_exhausted` |
| 分片清单 | `{file, samples, bytes, tokens}` | `samples` 视为行数 |
| tokenizer | 只写了 `config.tokenizer = o200k_base` | 从 config 取编码器；方法按锁定口径（`encode_ordinary`） |
| 切窗标记 | `_window{index, of, source{key,off}, messages_total}` | `of`→`count`，`source.key#off` 当指纹 |
| 窗口内容 | 写在 `request.messages`（`ac0c81b` 起） | 与未切窗记录同一读取路径，直接校验 |
| 停止点 | `stop_after_record{id,key,off,n_msgs,windows,tokens,tokens_total_after}` | 与 journal 最后一笔、交付 token 交叉核对 |
| 未交付 | 只有总量 `not_delivered` | 逐文件差额合计与总量一致时降级为 warn |
| 交付行数 | `delivered_samples`（行）/ `delivered_records`（源记录） | 分片行数按 `delivered_samples` 校验 |

历史包袱：`2b954a1` 把窗口写在顶层 `messages`，验收侧默认判错，
`--allow-top-level-window-messages` 可以放行——但 `ac0c81b` 已按裁决改回
`request.messages`，正常不该再用这个开关。

### 逐项交叉验证（`accept/crosscheck.py`）

```bash
python3 -m accept.crosscheck --raw-dir ./itest/raw --delivery ./itest/out
```

它不看开发侧代码，直接用本仓库的 `dedup` / `tokens` 从原始 jsonl 重算一遍，然后对比：

- 逐文件：扫描数、各过滤原因、session 移除、前缀移除、保留数（开发侧账本 vs 独立重算）；
- 交付集：把分片内容按 `_window` 合并后与独立算出的保留集按**内容摘要**比对；
  提前停止时"缺的条数"必须正好等于 `not_delivered`；
- token：交付复算 vs manifest 声明，误差必须 0。

`ac0c81b` 上的实测（同一套合成 fixture，33 条原始 / 12 条保留）：

| 场景 | 逐文件计数 | 保留集 | token 复算 | verify |
| --- | --- | --- | --- | --- |
| 全量跑完 | 一致 | 12 / 12，缺 0 多 0 | 218,416 / 218,416，误差 0 | 全绿 |
| 多窗口（20k 阈值切成 10 片） | 一致 | 12 / 12，缺 0 多 0 | 218,416 / 218,416，误差 0 | 全绿 |
| 停在窗口组前（target=组前累计） | 一致 | 12 / 10，缺 2 = not_delivered 2 | 29,484 / 29,484，误差 0 | 全绿 |
| 停在窗口组中间（target=组前+1） | 一致 | 12 / 11，缺 1 = not_delivered 1 | 215,506 / 215,506，误差 0 | 全绿 |

停止场景实测的超出量：组前边界 0、组内过线 186,021、组尾 1、组中 93,011，
均 ≤ 该条记录自身 186,022 token，**"最多超一条记录的 token 量"这个上界成立**。
连跑三次 `session_removed` / `prefix_removed` / 逐文件交付条数完全不变。

## 八、已知边界

- **单条消息本身就超 128k token**：无论怎么切窗都满足不了，验收会明确报错；
  这种情况要么换样本，要么显式放宽 `--max-record-tokens` 并写进交付说明。
- **切窗重叠**：重叠窗口会互相构成前缀关系，被交付集不变量判违规；
  需要重叠窗口时用 `--skip-prefix-invariant` 显式关掉并说明理由。
- **分片体积**：只容忍 2% 的写后切分；要求更严可以 `--shard-slack 0`。
- **manifest 字段名**：别名覆盖常见写法，缺字段时给 warn、错字段给 error；
  实在对不上就用 `--report` 输出明细，按 `where` 定位。
- **单行大于分片阈值**：`--shard-bytes` 是软上限，一条记录本身超过阈值时仍会写入并超包
  （默认 500 MB、单条 1~8 MB 时不会发生）。验收侧按硬上限检查，触发即报错。

## 九、按源文件命名的交付形态（`agent/agent/accept-source-names`）

开发侧改为「产物按源文件命名 + 停点在源文件边界」后，验收侧相应放开了三处写死：

| 项 | 以前 | 现在 |
| --- | --- | --- |
| 分片 glob | `deliver-*.jsonl` | 默认 `*.jsonl`，并内建排除 `keep.jsonl` / `index.jsonl` / `ledger.json`（同一 out-dir 直接验收时不会被误当分片） |
| 命名连续性 | 必须 `deliver-0001..N` 连号 | 按源文件命名时改查 `.part-NNNN` 从 1 连续、且不允许 `x.jsonl` 与 `x.part-0001.jsonl` 并存；旧命名仍走连号检查 |
| 停止点 | 只认 `stop_after_record`，超出量 ≤ 单条记录 token | 新增 `stop_file`：`stop_file` 之后不允许再有源文件、停止文件本身必须有交付，超出量上界改为**停止文件自身的产出** |

判定口径（文件边界停）：

- 交付里出现的源文件必须全部 ≤ `stop_file`（按 key 排序比较），否则报错；
- `stop_file` 必须真的交付了记录；
- `delivered_tokens − target ≤ stop_file 的产出 token`（1B 目标下约 3~6%）；
- `stop_after_record` 若存在，仍要与 journal 最后一笔一致；窗口组仍要求整组原子；
- `stop_file` 与 `stop_after_record` 至少有一个，否则判为「无法确认停在哪」。

新增回归用例：`accept/tests/test_source_names.py`（10 条，覆盖命名、part 编号、命名冲突、
元数据文件排除、`stop_file` 合法/越界/缺字段三种情形）。

## 十、逐文件账本字段（`ledger.json`，与开发侧同一份契约）

验收侧对账恒等式是 `原始 = 各移除步骤 + 交付 + 停止后未交付`，所以开发侧每个文件必须带：

| 字段 | 用途 |
| --- | --- |
| `kept_records` | 去重后应保留条数；缺了就会报「该文件去重后应保留 N 条，但交付条数记的是 0」 |
| `not_delivered` | 停止点之后没交付的条数；缺了逐文件恒等式两边对不上 |
| `status` | `delivered` / `zero_kept` / `not_reached`（已索引、被文件边界停挡在后面） |

**`stop_cut` 禁用**：它被归到 `not_delivered` 的别名，两边同时写就是双重计数。
验收侧一见到 `stop_cut` 与 `not_delivered` 同时出现就会加总两次并对不上，这是刻意的 fail-closed。

`shards[].source_key` 必填；它与 `stop_file` 同属「源对象 key」命名空间，按字符串序比较。

## 十一、`--shard-glob`

`verify` / `recount` 默认 `*.jsonl`（内建排除 `keep.jsonl` / `index.jsonl` / `ledger.json`）；
**`crosscheck` 默认仍是 `deliver-*.jsonl`**，不放宽——它扫的是开发侧 out-dir，那里住着
`index.jsonl` / `keep.jsonl`，放宽默认值会把它们当成交付分片。按源文件命名时显式传：

```bash
python -m accept.crosscheck --raw-dir <RAW> --delivery <OUT> --shard-glob 'gpt-*.jsonl'
```

`.part-NNNN` 从 0001 起、首片也带后缀；`<主干>.jsonl` 与 `<主干>.part-0001.jsonl` 不允许并存
（验收侧会报「命名冲突，交付形态不唯一」）。
