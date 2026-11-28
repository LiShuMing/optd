# Q6 规模实验的评审答复

2026-11-28，AGENT1 回复 H002。引擎版本为 `2ca6fdb`，实验环境、数据和四组轮换顺序见[合并持久化验证](view_output_bundle.md)。本文复核已有数据，给出下一份实验设计的口径。更大规模的运行结果仍为未知。

## 1. 在 6,005 行上，查询节省能否抵消维护开销？

**已验证。** 从同一轮、同一操作、同一 v3 版本的原始记录配对。增量和全量路径都持久化基表变更，再各读一次结果。每种操作有 24 对计时样本，删除 100 行包含 4 对检查点样本。下表分别对逐对差值取中位数（ms），各列中位数无法直接相加。

| 变更 | 增量多花的写入时间 | 读取视图省下的时间 | 增量多花的写＋读总时间 |
| --- | ---: | ---: | ---: |
| 插入 1 行 | 2.97 | 1.13 | 1.94 |
| 删除 1 行 | 2.01 | 1.19 | 0.89 |
| 插入 100 行 | 5.04 | 1.29 | 3.59 |
| 删除 100 行 | 4.27 | 1.17 | 3.09 |

在四组独立仓库中，每组分别计算四种变更的配对总耗时差，中位数方向全部一致。此前报告的 20.3% 衡量的是旧增量实现与新增量实现在普通提交上的写入 P50 差异。这一结果与同一新版上全量路径的一写一读更快可以同时成立。

用[原对照脚本](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/optd-incremental-sql/benchmarks/tools/compare_q6_writes.py)生成逐次 JSON 后，可在输出目录运行以下命令复算。此次复核使用上轮保存的数据，仅为核对同步次数额外运行了无视图诊断。

```sh
python3 - <<'PY'
import json
from pathlib import Path
from statistics import median
for j, stage in enumerate(('insert_1', 'delete_1', 'insert_100', 'delete_100')):
    pairs = []
    for trial in range(4):
        inc, full = [json.loads(Path(f'{trial}-new-{mode}.json').read_text())['rounds']
                     for mode in ('incremental', 'full')]
        pairs.extend(zip(inc[4+j::4], full[4+j::4]))
    print(stage, len(pairs), [median(sign*(a[key]-b[key]) for a,b in pairs)/1000
          for key,sign in [('dml_us',1), ('read_us',-1), ('total_us',1)]])
PY
```

## 2. 对两项存储优化的判断

**已验证。** 增量诊断日志的 28 次写入中，27 次普通提交各同步 8 次，1 次检查点提交同步 13 次。本次用相同 v3 诊断程序和工作负载对无视图仓库再跑一遍，28 次均为 7 次，28 个金额均与十进制期望值一致。无视图路径也写基表脏块和表增量。两条路径共有清单、临时指针和三次目录同步。视图包写入时计算 SHA-256；独立 Arrow 文件在同步后回读计算摘要。[提交发布](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/optd-incremental-sql/src/database/store.rs) · [独立文件写入](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/optd-incremental-sql/src/store.rs) · [视图包](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/optd-incremental-sql/src/database/store/view_bundle.rs)

**推演。** 对本样本的普通增量提交，如果把基表脏块、表增量和视图包三次文件同步合为一次，并保留现有发布顺序，计数应为 **6 次**：一个数据包、一个清单、一个临时指针、三次目录同步。相同假设下全量路径也是 6 次。降至 5 次需要额外改变布局或发布协议，并证明崩溃可恢复。次数相同也不能推出两条路径耗时相同。推广写时哈希可省去已确认的回读路径，但节省多少毫秒仍未知。建议在冻结 v3 的规模基线后分别测量这两个变量，避免把收益混在一起。

## 3. AGENT2 的实验设计应回答什么？

| 评审项 | 结论与验收口径 |
| --- | --- |
| 输入规模和正确性 | 以 6,005 行作锚点。离线按 10、50、100 倍分别生成 60,050、300,250、600,500 行，派生互不重叠的键，保留命中比例，并用 Decimal 独立计算每个版本的收入。另用固定版本 DuckDB/dbgen 的 SF 0.01、0.1 生成数据，复用仓库原始 Q6 和类型，分别记录分布、行数和摘要。两条数据轨道各自求交叉区间。 |
| 导入和变更键 | 已核对原始 seed.sql 为 1,501,836 字节，CLI 单脚本上限为 16 MiB。大样本需要分批导入，预热前核对两条路径的逻辑基表、实际块数和初始金额。变更键从各样本已有键域之外分配，删除条件只命中本轮新增的 1 或 100 行；尤其要检查放大后与固定键 1000000 的碰撞。 |
| 块列表拐点 | 触发条件是**实际块数超过 128**。每块最多 256 个不同条目，32,768 是满块容量，分裂和填充率会改变首次越界的行数。改变此关系且使用外部列表时会重写整份列表，完全复用关系的提交可沿用原引用。在 128 块两侧布点，逐次记录真实块数、列表字节数和同步次数。分页索引的收益留待测量。[块实现](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/optd-incremental-sql/src/row_blocks.rs) |
| 写入与读取计时 | 原始样本两条路径的 DELETE 选择器每次都访问 24 个基表叶块，INSERT VALUES 均为 0。100 行 INSERT 还改变了解析和求值工作量。四种 DML 分列。现有驱动直接计时写入和读取，细分选择／编译、视图维护和持久化须另行埋点；工作量计数不能还原阶段耗时。 |
| 检查点与交叉点 | 用实际清单识别检查点并与对应的全量写入配对，保留预热边界。4–8 个检查点可展示逐次耗时，无法据此验收生产 P99。每种 DML、读写比和数据轨道分别比较配对总耗时，给出各轮方向；只在方向稳定改变时报告相邻两个已测规模作为区间。没有稳定变号就报告已测范围与差值。 |
| 一写多读 | 主对照保持一写一读，全量路径每次重算并返回内存。R=10、100 作为独立场景实跑整轮，注明相同版本是否重复执行 SQL。若比较每版本复用一次全量结果，应另列其缓存、持久性和失效语义。不能用单次读取时间乘以 R 代替实测。 |

## 4. 工程治理与后续优先级

**已验证。** 工作流提案仍问是否需要 AGENTS.md，仓库已有该入口；状态管理 RFC 的历史描述和已完成的 R07c 共存；README 缺少两个增量 crate 的入口。ROADMAP 的 2,039 行和起始提交属于历史台账。建议单独补现状链接及归档索引，并保留历史证据和原设计的评审状态。[工作流提案](../development/agentic-workflow.md) · [状态管理 RFC](rfc_state_management.md) · [路线图](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/ROADMAP.md)

通用 CI 和文档工作流引用 checkout v2，增量工作流使用 v4 并调用现有 SQL 检查入口。SQL 模式未同时运行严格 rustdoc 和 mdBook。此次本地检出只有示例 Git 钩子，未配置 core.hooksPath；远端分支保护和其他开发者的环境仍未知。建议单列文档构建与发布前辅助检查，保留人工逐项核查。[CI 配置](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/.github/workflows/incremental.yaml) · [现有检查脚本](https://github.com/LiShuMing/optd/blob/2ca6fdbeeb53bdf3fda0a785bede4c185f0e5112/scripts/agent-check.sh)

**排序建议。** 先完成 H001 的 v3 规模对照。R05b 的 18 条 TPC-H 和 94 条 TPC-DS 是已记录的原始计划受阻数，未来可获准的数量、SQL 语义和运行性能仍须逐条验证。TODO-S4 应依据大表检查点的状态字节数与耗时排序，R08 应依据目标源快照与删除模式排序。本次评审仅回复这些提案，不构成上述实现的立项决定。[计划准入证据](tpc_incremental_plans.md)
