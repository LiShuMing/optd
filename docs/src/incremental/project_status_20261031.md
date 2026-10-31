# Incremental 项目状态与交接记录（2026-10-31）

**状态：** 功能迭代已暂停；本次仅整理、提交并按所有者授权发布现有改动。
**范围：** 当前工作目录内所有 Git 跟踪文件的改动及未跟踪文档；不纳入构建产物、运行数据库和原始日志。
**完整任务台账：** [ROADMAP](../../../ROADMAP.md)。旧设计和测试报告中的结果、提交及“尚未发布”说明均属于各自当时的检查点，不代表本次交付状态。

## 1. 运行状态与交付边界

- 工作目录：`/home/admin/xwork/optd`；任务分支：`optd-incremental-ivm`。
- 整理前 HEAD：`144503633646c1d3698c0d1e1a45ce05b160a26a`。
- 所有者要求再运行一小时后停止；功能、构建和测试已经结束，持续开发目标保持 `paused`，未将整个项目标记完成。本次发布不恢复功能迭代。
- 一小时起点为 2026-10-31 10:55:07 UTC，上限为 11:55:07 UTC。功能和验证已提前收尾；最终状态文件在 11:55:11 UTC 写入，随后目标状态确认暂停。
- 本轮启动的构建、测试、基准复验及调查命令均已回收。没有本轮遗留的运行中验证进程；没有执行部署或启动常驻服务。
- 已读取 GitHub 默认分支，确认主干为 `origin/main`。整理时主干为 `7b945efef0ed550f6062eec353899f81c476a5f3`，本地领先 63 个提交、没有分叉。
- 所有者本次明确授权提交全部当前改动并推送主干，因此此前仅本地保存的实现提交和本次文档整理一并进入交付范围。使用普通快进推送，不改写既有历史。
- 本次新提交的 author 和 committer 均使用 `shuming.li <ming.moriarty@gmail.com>`；不使用企业邮箱。既有提交身份保留。
- 远端目标为 `git@github.com:LiShuMing/optd.git` 的 `refs/heads/main`。实际推送结果和远端提交一致性须在推送后单独核验，不能用本记录代替远端回读。

## 2. 当前已落地的能力

SQL 绑定与语义检查、增量规则、物理执行、状态访问、事务与持久化分别有独立职责。规则通过现有 Optd 表达生成逻辑和物理维护计划，执行器运行编译后的物理指令；不在刷新时重新选择 SQL 改写规则。

当前台账为 **31 项中 13 项 verified、1 项 in_progress、17 项 pending**。已验证项为 R01a–R01d、R02a–R02d、R03a–R03c、R04a 和 R09c，包含工作量计数、事务表块、共享状态目录、分块关系缓存、MIN/MAX 索引、DISTINCT 引用计数、GROUPING SETS、outer/semi/anti 匹配计数，以及完整计划阻塞原因预检。各项验证范围以 ROADMAP 的验收记录为准。

最近一小时推进的是 R04b 的以下子能力，尚不足以关闭 R04b：

| 本地实现提交 | 已实现与验证的子能力 |
| --- | --- |
| `2a4a7e1` | 将可观察的 IN/NOT IN 三值逻辑改写为普通 Aggregate/Join/CASE 关系表达，保留 TRUE/FALSE/UNKNOWN、空输入与 NULL 语义。 |
| `4057b86` | 提取可证明的普通等值相关键，成员关系按相关键和比较值分组，标志关系仅按相关键分组，只维护相关分区。 |
| `feb4ec1` | 支持直接 NULL-safe 相关键和普通/NULL-safe 混合键；相关键匹配策略与标量 IN 的普通相等语义保持独立。 |
| `1445036` | 同一 Projection/Filter 内相同 IN/NOT IN 共享正向成员与计数绑定，保留不同表达式、子查询和作用域的独立性；65 个不同绑定仍触发展开限制。 |

普通相关分区和混合 NULL 策略各有 128 个带正负权重的更新批次，分别与独立三值逻辑模型及输出差分前缀比较。SQL 测试还覆盖多视图写入、删除、更新、请求重试、重启和历史回放。未获证明的计算型、范围及改变行数的相关作用域继续拒绝，不能静默完整重算。

## 3. 最新验证与实际访问范围

验证环境为 Rust 1.95.0、Linux x86_64、锁定且离线的依赖、4 个编译任务、debug 信息关闭、编译器增量模式关闭。当前验证目标目录为 `target/roadmap-20261031`，TMPDIR 为其 `tmp` 子目录，命令通过已有 `/tmp/optd-sql-env-20261029.sh` 环境脚本执行。

| 验证 | 观测结果 | 本地证据 |
| --- | --- | --- |
| `bash scripts/agent-check.sh sql` | exit 0；默认配置 324 passed / 0 failed / 2 ignored；Iceberg 配置 338 passed / 0 failed / 3 ignored；格式和两配置 all-target Clippy 均通过。 | `target/roadmap-r04b-binding-reuse-gates-20261031.log` |
| 私有项文档，`RUSTDOCFLAGS='-D warnings' cargo +1.95.0 doc --locked -p optd-incremental-sql --features iceberg --no-deps --document-private-items` | exit 0。 | `target/roadmap-r04b-binding-reuse-rustdoc-20261031.log` |
| 23 项 quantified-truth 定向测试 | exit 0；普通/混合相关键、共享/不共享边界、SQL 恢复、schema 和拒绝场景通过。 | `target/roadmap-r04b-binding-reuse-focused-20261031.log` |
| 固定变化分区、未变化分区数 8 和 512 | 两种规模的 peer visits 均由共享前 12 降为共享后 4；输出差分均为 2 行；算子内完整扫描和快照求值均为 0。 | `target/roadmap-r04b-binding-reuse-scope-20261031.log` |
| 新构建程序复验 Q10/Q45 | 两条查询各完成 3 轮 SQL 写入、结果比较、基础表校验、重启和输出历史校验；严格计划均合格，未出现完整节点重算。 | `target/roadmap-r04b-truth-final-live-20261031/` |
| 最近一次文档构建和差异检查 | mdBook 与 `git diff --check` 均 exit 0。 | `target/roadmap-r04b-final-docs-20261031.log` |

最新完整门禁日志 SHA-256 为 `e85839793e1eb6dad85624b2a9a605ca682df72a0093e2cc05f346d34d0ad986`。其他日志摘要、冻结程序身份、结果范围和整理前工作区清单保留在 `target/roadmap-one-hour-final-evidence-20261031.json`。

Q10/Q45 是非标准小数据的 **relational_core** 定向用例：明确去除外层排序和行数限制。它们既不是原始 SQL 的排序/Top-K 认证，也不是全套 TPC、标准规模或受控性能认证。历史整套查询结果继续对应历史提交和可执行程序，不能重新标为当前源码的整套通过。算子桶访问次数也不能证明源读取、状态复制、物化和落盘全部与变化量成正比。

被忽略的昂贵用例没有在最近完整门禁中执行，不计入通过项。本次整理只改变文档、索引及保留原有注释差异，不改变执行逻辑；不以整理动作冒充新的完整运行时验证。发布前额外执行的格式、文档和差异检查另行记录。

## 4. 尚未完成的全部台账项

| ID / 当前状态 | 未完成内容 |
| --- | --- |
| R03d / pending | 可撤销 VARIANCE/STDDEV 数值状态、删除稳定性，及 Q17/Q39 独立验证。 |
| R03e / pending | 浮点聚合、舍入和特殊值契约及多批次验证；Q35 浮点输出 AVG 仍受此门禁约束。 |
| R04b / in_progress | Mark join 的完整三值/相关作用域验收及 Q10/Q35/Q45 全部条件；当前子能力和 Q10/Q45 定向复验不等于整项完成。 |
| R04c / pending | 非等值连接、范围索引、边界移动与删除的影响范围证明。 |
| R05a / pending | 窗口专用物理维护：分区累加器、有限 frame 有序范围、排名位置更新，以及并列与恢复语义。现有受影响分区重算不等于该项完成。 |
| R05b / pending | 无分区窗口、排序、Top-K 有序状态、并列规则、NULL 顺序和删除补位。 |
| R06 / pending | 可扩展的分组/关联键、分区、范围及全局依赖描述，传播阈值变化对未改动源 key 的影响。 |
| R07a / pending | 用变化块与 delta 日志替代完整变化关系的检查点写入，并保持崩溃恢复原子性。 |
| R07b / pending | 持久化并恢复算子状态、规则身份、兼容版本与有界回放；当前重开仍可能重建派生状态。 |
| R07c / pending | 请求索引、历史保留和压缩，保证重试、回放与恢复仍可达。 |
| R08 / pending | 读取变化文件或原生变更，独立验证 delete-file 与来源身份；不能把完整端点差分叫作原生增量摄取。 |
| R09a / pending | 基于实际状态规模和变化比例选择合格物理算法，严格模式不能静默选择完整重算。 |
| R09b / pending | 可复用执行模板，降低重复 DataFusion 计划和物化成本，保持 epoch 隔离。 |
| R10a / pending | 全链路固定变化量扩展测试，分别约束源读取、状态复制、物化与写盘，补齐现有算子局部证据之外的范围。 |
| R10b / pending | 长时间随机 SQL/恢复循环，混合事务、重复/NULL/键移动、热点、窗口并列、丢弃候选及崩溃点。 |
| R10c / pending | 固定硬件、数据、程序身份、独立状态和多变化比例的全量/增量性能对照。 |
| R11 / pending | Google Rust 规范、关键内部类型/方法契约、两配置静态与运行门禁和源码所有权的持续检查；本次保留的 DML 私有注释删减需纳入后续审阅。 |
| R12 / pending | MV-on-MV 依赖图、只读快照、在线初始化、DDL 演进与约束；分布式事务仍在当前本地协议范围之外。 |

建议恢复顺序：先完成 R04b/R06 的剩余依赖与语义门禁，并处理 R03e/Q35；再推进 R07/R08 的全链路增量化。范围/窗口/Top-K 与性能验收保留独立设计和证明，不通过删除 SQL 语义算子来扩大生产能力声明。

## 5. 本次纳入提交的全部现有工作区改动

整理前清单已记录在 `target/publish-main-20261031/before.json`，包括逐文件大小和 SHA-256。以下 14 个文件全部保留并纳入提交；另新增本状态记录，更新 ROADMAP 交付说明和文档索引。

| 原工作区状态 | 文件 | 内容及处理 |
| --- | --- | --- |
| modified | `docs/src/SUMMARY.md` | 原有九个设计/报告链接；全部保留，补本记录入口。 |
| modified | `docs/src/incremental/rule_rewrite.md` | 旧解释器方案被 Optd 计划执行方案取代的说明。 |
| modified | `docs/src/incremental/rule_rewrite_usage.md` | Optd 逻辑/物理计划、扩展规则、状态及执行方式的使用说明。 |
| modified | `docs/src/incremental/scoped_snapshots.md` | scoped snapshot 展开为显式 Optd 操作的衔接说明。 |
| modified | `optd-incremental-sql/src/database/dml_compiler.rs` | 已有差异为 1 行注释增加、22 行注释删除；原样保留，没有执行语义变化，没有还原或隐藏该差异。 |
| untracked | `docs/src/incremental/dbsp_python.md` | Python DBSP 原型设计与 TPC 实验范围。 |
| untracked | `docs/src/incremental/python_optimizer.md` | Python 通用关系优化器和 DBSP 后端设计。 |
| untracked | `docs/src/incremental/optd_plan_execution.md` | 复用 Optd 的规则/执行分离架构设计。 |
| untracked | `docs/src/incremental/optd_plan_qualification.md` | 对应架构的历史本地验证记录。 |
| untracked | `docs/src/incremental/tpc_incremental_efficiency.md` | relational_core 变体及聚合/连接维护设计。 |
| untracked | `docs/src/incremental/tpc_incremental_efficiency_qualification.md` | 对应历史正确性、严格计划与效率边界报告。 |
| untracked | `docs/src/incremental/sql_database_loop.md` | SQL 表、同步 MV、事务及本地主机发布/恢复设计。 |
| untracked | `docs/src/incremental/sql_database_qualification.md` | SQL 使用、历史查询结果、故障与剩余验收记录。 |
| untracked | `docs/src/incremental/rust_refactor.md` | Google Rust 规范、模块重构、注释审阅及历史门禁记录。 |

上述历史文档只补充指向当前状态的说明；原始提交、命令、失败和未覆盖范围保持原样。`target/` 以及已有 `/tmp/` 下构建目录、日志、数据库和基准产物保留本地，不强制加入 Git；本记录与 ROADMAP 保存其用途、关键结果、定位和证据摘要。任何本地证据文件缺失时，应标记不可复验，不能推断通过。

## 6. 发布检查记录

本次整理实际执行的检查如下，均通过；没有重复完整语义/基准套件，上一节的 324/338 项结果仍对应已记录的那次完整运行。

| 本次检查命令 | 结果 |
| --- | --- |
| 通过原环境脚本执行 `cargo +1.95.0 fmt --all -- --check` | exit 0。 |
| 设置 `MDBOOK_BUILD__BUILD_DIR=target/publish-main-20261031/book` 后执行 `mdbook build docs` | exit 0；日志为 `target/publish-main-20261031/mdbook.log`。 |
| `git diff --check` | exit 0。 |

本次新提交前检查 author/committer 为上述 Gmail 身份、提交信息不超过 2048 字符及 UTF-8 字节、暂存区完整包含上表文件，且无意外文件加入。公共发布检查未发现待交付文件中的凭据字面量、私钥、企业邮箱或内部服务 URL；这属于有界模式检查，不是完整安全认证。

Git 全局 hook 会触发额外分析/上传。本次仅对 Git 提交/推送命令设置 `core.hooksPath=/dev/null`，不修改全局配置；所需格式、文档和既有测试证据单独检查。不将 hook 执行或额外目标上传列为交付结果。

发布后必须以 `git ls-remote origin refs/heads/main` 核对远端 SHA 与本地 HEAD 一致，并确认工作区无遗漏。本次不创建 PR、不强推、不部署；持续开发目标仍保持暂停。
