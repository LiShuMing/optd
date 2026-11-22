# Incremental Plan Text EXPLAIN & TPC-H / TPC-DS Plan Catalog

This document specifies the text-based incremental `EXPLAIN` interface in `optd-incremental-sql`
and archives the compiled incremental plan matrix across all **22 TPC-H** and **99 TPC-DS**
queries under both `original` SQL and `relational_core` (outer `ORDER BY` / `LIMIT` / `FETCH`
removed) modes.

> **Important scope note:** Schema-only plan compilation verifies strict incremental plan
> eligibility, operator strategies, state slots and snapshot dependencies without scanning data
> rows. It is **not** a substitute for runtime correctness qualification or controlled performance
> benchmarking (`R10c`). Original SQL and `relational_core` variants are always reported separately.

---

## 1. Text EXPLAIN Usage

### SQL Statements (`SqlSession::execute`)

* `EXPLAIN <SELECT ...>` — compiles the query against base-table schemas (zero row reads) and
  returns a single-column (`plan: Utf8`) `RecordBatch` containing the Unicode DAG tree.
* `EXPLAIN VERBOSE <SELECT ...>` — appends each node's deduplicated lowered `PhysicalStep` DAG
  (`%0`, `%1`, ...) below the logical DAG tree.
* `EXPLAIN ANALYZE ...` — explicitly refused (`use EXPLAIN or EXPLAIN VERBOSE`).
* Blocked/non-strict-eligible queries still render their complete DAG tree with inline `[BLOCKED]`
  markers and a `Blockers` summary footer.

### Rust API & CLI

* `IncrementalPlan::explain_tree()` / `IncrementalPlan::explain_detailed()`
* `PlanPreflight::explain_tree()`
* `SqlView::explain_query(sql, schemas, optimizer)` -> `QueryExplain`
* `SqlSession::explain_view_tree(name)` / `SqlSession::explain_view_detailed(name)` / `SqlSession::explain_query(query, verbose)`
* `examples/sql_database`: `--explain <view>`, `--explain-tree <view>`, `--explain-detailed <view>`, `--explain-query <query.sql>`, `--explain-query-detailed <query.sql>`

### Regenerating All 22 TPC-H + 99 TPC-DS Full Text Plans Locally

```sh
# 1. Extract pinned TPC-H SQL from repository SLT tests and fetch pinned TPC-DS v1.4.3 SQL
python3 optd-incremental-sql/benchmarks/tools/extract_repository_tpch.py
python3 optd-incremental-sql/benchmarks/tools/fetch_duckdb_sources.py

# 2. Dump all text plans + summary.json + SUMMARY.md (schema-only, < 1s)
cargo +1.95.0 run --locked -p optd-incremental-sql --example explain_benchmarks -- \
  optd-incremental-sql/benchmarks/tpch/manifest.json target/tpc-incremental-plans --mode both
cargo +1.95.0 run --locked -p optd-incremental-sql --example explain_benchmarks -- \
  optd-incremental-sql/benchmarks/tpcds/manifest.json target/tpc-incremental-plans --mode both
# Add --detailed to also emit lowered PhysicalStep DAGs per node.
```

---

## 2. Corpus-Wide Summary (22 TPC-H + 99 TPC-DS)

| Suite | Mode | Total | Strict Eligible | Blocked | Errors | Total Nodes | State Slots | Snapshot Deps | Strategy Breakdown |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| **TPC-H** | `original` | 22 | **4** (q06, q14, q17, q19) | 18 | 0 | 374 | 92 | 40 | AccumulatorDelta=29, IndexedDelta=51, MatchCountDelta=12, NativeDelta=264, OperatorRecompute=18 |
| **TPC-H** | `relational_core` | 22 | **22 / 22** | 0 | 0 | 356 | 92 | 4 | AccumulatorDelta=29, IndexedDelta=51, MatchCountDelta=12, NativeDelta=264 |
| **TPC-DS** | `original` | 99 | **5** (q09, q13, q48, q87, q88) | 94 | 0 | 3415 | 911 | 286 | AccumulatorDelta=258, IndexedDelta=521, KeyedRecompute=18, MatchCountDelta=106, NativeDelta=2400, OperatorRecompute=104, RankDelta=8 |
| **TPC-DS** | `relational_core` | 99 | **99 / 99** | 0 | 0 | 3311 | 911 | 78 | AccumulatorDelta=258, IndexedDelta=521, KeyedRecompute=18, MatchCountDelta=106, NativeDelta=2400, RankDelta=8 |

**Key structural takeaways:**

1. **100% strict incremental compilation in `relational_core`**: All **22/22 TPC-H** and **99/99 TPC-DS**
   queries compile with zero `OperatorRecompute` blockers and zero compilation errors.
2. **Every `original`-mode blocker is outer ordering / Top-K (`R05b`)**: Across all 121 queries,
   100% of `original`-mode refusals come exclusively from `builtin.ordered_recompute` (18 in TPC-H,
   90 in TPC-DS) and `builtin.limit_recompute` (14 in TPC-DS).
3. **Operator strategy mix in `relational_core`**:
   * `NativeDelta` dominates linear pipelines (74.2% of TPC-H nodes, 72.5% of TPC-DS nodes).
   * Stateful incremental operators cover all joins, aggregates and windows: `IndexedDelta` (51 / 521),
     `AccumulatorDelta` (29 / 258), `MatchCountDelta` (12 / 106), `KeyedRecompute` for partitioned
     windows (0 / 18 across 13 TPC-DS queries), and `RankDelta` for global `RANK()` (0 / 8 across
     TPC-DS `q44` and `q49`).
   * Guarded scalar-product snapshots (`SnapshotReadWhenChanged`) appear only where genuinely
     required by global scalar dependencies (2 TPC-H queries: `q11`, `q22`; 11 TPC-DS queries).

---

## 3. Representative Incremental Plans

### 3.1 TPC-H Q01 (`relational_core`) — Retractable Accumulator Pipeline

```text
Suite: tpch | Query: q01 | Mode: relational_core | Removed Clauses: ["outer_order_by"]
==============================================================================
Incremental Plan (strict_eligible=true, nodes=5, states=1, snapshots=0, blockers=0)
Strategies: AccumulatorDelta=1, NativeDelta=4
Logical DAG:
#4 Projection [NativeDelta] rule=builtin.projection@1
│  op: Projection: lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity) AS sum_qty, sum(lineitem.l_extendedprice) AS sum_base_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS sum_disc_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax) AS sum_charge, avg(lineitem.l_quantity) AS avg_qty, avg(lineitem.l_extendedprice) AS avg_price, avg(lineitem.l_discount) AS avg_disc, count(Int64(1)) AS count(*) AS count_order
└─ #3 Aggregate [AccumulatorDelta] rule=builtin.aggregate.accumulator@1 state=Accumulator(#3)
   │  op: Aggregate: groupBy=[[lineitem.l_returnflag, lineitem.l_linestatus]], aggr=[[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(__common_expr_1) AS sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(__common_expr_1 * (Decimal128(1,20,0) + lineitem.l_tax)) AS sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]]
   └─ #2 Projection [NativeDelta] rule=builtin.projection@1
      │  op: Projection: lineitem.l_extendedprice * (Decimal128(1,20,0) - lineitem.l_discount) AS __common_expr_1, lineitem.l_quantity, lineitem.l_extendedprice, lineitem.l_discount, lineitem.l_tax, lineitem.l_returnflag, lineitem.l_linestatus
      └─ #1 Filter [NativeDelta] rule=builtin.filter@1
         │  op: Filter: lineitem.l_shipdate <= Date32("1998-09-21")
         └─ #0 TableScan: lineitem [NativeDelta] rule=builtin.source.delta@1
               op: TableScan: lineitem projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate]
```

### 3.2 TPC-H Q03 (`original`) — Blocked Outer Top-K (`Sort` + `fetch=10`)

```text
Suite: tpch | Query: q03 | Mode: original | Removed Clauses: []
==============================================================================
Incremental Plan (strict_eligible=false, nodes=15, states=3, snapshots=2, blockers=1)
Strategies: AccumulatorDelta=1, IndexedDelta=2, NativeDelta=11, OperatorRecompute=1
Logical DAG:
#14 Sort [BLOCKED: OperatorRecompute] rule=builtin.ordered_recompute@1 ordered=true
│  op: Sort: revenue DESC NULLS FIRST, orders.o_orderdate ASC NULLS LAST, fetch=10
│  snapshot: #13(After, unconditional), #14(Before, unconditional)
│  blocker: [policy / builtin.ordered_recompute] retain ordered sequences and propagate sequence-only changes to consumers
└─ #13 Projection [NativeDelta] rule=builtin.projection@1
   │  op: Projection: lineitem.l_orderkey, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS revenue, orders.o_orderdate, orders.o_shippriority
   └─ #12 Aggregate [AccumulatorDelta] rule=builtin.aggregate.accumulator@1 state=Accumulator(#12)
      │  op: Aggregate: groupBy=[[lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority]], aggr=[[sum(lineitem.l_extendedprice * (Decimal128(1,20,0) - lineitem.l_discount)) AS sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]]
      └─ #11 Projection [NativeDelta] rule=builtin.projection@1
         │  op: Projection: orders.o_orderdate, orders.o_shippriority, lineitem.l_orderkey, lineitem.l_extendedprice, lineitem.l_discount
         └─ #10 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#10, Scoped, scope=JoinKeys, before=true, after=true)
            │  op: Inner Join: orders.o_orderkey = lineitem.l_orderkey
            ├─ #6 Projection [NativeDelta] rule=builtin.projection@1
            │  │  op: Projection: orders.o_orderkey, orders.o_orderdate, orders.o_shippriority
            │  └─ #5 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#5, Scoped, scope=JoinKeys, before=true, after=true)
            │     │  op: Inner Join: customer.c_custkey = orders.o_custkey
            │     ├─ #2 Projection [NativeDelta] rule=builtin.projection@1
            │     │  │  op: Projection: customer.c_custkey
            │     │  └─ #1 Filter [NativeDelta] rule=builtin.filter@1
            │     │     │  op: Filter: customer.c_mktsegment = Utf8View("BUILDING")
            │     │     └─ #0 TableScan: customer [NativeDelta] rule=builtin.source.delta@1
            │     │           op: TableScan: customer projection=[c_custkey, c_mktsegment]
            │     └─ #4 Filter [NativeDelta] rule=builtin.filter@1
            │        │  op: Filter: orders.o_orderdate < Date32("1995-03-15")
            │        └─ #3 TableScan: orders [NativeDelta] rule=builtin.source.delta@1
            │              op: TableScan: orders projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority]
            └─ #9 Projection [NativeDelta] rule=builtin.projection@1
               │  op: Projection: lineitem.l_orderkey, lineitem.l_extendedprice, lineitem.l_discount
               └─ #8 Filter [NativeDelta] rule=builtin.filter@1
                  │  op: Filter: lineitem.l_shipdate > Date32("1995-03-15")
                  └─ #7 TableScan: lineitem [NativeDelta] rule=builtin.source.delta@1
                        op: TableScan: lineitem projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate]
Blockers (1):
  - #14 Sort [policy / builtin.ordered_recompute]: retain ordered sequences and propagate sequence-only changes to consumers
      detail: ordered sequence is consumed by a semantic ordering/row-bound path; ordered state is not yet qualified
```

### 3.3 TPC-H Q21 (`relational_core`) — Multi-Join + `LeftSemi`/`LeftAnti` `MatchCountDelta` + Shared DAG Node

```text
Suite: tpch | Query: q21 | Mode: relational_core | Removed Clauses: ["outer_order_by", "outer_limit_offset"]
==============================================================================
Incremental Plan (strict_eligible=true, nodes=27, states=6, snapshots=0, blockers=0)
Strategies: AccumulatorDelta=1, IndexedDelta=3, MatchCountDelta=2, NativeDelta=21
Logical DAG:
#26 Projection [NativeDelta] rule=builtin.projection@1
│  op: Projection: supplier.s_name, count(Int64(1)) AS count(*) AS numwait
└─ #25 Aggregate [AccumulatorDelta] rule=builtin.aggregate.accumulator@1 state=Accumulator(#25)
   │  op: Aggregate: groupBy=[[supplier.s_name]], aggr=[[count(Int64(1))]]
   └─ #24 Projection [NativeDelta] rule=builtin.projection@1
      │  op: Projection: supplier.s_name
      └─ #23 LeftAnti Join [MatchCountDelta] rule=builtin.match_count_join@2 state=Keyed(#23, MatchCounts, scope=JoinKeys, before=true, after=true)
         │  op: LeftAnti Join: l1.l_orderkey = __correlated_sq_2.l_orderkey Filter: __correlated_sq_2.l_suppkey != l1.l_suppkey
         ├─ #20 LeftSemi Join [MatchCountDelta] rule=builtin.match_count_join@2 state=Keyed(#20, MatchCounts, scope=JoinKeys, before=true, after=true)
         │  │  op: LeftSemi Join: l1.l_orderkey = __correlated_sq_1.l_orderkey Filter: __correlated_sq_1.l_suppkey != l1.l_suppkey
         │  ├─ #16 Projection [NativeDelta] rule=builtin.projection@1
         │  │  │  op: Projection: supplier.s_name, l1.l_orderkey, l1.l_suppkey
         │  │  └─ #15 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#15, Scoped, scope=JoinKeys, before=true, after=true)
         │  │     │  op: Inner Join: supplier.s_nationkey = nation.n_nationkey
         │  │     ├─ #11 Projection [NativeDelta] rule=builtin.projection@1
         │  │     │  │  op: Projection: supplier.s_name, supplier.s_nationkey, l1.l_orderkey, l1.l_suppkey
         │  │     │  └─ #10 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#10, Scoped, scope=JoinKeys, before=true, after=true)
         │  │     │     │  op: Inner Join: l1.l_orderkey = orders.o_orderkey
         │  │     │     ├─ #6 Projection [NativeDelta] rule=builtin.projection@1
         │  │     │     │  │  op: Projection: supplier.s_name, supplier.s_nationkey, l1.l_orderkey, l1.l_suppkey
         │  │     │     │  └─ #5 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#5, Scoped, scope=JoinKeys, before=true, after=true)
         │  │     │     │     │  op: Inner Join: supplier.s_suppkey = l1.l_suppkey
         │  │     │     │     ├─ #0 TableScan: supplier [NativeDelta] rule=builtin.source.delta@1
         │  │     │     │     │     op: TableScan: supplier projection=[s_suppkey, s_name, s_nationkey]
         │  │     │     │     └─ #4 SubqueryAlias [NativeDelta] rule=builtin.alias@1
         │  │     │     │        │  op: SubqueryAlias: l1
         │  │     │     │        └─ #3 Projection [NativeDelta] rule=builtin.projection@1
         │  │     │     │           │  op: Projection: lineitem.l_orderkey, lineitem.l_suppkey
         │  │     │     │           └─ #2 Filter [NativeDelta] rule=builtin.filter@1
         │  │     │     │              │  op: Filter: lineitem.l_receiptdate > lineitem.l_commitdate
         │  │     │     │              └─ #1 TableScan: lineitem [NativeDelta] rule=builtin.source.delta@1
         │  │     │     │                    op: TableScan: lineitem projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate]
         │  │     │     └─ #9 Projection [NativeDelta] rule=builtin.projection@1
         │  │     │        │  op: Projection: orders.o_orderkey
         │  │     │        └─ #8 Filter [NativeDelta] rule=builtin.filter@1
         │  │     │           │  op: Filter: orders.o_orderstatus = Utf8View("F")
         │  │     │           └─ #7 TableScan: orders [NativeDelta] rule=builtin.source.delta@1
         │  │     │                 op: TableScan: orders projection=[o_orderkey, o_orderstatus]
         │  │     └─ #14 Projection [NativeDelta] rule=builtin.projection@1
         │  │        │  op: Projection: nation.n_nationkey
         │  │        └─ #13 Filter [NativeDelta] rule=builtin.filter@1
         │  │           │  op: Filter: nation.n_name = Utf8View("SAUDI ARABIA")
         │  │           └─ #12 TableScan: nation [NativeDelta] rule=builtin.source.delta@1
         │  │                 op: TableScan: nation projection=[n_nationkey, n_name]
         │  └─ #19 SubqueryAlias [NativeDelta] rule=builtin.alias@1
         │     │  op: SubqueryAlias: __correlated_sq_1
         │     └─ #18 SubqueryAlias [NativeDelta] rule=builtin.alias@1
         │        │  op: SubqueryAlias: l2
         │        └─ #17 TableScan: lineitem [NativeDelta] rule=builtin.source.delta@1
         │              op: TableScan: lineitem projection=[l_orderkey, l_suppkey]
         └─ #22 SubqueryAlias [NativeDelta] rule=builtin.alias@1
            │  op: SubqueryAlias: __correlated_sq_2
            └─ #21 SubqueryAlias [NativeDelta] rule=builtin.alias@1
               │  op: SubqueryAlias: l3
               └─ #3 Projection (shared, see above)
```

### 3.4 TPC-DS Q12 (`relational_core`) — Join + Accumulator + Partitioned Window (`KeyedRecompute`)

```text
Suite: tpcds | Query: q12 | Mode: relational_core | Removed Clauses: ["outer_order_by", "outer_limit_offset"]
==============================================================================
Incremental Plan (strict_eligible=true, nodes=13, states=4, snapshots=0, blockers=0)
Strategies: AccumulatorDelta=1, IndexedDelta=2, KeyedRecompute=1, NativeDelta=9
Logical DAG:
#12 Projection [NativeDelta] rule=builtin.projection@1
│  op: Projection: item.i_item_id, item.i_item_desc, item.i_category, item.i_class, item.i_current_price, sum(web_sales.ws_ext_sales_price) AS itemrevenue, CAST(sum(web_sales.ws_ext_sales_price) AS Float64) * Float64(100) / CAST(sum(sum(web_sales.ws_ext_sales_price)) PARTITION BY [item.i_class] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS Float64) AS revenueratio
└─ #11 WindowAggr [KeyedRecompute] rule=builtin.partitioned_window@1 state=Keyed(#11, Scoped, scope=WindowPartitions, before=true, after=true)
   │  op: WindowAggr: windowExpr=[[sum(sum(web_sales.ws_ext_sales_price)) PARTITION BY [item.i_class] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
   └─ #10 Aggregate [AccumulatorDelta] rule=builtin.aggregate.accumulator@1 state=Accumulator(#10)
      │  op: Aggregate: groupBy=[[item.i_item_id, item.i_item_desc, item.i_category, item.i_class, item.i_current_price]], aggr=[[sum(web_sales.ws_ext_sales_price)]]
      └─ #9 Projection [NativeDelta] rule=builtin.projection@1
         │  op: Projection: web_sales.ws_ext_sales_price, item.i_item_id, item.i_item_desc, item.i_current_price, item.i_class, item.i_category
         └─ #8 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#8, Scoped, scope=JoinKeys, before=true, after=true)
            │  op: Inner Join: web_sales.ws_sold_date_sk = date_dim.d_date_sk
            ├─ #4 Projection [NativeDelta] rule=builtin.projection@1
            │  │  op: Projection: web_sales.ws_sold_date_sk, web_sales.ws_ext_sales_price, item.i_item_id, item.i_item_desc, item.i_current_price, item.i_class, item.i_category
            │  └─ #3 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#3, Scoped, scope=JoinKeys, before=true, after=true)
            │     │  op: Inner Join: web_sales.ws_item_sk = item.i_item_sk
            │     ├─ #0 TableScan: web_sales [NativeDelta] rule=builtin.source.delta@1
            │     │     op: TableScan: web_sales projection=[ws_sold_date_sk, ws_item_sk, ws_ext_sales_price]
            │     └─ #2 Filter [NativeDelta] rule=builtin.filter@1
            │        │  op: Filter: item.i_category = Utf8View("Sports") OR item.i_category = Utf8View("Books") OR item.i_category = Utf8View("Home")
            │        └─ #1 TableScan: item [NativeDelta] rule=builtin.source.delta@1
            │              op: TableScan: item projection=[i_item_sk, i_item_id, i_item_desc, i_current_price, i_class, i_category]
            └─ #7 Projection [NativeDelta] rule=builtin.projection@1
               │  op: Projection: date_dim.d_date_sk
               └─ #6 Filter [NativeDelta] rule=builtin.filter@1
                  │  op: Filter: date_dim.d_date >= Date32("1999-02-22") AND date_dim.d_date <= Date32("1999-03-24")
                  └─ #5 TableScan: date_dim [NativeDelta] rule=builtin.source.delta@1
                        op: TableScan: date_dim projection=[d_date_sk, d_date]
```

### 3.5 TPC-DS Q44 (`relational_core`, excerpt) — Global `RankDelta` + Shared Scans

```text
Suite: tpcds | Query: q44 | Mode: relational_core | Removed Clauses: ["outer_order_by", "outer_limit_offset"]
==============================================================================
Incremental Plan (strict_eligible=true, nodes=39, states=7, snapshots=4, blockers=0)
Strategies: AccumulatorDelta=2, IndexedDelta=3, NativeDelta=32, RankDelta=2
Logical DAG:
#38 Projection [NativeDelta] rule=builtin.projection@1
│  op: Projection: asceding.rnk, i1.i_product_name AS best_performing, i2.i_product_name AS worst_performing
└─ #37 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#37, Scoped, scope=JoinKeys, before=true, after=true)
   │  op: Inner Join: descending.item_sk = i2.i_item_sk
   ├─ #35 Projection [NativeDelta] rule=builtin.projection@1
   │  │  op: Projection: asceding.rnk, descending.item_sk, i1.i_product_name
   │  └─ #34 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#34, Scoped, scope=JoinKeys, before=true, after=true)
   │     │  op: Inner Join: asceding.item_sk = i1.i_item_sk
   │     ├─ #31 Projection [NativeDelta] rule=builtin.projection@1
   │     │  │  op: Projection: asceding.item_sk, asceding.rnk, descending.item_sk
   │     │  └─ #30 Inner Join [IndexedDelta] rule=builtin.inner_join@1 state=Keyed(#30, Scoped, scope=JoinKeys, before=true, after=true)
   │     │     │  op: Inner Join: asceding.rnk = descending.rnk
   │     │     ├─ #19 SubqueryAlias [NativeDelta] rule=builtin.alias@1
   │     │     │  │  op: SubqueryAlias: asceding
   │     │     │  └─ #18 SubqueryAlias [NativeDelta] rule=builtin.alias@1
   │     │     │     │  op: SubqueryAlias: v11
   │     │     │     └─ #17 Projection [NativeDelta] rule=builtin.projection@1
   │     │     │        │  op: Projection: v1.item_sk, rank() ORDER BY [v1.rank_col ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS rnk
   │     │     │        └─ #16 Filter [NativeDelta] rule=builtin.filter@1
   │     │     │           │  op: Filter: rank() ORDER BY [v1.rank_col ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW < UInt64(11)
   │     │     │           └─ #15 Projection [NativeDelta] rule=builtin.projection@1
   │     │     │              │  op: Projection: v1.item_sk, rank() ORDER BY [v1.rank_col ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
   │     │     │              └─ #14 WindowAggr [RankDelta] rule=builtin.window.rank@1 state=Rank(#14)
   │     │     │                 │  op: WindowAggr: windowExpr=[[rank() ORDER BY [v1.rank_col ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
   │     │     │                 └─ #13 SubqueryAlias [NativeDelta] rule=builtin.alias@1
   │     │     │                    │  op: SubqueryAlias: v1
   │     │     │                    └─ #12 Projection [NativeDelta] rule=builtin.projection@1
   │     │     │                       │  op: Projection: ss1.ss_item_sk AS item_sk, avg(ss1.ss_net_profit) AS rank_col
   │     │     │                       └─ #11 Inner Join [NativeDelta] rule=builtin.inner_join@1
   │     │     │                          │  op: Inner Join: Filter: CAST(avg(ss1.ss_net_profit) AS Decimal128(30, 15)) > CAST(Float64(0.9) * __scalar_sq_1.rank_col AS Decimal128(30, 15))
   │     │     │                          │  snapshot: #10(Before, unconditional), #4(After, unconditional)
   │     │     │                          ├─ #4 Aggregate [AccumulatorDelta] rule=builtin.aggregate.accumulator@1 state=Accumulator(#4)
   │     │     │                          │  │  op: Aggregate: groupBy=[[ss1.ss_item_sk]], aggr=[[avg(ss1.ss_net_profit)]]
   │     │     │                          │  └─ #3 SubqueryAlias [NativeDelta] rule=builtin.alias@1
   │     │     │                          │     │  op: SubqueryAlias: ss1
   │     │     │                          │     └─ #2 Projection [NativeDelta] rule=builtin.projection@1
   │     │     │                          │        │  op: Projection: store_sales.ss_item_sk, store_sales.ss_net_profit
   │     │     │                          │        └─ #1 Filter [NativeDelta] rule=builtin.filter@1
   │     │     │                          │           │  op: Filter: store_sales.ss_store_sk = Int64(4)
   │     │     │                          │           └─ #0 TableScan: store_sales [NativeDelta] rule=builtin.source.delta@1
... (43 more lines; see local dump)
```

### 3.6 TPC-DS Q09 (`original`, excerpt) — Strict-Eligible Guarded Scalar Products (`SnapshotReadWhenChanged`)

```text
Suite: tpcds | Query: q09 | Mode: original | Removed Clauses: []
==============================================================================
Incremental Plan (strict_eligible=true, nodes=87, states=15, snapshots=30, blockers=0)
Strategies: AccumulatorDelta=15, NativeDelta=72
Logical DAG:
#86 Projection [NativeDelta] rule=builtin.projection@1
│  op: Projection: CASE WHEN __scalar_sq_1.count(*) > Int64(74129) THEN __scalar_sq_2.avg(store_sales.ss_ext_discount_amt) ELSE __scalar_sq_3.avg(store_sales.ss_net_paid) END AS bucket1, CASE WHEN __scalar_sq_4.count(*) > Int64(122840) THEN __scalar_sq_5.avg(store_sales.ss_ext_discount_amt) ELSE __scalar_sq_6.avg(store_sales.ss_net_paid) END AS bucket2, CASE WHEN __scalar_sq_7.count(*) > Int64(56580) THEN __scalar_sq_8.avg(store_sales.ss_ext_discount_amt) ELSE __scalar_sq_9.avg(store_sales.ss_net_paid) END AS bucket3, CASE WHEN __scalar_sq_10.count(*) > Int64(10097) THEN __scalar_sq_11.avg(store_sales.ss_ext_discount_amt) ELSE __scalar_sq_12.avg(store_sales.ss_net_paid) END AS bucket4, CASE WHEN __scalar_sq_13.count(*) > Int64(165306) THEN __scalar_sq_14.avg(store_sales.ss_ext_discount_amt) ELSE __scalar_sq_15.avg(store_sales.ss_net_paid) END AS bucket5
└─ #85 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  op: Left Join:
   │  snapshot: #84(Before, trigger=#80), #80(After, trigger=#84)
   ├─ #80 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  op: Left Join:
   │  │  snapshot: #79(Before, trigger=#75), #75(After, trigger=#79)
   │  ├─ #75 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  op: Left Join:
   │  │  │  snapshot: #74(Before, trigger=#69), #69(After, trigger=#74)
   │  │  ├─ #69 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  │  op: Left Join:
   │  │  │  │  snapshot: #68(Before, trigger=#64), #64(After, trigger=#68)
   │  │  │  ├─ #64 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  │  │  op: Left Join:
   │  │  │  │  │  snapshot: #63(Before, trigger=#59), #59(After, trigger=#63)
   │  │  │  │  ├─ #59 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  │  │  │  op: Left Join:
   │  │  │  │  │  │  snapshot: #58(Before, trigger=#53), #53(After, trigger=#58)
   │  │  │  │  │  ├─ #53 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  │  │  │  │  op: Left Join:
   │  │  │  │  │  │  │  snapshot: #52(Before, trigger=#48), #48(After, trigger=#52)
   │  │  │  │  │  │  ├─ #48 Left Join [NativeDelta] rule=builtin.scalar_product@1
   │  │  │  │  │  │  │  │  op: Left Join:
   │  │  │  │  │  │  │  │  snapshot: #47(Before, trigger=#43), #43(After, trigger=#47)
   │  │  │  │  │  │  │  ├─ #43 Left Join [NativeDelta] rule=builtin.scalar_product@1
... (174 more lines; see local dump)
```

---

## 4. Complete TPC-H Matrix (22 Queries)

| Query | `original` | `relational_core` | Core Nodes | Core States | Core Snapshots | Core Strategies | Original Blockers |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| `q01` | blocked (1) | strict | 5 | 1 | 0 | AccumulatorDelta=1, NativeDelta=4 | builtin.ordered_recompute |
| `q02` | blocked (1) | strict | 30 | 9 | 0 | AccumulatorDelta=1, IndexedDelta=7, MatchCountDelta=1, NativeDelta=21 | builtin.ordered_recompute |
| `q03` | blocked (1) | strict | 14 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=11 | builtin.ordered_recompute |
| `q04` | blocked (1) | strict | 11 | 2 | 0 | AccumulatorDelta=1, MatchCountDelta=1, NativeDelta=9 | builtin.ordered_recompute |
| `q05` | blocked (1) | strict | 22 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=16 | builtin.ordered_recompute |
| `q06` | strict | strict | 5 | 1 | 0 | AccumulatorDelta=1, NativeDelta=4 | - |
| `q07` | blocked (1) | strict | 23 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=17 | builtin.ordered_recompute |
| `q08` | blocked (1) | strict | 32 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=7, NativeDelta=24 | builtin.ordered_recompute |
| `q09` | blocked (1) | strict | 21 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=15 | builtin.ordered_recompute |
| `q10` | blocked (1) | strict | 16 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=3, NativeDelta=12 | builtin.ordered_recompute |
| `q11` | blocked (1) | strict | 20 | 6 | 2 | AccumulatorDelta=2, IndexedDelta=4, NativeDelta=14 | builtin.ordered_recompute |
| `q12` | blocked (1) | strict | 8 | 2 | 0 | AccumulatorDelta=1, IndexedDelta=1, NativeDelta=6 | builtin.ordered_recompute |
| `q13` | blocked (1) | strict | 11 | 3 | 0 | AccumulatorDelta=2, MatchCountDelta=1, NativeDelta=8 | builtin.ordered_recompute |
| `q14` | strict | strict | 8 | 2 | 0 | AccumulatorDelta=1, IndexedDelta=1, NativeDelta=6 | - |
| `q15` | blocked (1) | strict | 15 | 4 | 0 | AccumulatorDelta=2, IndexedDelta=2, NativeDelta=11 | builtin.ordered_recompute |
| `q16` | blocked (1) | strict | 13 | 4 | 0 | AccumulatorDelta=2, IndexedDelta=1, MatchCountDelta=1, NativeDelta=9 | builtin.ordered_recompute |
| `q17` | strict | strict | 14 | 4 | 0 | AccumulatorDelta=2, IndexedDelta=1, MatchCountDelta=1, NativeDelta=10 | - |
| `q18` | blocked (1) | strict | 13 | 5 | 0 | AccumulatorDelta=2, IndexedDelta=2, MatchCountDelta=1, NativeDelta=8 | builtin.ordered_recompute |
| `q19` | strict | strict | 9 | 2 | 0 | AccumulatorDelta=1, IndexedDelta=1, NativeDelta=7 | - |
| `q20` | blocked (1) | strict | 23 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=1, MatchCountDelta=3, NativeDelta=18 | builtin.ordered_recompute |
| `q21` | blocked (1) | strict | 27 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=3, MatchCountDelta=2, NativeDelta=21 | builtin.ordered_recompute |
| `q22` | blocked (1) | strict | 16 | 3 | 2 | AccumulatorDelta=2, MatchCountDelta=1, NativeDelta=13 | builtin.ordered_recompute |

---

## 5. Complete TPC-DS Matrix (99 Queries)

| Query | `original` | `relational_core` | Core Nodes | Core States | Core Snapshots | Core Strategies | Original Blockers |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| `q01` | blocked (1) | strict | 26 | 6 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=1, NativeDelta=20 | builtin.ordered_recompute |
| `q02` | blocked (1) | strict | 26 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=21 | builtin.ordered_recompute |
| `q03` | blocked (1) | strict | 14 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=11 | builtin.ordered_recompute |
| `q04` | blocked (1) | strict | 64 | 20 | 0 | AccumulatorDelta=6, IndexedDelta=14, NativeDelta=44 | builtin.ordered_recompute |
| `q05` | blocked (1) | strict | 60 | 13 | 0 | AccumulatorDelta=6, IndexedDelta=6, MatchCountDelta=1, NativeDelta=47 | builtin.ordered_recompute |
| `q06` | blocked (1) | strict | 35 | 9 | 0 | AccumulatorDelta=3, IndexedDelta=5, MatchCountDelta=1, NativeDelta=26 | builtin.ordered_recompute |
| `q07` | blocked (1) | strict | 21 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=16 | builtin.ordered_recompute |
| `q08` | blocked (1) | strict | 31 | 8 | 0 | AccumulatorDelta=3, IndexedDelta=4, MatchCountDelta=1, NativeDelta=23 | builtin.ordered_recompute |
| `q09` | strict | strict | 87 | 15 | 30 | AccumulatorDelta=15, NativeDelta=72 | - |
| `q10` | blocked (1) | strict | 34 | 9 | 0 | AccumulatorDelta=1, IndexedDelta=2, MatchCountDelta=6, NativeDelta=25 | builtin.ordered_recompute |
| `q11` | blocked (1) | strict | 43 | 13 | 0 | AccumulatorDelta=4, IndexedDelta=9, NativeDelta=30 | builtin.ordered_recompute |
| `q12` | blocked (1) | strict | 13 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=2, KeyedRecompute=1, NativeDelta=9 | builtin.ordered_recompute |
| `q13` | strict | strict | 27 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=21 | - |
| `q14` | blocked (1) | strict | 101 | 32 | 6 | AccumulatorDelta=11, IndexedDelta=12, MatchCountDelta=9, NativeDelta=69 | builtin.ordered_recompute |
| `q15` | blocked (1) | strict | 13 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=3, NativeDelta=9 | builtin.ordered_recompute |
| `q16` | blocked (1) | strict | 29 | 7 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=2, NativeDelta=22 | builtin.ordered_recompute |
| `q17` | blocked (1) | strict | 29 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=7, NativeDelta=21 | builtin.ordered_recompute |
| `q18` | blocked (1) | strict | 40 | 11 | 0 | AccumulatorDelta=5, IndexedDelta=6, NativeDelta=29 | builtin.ordered_recompute |
| `q19` | blocked (1) | strict | 22 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=16 | builtin.ordered_recompute |
| `q20` | blocked (1) | strict | 13 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=2, KeyedRecompute=1, NativeDelta=9 | builtin.ordered_recompute |
| `q21` | blocked (1) | strict | 18 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=3, NativeDelta=14 | builtin.ordered_recompute |
| `q22` | blocked (1) | strict | 21 | 7 | 0 | AccumulatorDelta=5, IndexedDelta=2, NativeDelta=14 | builtin.ordered_recompute |
| `q23` | blocked (1) | strict | 67 | 19 | 2 | AccumulatorDelta=6, IndexedDelta=11, MatchCountDelta=2, NativeDelta=48 | builtin.ordered_recompute |
| `q24` | blocked (1) | strict | 37 | 12 | 2 | AccumulatorDelta=4, IndexedDelta=8, NativeDelta=25 | builtin.ordered_recompute |
| `q25` | blocked (1) | strict | 29 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=7, NativeDelta=21 | builtin.ordered_recompute |
| `q26` | blocked (1) | strict | 21 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=16 | builtin.ordered_recompute |
| `q27` | blocked (1) | strict | 37 | 10 | 0 | AccumulatorDelta=3, IndexedDelta=7, NativeDelta=27 | builtin.ordered_recompute |
| `q28` | blocked (11) | strict | 36 | 6 | 10 | AccumulatorDelta=6, NativeDelta=30 | builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute; builtin.limit_recompute |
| `q29` | blocked (1) | strict | 32 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=7, NativeDelta=24 | builtin.ordered_recompute |
| `q30` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=2, IndexedDelta=4, MatchCountDelta=1, NativeDelta=21 | builtin.ordered_recompute |
| `q31` | blocked (1) | strict | 64 | 23 | 0 | AccumulatorDelta=6, IndexedDelta=17, NativeDelta=41 | builtin.ordered_recompute |
| `q32` | blocked (1) | strict | 20 | 6 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=1, NativeDelta=14 | builtin.limit_recompute |
| `q33` | blocked (1) | strict | 50 | 16 | 0 | AccumulatorDelta=4, IndexedDelta=9, MatchCountDelta=3, NativeDelta=34 | builtin.ordered_recompute |
| `q34` | blocked (1) | strict | 23 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=18 | builtin.ordered_recompute |
| `q35` | blocked (1) | strict | 32 | 9 | 0 | AccumulatorDelta=1, IndexedDelta=2, MatchCountDelta=6, NativeDelta=23 | builtin.ordered_recompute |
| `q36` | blocked (1) | strict | 33 | 8 | 0 | AccumulatorDelta=4, IndexedDelta=3, KeyedRecompute=1, NativeDelta=25 | builtin.ordered_recompute |
| `q37` | blocked (1) | strict | 17 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=1, MatchCountDelta=2, NativeDelta=13 | builtin.ordered_recompute |
| `q38` | blocked (1) | strict | 30 | 12 | 0 | AccumulatorDelta=4, IndexedDelta=6, MatchCountDelta=2, NativeDelta=18 | builtin.limit_recompute |
| `q39` | blocked (1) | strict | 34 | 7 | 0 | AccumulatorDelta=2, IndexedDelta=5, NativeDelta=27 | builtin.ordered_recompute |
| `q40` | blocked (1) | strict | 18 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=3, MatchCountDelta=1, NativeDelta=13 | builtin.ordered_recompute |
| `q41` | blocked (1) | strict | 16 | 3 | 0 | AccumulatorDelta=2, MatchCountDelta=1, NativeDelta=13 | builtin.ordered_recompute |
| `q42` | blocked (1) | strict | 13 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=10 | builtin.ordered_recompute |
| `q43` | blocked (1) | strict | 13 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=10 | builtin.ordered_recompute |
| `q44` | blocked (1) | strict | 39 | 7 | 4 | AccumulatorDelta=2, IndexedDelta=3, NativeDelta=32, RankDelta=2 | builtin.ordered_recompute |
| `q45` | blocked (1) | strict | 23 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=4, MatchCountDelta=1, NativeDelta=17 | builtin.ordered_recompute |
| `q46` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=1, IndexedDelta=6, NativeDelta=21 | builtin.ordered_recompute |
| `q47` | blocked (1) | strict | 28 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=5, KeyedRecompute=2, NativeDelta=20 | builtin.ordered_recompute |
| `q48` | strict | strict | 20 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=15 | - |
| `q49` | blocked (1) | strict | 69 | 16 | 0 | AccumulatorDelta=4, IndexedDelta=6, NativeDelta=53, RankDelta=6 | builtin.ordered_recompute |
| `q50` | blocked (1) | strict | 19 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=14 | builtin.ordered_recompute |
| `q51` | blocked (1) | strict | 28 | 8 | 0 | AccumulatorDelta=2, IndexedDelta=2, KeyedRecompute=3, MatchCountDelta=1, NativeDelta=20 | builtin.ordered_recompute |
| `q52` | blocked (1) | strict | 14 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=11 | builtin.ordered_recompute |
| `q53` | blocked (1) | strict | 20 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=3, KeyedRecompute=1, NativeDelta=15 | builtin.ordered_recompute |
| `q54` | blocked (1) | strict | 51 | 12 | 4 | AccumulatorDelta=5, IndexedDelta=4, MatchCountDelta=3, NativeDelta=39 | builtin.ordered_recompute |
| `q55` | blocked (1) | strict | 13 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=2, NativeDelta=10 | builtin.ordered_recompute |
| `q56` | blocked (1) | strict | 50 | 16 | 0 | AccumulatorDelta=4, IndexedDelta=9, MatchCountDelta=3, NativeDelta=34 | builtin.ordered_recompute |
| `q57` | blocked (1) | strict | 28 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=5, KeyedRecompute=2, NativeDelta=20 | builtin.ordered_recompute |
| `q58` | blocked (1) | strict | 52 | 17 | 0 | AccumulatorDelta=3, IndexedDelta=8, MatchCountDelta=6, NativeDelta=35 | builtin.ordered_recompute |
| `q59` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=1, IndexedDelta=6, NativeDelta=21 | builtin.ordered_recompute |
| `q60` | blocked (1) | strict | 50 | 16 | 0 | AccumulatorDelta=4, IndexedDelta=9, MatchCountDelta=3, NativeDelta=34 | builtin.ordered_recompute |
| `q61` | blocked (1) | strict | 48 | 13 | 2 | AccumulatorDelta=2, IndexedDelta=11, NativeDelta=35 | builtin.ordered_recompute |
| `q62` | blocked (1) | strict | 19 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=14 | builtin.ordered_recompute |
| `q63` | blocked (1) | strict | 20 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=3, KeyedRecompute=1, NativeDelta=15 | builtin.ordered_recompute |
| `q64` | blocked (1) | strict | 108 | 37 | 0 | AccumulatorDelta=3, IndexedDelta=33, MatchCountDelta=1, NativeDelta=71 | builtin.ordered_recompute |
| `q65` | blocked (1) | strict | 22 | 6 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=1, NativeDelta=16 | builtin.ordered_recompute |
| `q66` | blocked (1) | strict | 36 | 11 | 0 | AccumulatorDelta=3, IndexedDelta=8, NativeDelta=25 | builtin.ordered_recompute |
| `q67` | blocked (1) | strict | 37 | 13 | 0 | AccumulatorDelta=9, IndexedDelta=3, KeyedRecompute=1, NativeDelta=24 | builtin.ordered_recompute |
| `q68` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=1, IndexedDelta=6, NativeDelta=21 | builtin.ordered_recompute |
| `q69` | blocked (1) | strict | 32 | 9 | 0 | AccumulatorDelta=1, IndexedDelta=2, MatchCountDelta=6, NativeDelta=23 | builtin.ordered_recompute |
| `q70` | blocked (1) | strict | 33 | 11 | 0 | AccumulatorDelta=4, IndexedDelta=4, KeyedRecompute=2, MatchCountDelta=1, NativeDelta=22 | builtin.ordered_recompute |
| `q71` | blocked (1) | strict | 26 | 6 | 0 | AccumulatorDelta=1, IndexedDelta=5, NativeDelta=20 | builtin.ordered_recompute |
| `q72` | blocked (1) | strict | 42 | 11 | 0 | AccumulatorDelta=1, IndexedDelta=8, MatchCountDelta=2, NativeDelta=31 | builtin.ordered_recompute |
| `q73` | blocked (1) | strict | 23 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=18 | builtin.ordered_recompute |
| `q74` | blocked (1) | strict | 43 | 13 | 0 | AccumulatorDelta=4, IndexedDelta=9, NativeDelta=30 | builtin.ordered_recompute |
| `q75` | blocked (1) | strict | 64 | 20 | 0 | AccumulatorDelta=4, IndexedDelta=10, MatchCountDelta=6, NativeDelta=44 | builtin.ordered_recompute |
| `q76` | blocked (1) | strict | 27 | 7 | 0 | AccumulatorDelta=1, IndexedDelta=6, NativeDelta=20 | builtin.ordered_recompute |
| `q77` | blocked (1) | strict | 67 | 21 | 2 | AccumulatorDelta=9, IndexedDelta=10, MatchCountDelta=2, NativeDelta=46 | builtin.ordered_recompute |
| `q78` | blocked (1) | strict | 42 | 11 | 0 | AccumulatorDelta=3, IndexedDelta=3, MatchCountDelta=5, NativeDelta=31 | builtin.ordered_recompute |
| `q79` | blocked (1) | strict | 22 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=17 | builtin.ordered_recompute |
| `q80` | blocked (1) | strict | 70 | 21 | 0 | AccumulatorDelta=6, IndexedDelta=12, MatchCountDelta=3, NativeDelta=49 | builtin.ordered_recompute |
| `q81` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=2, IndexedDelta=4, MatchCountDelta=1, NativeDelta=21 | builtin.ordered_recompute |
| `q82` | blocked (1) | strict | 17 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=1, MatchCountDelta=2, NativeDelta=13 | builtin.ordered_recompute |
| `q83` | blocked (1) | strict | 52 | 17 | 0 | AccumulatorDelta=3, IndexedDelta=8, MatchCountDelta=6, NativeDelta=35 | builtin.ordered_recompute |
| `q84` | blocked (1) | strict | 20 | 5 | 0 | IndexedDelta=5, NativeDelta=15 | builtin.ordered_recompute |
| `q85` | blocked (1) | strict | 31 | 8 | 0 | AccumulatorDelta=1, IndexedDelta=7, NativeDelta=23 | builtin.ordered_recompute |
| `q86` | blocked (1) | strict | 20 | 6 | 0 | AccumulatorDelta=3, IndexedDelta=2, KeyedRecompute=1, NativeDelta=14 | builtin.ordered_recompute |
| `q87` | strict | strict | 30 | 12 | 0 | AccumulatorDelta=4, IndexedDelta=6, MatchCountDelta=2, NativeDelta=18 | - |
| `q88` | strict | strict | 89 | 25 | 14 | AccumulatorDelta=8, IndexedDelta=17, NativeDelta=64 | - |
| `q89` | blocked (1) | strict | 18 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=3, KeyedRecompute=1, NativeDelta=13 | builtin.ordered_recompute |
| `q90` | blocked (1) | strict | 30 | 7 | 2 | AccumulatorDelta=2, IndexedDelta=5, NativeDelta=23 | builtin.ordered_recompute |
| `q91` | blocked (1) | strict | 28 | 7 | 0 | AccumulatorDelta=1, IndexedDelta=6, NativeDelta=21 | builtin.ordered_recompute |
| `q92` | blocked (1) | strict | 20 | 6 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=1, NativeDelta=14 | builtin.ordered_recompute |
| `q93` | blocked (1) | strict | 12 | 3 | 0 | AccumulatorDelta=1, IndexedDelta=1, MatchCountDelta=1, NativeDelta=9 | builtin.ordered_recompute |
| `q94` | blocked (1) | strict | 29 | 7 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=2, NativeDelta=22 | builtin.ordered_recompute |
| `q95` | blocked (1) | strict | 32 | 9 | 0 | AccumulatorDelta=2, IndexedDelta=3, MatchCountDelta=4, NativeDelta=23 | builtin.ordered_recompute |
| `q96` | blocked (1) | strict | 18 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=3, NativeDelta=14 | builtin.ordered_recompute |
| `q97` | blocked (1) | strict | 19 | 6 | 0 | AccumulatorDelta=3, MatchCountDelta=3, NativeDelta=13 | builtin.limit_recompute |
| `q98` | blocked (1) | strict | 13 | 4 | 0 | AccumulatorDelta=1, IndexedDelta=2, KeyedRecompute=1, NativeDelta=9 | builtin.ordered_recompute |
| `q99` | blocked (1) | strict | 19 | 5 | 0 | AccumulatorDelta=1, IndexedDelta=4, NativeDelta=14 | builtin.ordered_recompute |
