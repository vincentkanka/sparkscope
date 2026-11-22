# SparkScope

**Explainable Apache Spark and PySpark configuration and execution-plan diagnostics.**

SparkScope helps data engineers inspect selected Spark settings, analyze user-supplied table and join metadata, examine a DataFrame's planned physical operators, and compare two plans. Findings are **diagnostic observations and heuristics**, not measured performance improvements or automatic fixes.

- **Release:** v0.4.0
- **Python:** 3.11+
- **Source:** https://github.com/vincentkanka/sparkscope
- **Package:** https://pypi.org/project/sparkscope/
- **License:** Apache-2.0

## Features

| Capability | Available in v0.4.0 |
| --- | --- |
| Analyze Spark configuration JSON | Yes |
| Analyze supplied table and join metadata JSON | Yes |
| Inspect selected settings in a classic PySpark `SparkSession` | Yes |
| Report findings with severity and rule IDs | Yes |
| Inspect recognized DataFrame physical-plan operators | Yes |
| Show preliminary AQE final-plan status | Yes |
| Compare operator counts and finding changes between two reports | Yes |
| Run workloads or measure execution time | No |
| Parse event logs, task metrics, or quantify data skew | No |
| Automatically fix workloads or prove performance speedups | No |
| Support Spark Connect-only sessions | No |

## Installation

For configuration and workload-metadata JSON analysis, PySpark is not required:

```bash
python -m pip install 'sparkscope==0.4.0'
```

For classic PySpark session and DataFrame analysis:

```bash
python -m pip install 'sparkscope[spark]==0.4.0'
```

The Spark extra declares `pyspark>=3.5,<5`. Running local Spark also requires an appropriate Java runtime. Declared dependency compatibility does not mean every Spark/Java combination has been individually tested.

## Quick start: configuration inspection

```python
from pyspark.sql import SparkSession
from sparkscope import analyze

spark = SparkSession.builder.master('local[2]').appName('SparkScopeDemo').getOrCreate()
try:
    report = analyze(spark)
    report.show()
    payload = report.to_dict()
finally:
    spark.stop()
```

SparkScope reads a small allowlist of configuration keys: `spark.sql.adaptive.enabled`, `spark.sql.shuffle.partitions`, `spark.executor.cores`, and `spark.serializer`, when available. Missing settings are not assumed to be disabled. It does not collect arbitrary Spark configuration entries.

## Quick start: inspect a DataFrame plan

```python
from pyspark.sql import SparkSession
from sparkscope import analyze

spark = SparkSession.builder.master('local[2]').appName('SparkScopePlanDemo').getOrCreate()
try:
    left = spark.range(100).selectExpr('id AS customer_id')
    right = spark.range(10).selectExpr('id AS customer_id')
    joined = left.join(right, 'customer_id')

    report = analyze(spark, dataframe=joined)
    report.show()
    print(report.plan_summary.to_dict() if report.plan_summary else None)
finally:
    spark.stop()
```

The plan collector captures `DataFrame.explain(mode="simple")`. It recognizes selected physical operators, including `SortMergeJoin`, `BroadcastHashJoin`, `ShuffledHashJoin`, `BroadcastNestedLoopJoin`, `CartesianProduct`, `Exchange`, `BroadcastExchange`, `ReusedExchange`, `AQEShuffleRead`, `BatchEvalPython`, and `ArrowEvalPython`.

Diagnostic findings may flag Cartesian products, broadcast nested-loop joins, or Python evaluation operators for investigation. A sort-merge join is reported as an observation, not inherently a problem. An `Exchange` is counted but is not treated as inefficient by itself.

**Plan limitations:** Plan output is not a stable public machine-readable schema. Operator recognition is based on simple explain-text parsing; the number of operators is not a runtime measurement. Adaptive Query Execution may change the final plan. Inspect plans and compare versions carefully before drawing performance conclusions.

## Quick start: compare two plans

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from sparkscope import analyze, compare

spark = SparkSession.builder.master('local[2]').appName('SparkScopeCompareDemo').getOrCreate()
try:
    large = spark.range(10000).selectExpr('id AS customer_id')
    small = spark.range(100).selectExpr('id AS customer_id')

    before = analyze(spark, dataframe=large.join(small, 'customer_id'))
    after = analyze(spark, dataframe=large.join(broadcast(small), 'customer_id'))

    result = compare(before, after)
    result.show()
    print(result.to_dict())
finally:
    spark.stop()
```

`compare()` operates on two existing SparkScope reports. It reports recognized operator-count deltas, new/resolved rule findings, and cautions when Spark versions, selected settings, or adaptive-plan status affect comparability. Identical counts **do not** mean the plan trees are identical; changed counts **do not** demonstrate faster execution.

## CLI: configuration JSON

Create `spark_config.json`:

```json
{
  "spark.sql.adaptive.enabled": "false",
  "spark.sql.shuffle.partitions": "2000",
  "spark.executor.cores": "8",
  "spark.serializer": "org.apache.spark.serializer.JavaSerializer"
}
```

Run:

```bash
sparkscope analyze spark_config.json
```

## CLI: workload metadata JSON

Create `workload.json`:

```json
{
  "tables": [
    {
      "name": "transactions",
      "size_mb": 50000,
      "row_count": 200000000,
      "partition_columns": ["transaction_date"]
    },
    {
      "name": "customers",
      "size_mb": 50,
      "row_count": 100000,
      "partition_columns": []
    }
  ],
  "joins": [
    {
      "left_table": "transactions",
      "right_table": "customers",
      "join_type": "inner",
      "join_keys": ["customer_id"]
    }
  ]
}
```

Run:

```bash
sparkscope analyze-workload workload.json
```

Workload findings rely on **supplied metadata** rather than live table statistics. Broadcast heuristics depend on join type, supported build side, join keys, and approximate sizes; they are not recommendations to force a broadcast without further validation.

## Reports

`SparkReport` provides `show()`, `to_dict()`, `total_findings`, `severity_counts`, and an optional `plan_summary`. `PlanComparison` provides `show()`, `to_dict()`, `operator_changes`, `added_findings`, `resolved_findings`, `warnings`, and `has_changes`.

To serialize a report or comparison:

```python
import json

with open('sparkscope-report.json', 'w', encoding='utf-8') as handle:
    json.dump(report.to_dict(), handle, indent=2)
```

Review any exported report before sharing: selected settings, finding descriptions, and plan-derived information may disclose environment details.

## Development and testing

```bash
git clone https://github.com/vincentkanka/sparkscope.git
cd sparkscope
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,spark]'
python -m pytest -v
sparkscope analyze examples/sample_config.json
sparkscope analyze-workload examples/sample_workload.json
python examples/live_plan_analysis.py
python examples/compare_join_plans.py
```

Spark integration tests require a functioning local Spark and Java installation. Read-only `analyze()` inspects configuration and physical-plan text; it does not invoke DataFrame actions, although Spark planning can consult metadata or initialize Spark components. The project does not guarantee zero external side effects in every Spark deployment.

## Roadmap

- **v0.1.0:** Initial configuration analyzer.
- **v0.2.0:** Table/join workload metadata analysis.
- **v0.3.0:** Automatic classic SparkSession configuration inspection and structured Python API.
- **v0.3.1:** Documentation and package-metadata patch.
- **v0.4.0:** Physical-plan observations and before/after report comparison.
- **Future releases:** Opt-in runtime benchmarks; stronger parser and Spark-version compatibility validation; agent skills/MCP integration; possible task/event-log analysis.

## Feedback and contributions

Bugs, reproducible plan examples, and feedback are welcome through [GitHub Issues](https://github.com/vincentkanka/sparkscope/issues). Please redact credentials and sensitive query or data details from uploaded plans and reports.
