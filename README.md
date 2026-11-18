# SparkScope

**A lightweight, open-source toolkit for reviewing Apache Spark and PySpark performance configurations and workload metadata.**

SparkScope helps data engineers identify potentially inefficient settings and workload patterns, explain why they matter, and decide what to validate next. Its recommendations are **diagnostic heuristics**, not measured speedups or automatic fixes.

- **Latest public PyPI release:** `0.2.0`
- **Next release in development:** `0.3.0` — read-only inspection of a classic PySpark `SparkSession`, a simple Python API, and structured reporting
- **Repository:** https://github.com/vincentkanka/sparkscope
- **PyPI:** https://pypi.org/project/sparkscope/
- **License:** Apache-2.0

## Why SparkScope?

Spark performance tuning depends on data distribution, SQL plans, cluster resources, runtime settings, and workload characteristics. SparkScope provides a repeatable starting point for reviewing selected settings and manually supplied workload metadata. It **does not** currently measure job execution time, parse Spark event logs, or prove that a suggested change will improve performance.

## Capabilities and release status

| Capability | Published v0.2.0 | v0.3.0 development branch |
| --- | --- | --- |
| Analyze Spark configuration JSON | Yes | Yes — validation and missing-value handling improved |
| Analyze table/join metadata JSON | Yes | Yes — broadcast guidance is join-type-aware |
| Identify potential broadcast candidates | Yes — size heuristic | Yes — also checks allowable join build sides and supplied equi-join keys |
| Flag large tables without supplied partition columns | Yes | Yes |
| Warn on join-key/partition-column mismatch | Yes | Removed; it does not establish that a shuffle occurs |
| Inspect a running classic PySpark `SparkSession` | No | Yes — selected settings only |
| `from sparkscope import analyze` | No | Yes |
| `SparkReport.show()` and `SparkReport.to_dict()` | No | Yes |
| Execution-plan analysis | No | No |
| Event-log analysis, skew detection, shuffle metrics | No | No |
| HTML report or web UI | No | No |

**Important:** The v0.3.0 API is currently development-branch functionality. Installing `sparkscope` from PyPI without additional steps installs the latest published release, which may not yet include this API.

## Requirements

- Python **3.11 or newer**
- No PySpark dependency for the existing JSON-based CLI
- For the v0.3.0 live-session API: classic PySpark and an appropriately configured Java runtime
- Spark Connect-only sessions are not yet supported by the collector

## Installation

### Latest published release — JSON CLI

```bash
python -m pip install sparkscope
sparkscope --help
```

### v0.3.0 development branch — SparkSession analysis

Until v0.3.0 is published to PyPI, install directly from the development branch:

```bash
python -m pip install \
  'git+https://github.com/vincentkanka/sparkscope.git@feature/reliable-config-rules#egg=sparkscope[spark]'
```

For local development from a clone of that branch:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev,spark]'
```

Once v0.3.0 has been released and its optional dependency is verified, the intended installation command will be:

```bash
python -m pip install 'sparkscope[spark]>=0.3.0'
```

## Usage

### 1. Analyze Spark configuration JSON (v0.2.0 and later)

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

The terminal report includes severity, rule ID, description, and recommendation. For this sample, rules may flag disabled AQE, a potentially excessive shuffle partition count, executor cores, and Java serialization. **The exact severities differ between published v0.2.0 and the improved v0.3.0 branch.**

### 2. Analyze table and join metadata JSON (v0.2.0 and later)

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

The analyzer uses **user-supplied metadata**; it does not fetch table sizes or infer actual join strategies from Spark. Its broadcast screening threshold is a heuristic, not a safe memory limit. Verify findings against a physical plan and real workload measurements.

Repository examples are also available at `examples/sample_config.json` and `examples/sample_workload.json`.

### 3. Analyze a running SparkSession (v0.3.0 development only)

Install the development version with the optional `spark` extra as described above, then:

```python
from pyspark.sql import SparkSession
from sparkscope import analyze

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SparkScopeExample")
    .getOrCreate()
)

try:
    report = analyze(spark)
    report.show()

    print(report.severity_counts)
    print(report.total_findings)

    # JSON-compatible Python dictionary
    payload = report.to_dict()
finally:
    spark.stop()
```

The v0.3.0 collector reads only the following settings when accessible:

- `spark.sql.adaptive.enabled`
- `spark.sql.shuffle.partitions`
- `spark.executor.cores`
- `spark.serializer`

It includes the Spark version and records whether a setting came from `spark.conf` or the SparkContext configuration snapshot. It does **not** trigger DataFrame actions, modify Spark settings, or scan application data. Some settings may be absent from the collected snapshot; absence should not be interpreted as proof of a disabled feature.

### Report structure (v0.3.0 development)

`SparkReport` exposes:

| Field or method | Meaning |
| --- | --- |
| `spark_version` | Reported Spark version |
| `config` | Collected, selected Spark settings |
| `sources` | Where each setting was read from |
| `findings` | List of rule findings |
| `total_findings` | Number of findings |
| `severity_counts` | Counts for HIGH, MEDIUM, LOW |
| `show()` | Prints a readable terminal report |
| `to_dict()` | Returns a JSON-compatible dictionary |

To write a local JSON report in Python:

```python
import json

with open("sparkscope-report.json", "w", encoding="utf-8") as handle:
    json.dump(report.to_dict(), handle, indent=2)
```

Review exported settings before sharing reports; configuration values may reveal environment details.

## Current rule categories

### Configuration rules

- Adaptive Query Execution: explicitly disabled or invalid setting
- Shuffle partitions: invalid count or a potentially high/low value
- Executor cores: invalid count or a potentially high value
- Serialization: explicitly configured Java serializer versus Kryo

In the v0.3.0 branch, missing settings are no longer automatically treated as disabled or improperly configured. Threshold-based warnings are intentionally phrased as candidates for investigation.

### Workload metadata rules

- Potential broadcast candidates when supplied table sizes and join type permit consideration
- Large tables lacking supplied storage partition columns
- Missing join-key metadata for applicable join types (v0.3.0 branch)

A storage partition-column mismatch alone is **not** reliable evidence of a shuffle. The former mismatch rule is being removed in v0.3.0.

## Development and testing

```bash
git clone https://github.com/vincentkanka/sparkscope.git
cd sparkscope
git switch feature/reliable-config-rules

python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,spark]'

python -m pytest -v
sparkscope analyze examples/sample_config.json
sparkscope analyze-workload examples/sample_workload.json
python examples/live_analysis.py
```

The live Spark example requires a compatible Java runtime. Run real integration tests on supported environments before claiming compatibility with any particular Spark/Java combination.

## Roadmap

### v0.1.0 — Initial configuration analyzer (released)

Basic JSON configuration rules, terminal findings, CLI, tests, and examples.

### v0.2.0 — Workload metadata analyzer (released)

Table/join metadata models, broadcast-candidate screening, large-unpartitioned-table warnings, initial partition/join metadata checks, and the `analyze-workload` CLI.

### v0.3.0 — Automatic SparkSession analysis (in development)

More reliable configuration and join rules, read-only SparkSession collection, the `analyze(spark)` API, structured reports, and real local Spark integration tests.

### v0.4.0 — Proposed execution-plan inspection

Investigate read-only physical-plan inspection for DataFrames, plan-aware join recommendations, and evidence-based shuffle diagnostics. Scope and compatibility remain to be validated.

### Future (not implemented)

Spark event-log parsing; stage/task/skew and shuffle metrics; HTML reports; historical comparisons; configurable policies; reproducible performance benchmarks.

## Design principles and limitations

1. **Evidence over guesses.** A rule suggests what to investigate; it does not establish a performance improvement.
2. **No surprise execution.** Live configuration inspection does not run a Spark job.
3. **Minimal collection.** Read only selected settings required for current checks.
4. **Explainable findings.** Findings include descriptions and validation-oriented recommendations.
5. **Backward compatibility.** The existing JSON CLI continues to work.

SparkScope is experimental software. It is not a substitute for the Spark UI, physical plans, job metrics, or representative benchmarks. In particular, current SparkScope versions do not detect actual data skew, quantify shuffle overhead, provide trustworthy percentage speedup estimates, or optimize a pipeline automatically.

## Contributing

Bug reports, accurate rule proposals, reproducible workloads, tests, and documentation improvements are welcome. Please review [CONTRIBUTING.md](CONTRIBUTING.md) and open an [issue](https://github.com/vincentkanka/sparkscope/issues) before proposing major changes.

## License

Apache License 2.0. See [LICENSE](LICENSE).
