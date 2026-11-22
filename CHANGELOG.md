# Changelog

Notable SparkScope changes are recorded below. Features are listed as released only after their distributions are published.

## [0.4.0] — Execution-plan inspection and comparison

### Added
- Optional DataFrame argument to the Python API: `analyze(spark, dataframe=df)`.
- Read-only collection of simple DataFrame explain output; extraction and counting of selected physical-plan operators.
- Operator observations for joins, exchanges, Python evaluation, and adaptive-plan status.
- Conservative plan findings for Cartesian products, broadcast nested-loop joins, sort-merge join observations, and Python evaluation operators.
- Before/after comparison API: `compare(before_report, after_report)`.
- `PlanComparison.show()` and `PlanComparison.to_dict()` for operator-count changes, added/resolved findings, and comparability warnings.
- Plan parser regression tests, example scripts, and Spark integration tests.

### Notes and limitations
- Plan comparison is structural at the recognized operator-count level; it is not a plan-tree diff or a speedup benchmark.
- Explain-text parsing varies with Spark versions; unsupported operators may be omitted.
- Adaptive Query Execution can change an initially planned strategy.
- No automatic job execution, opt-in runtime benchmarking, event-log metrics, or Spark Connect-only support in this release.
- Existing `analyze(spark)`, JSON CLI commands, and configuration/workload analysis remain supported.

## [0.3.1] — Documentation patch

### Fixed
- Corrected the PyPI description, installation instructions, and release status to reflect the shipped v0.3 functionality.
- Clarified supported interfaces and current limitations.

### Notes
- Documentation and metadata change; no intended analyzer behavior change.

## [0.3.0] — Automatic SparkSession configuration analysis

### Added
- Read-only selected configuration collection for classic PySpark SparkSessions.
- Public `analyze(spark)` API and `SparkReport` with terminal display and JSON-compatible output.
- Optional PySpark dependency extra, collector/API tests, and local Spark integration examples.

### Improved
- More reliable missing-value handling and validation for AQE, executor cores, shuffle partitions, and serializer settings.
- Join-type-aware broadcast screening and missing join-key metadata finding.
- Preserved both JSON-based CLI commands.

### Removed
- Misleading storage partition-column/join-key mismatch rule.

## [0.2.0] — Join and partition metadata analyzer

### Added
- Workload and join metadata models and JSON analysis.
- Preliminary broadcast-candidate and large-unpartitioned-table heuristics.
- `sparkscope analyze-workload` CLI, examples, and tests.

## [0.1.0] — Initial configuration analyzer

### Added
- JSON-based Spark configuration analysis, findings, severity labels, and terminal CLI.
- Initial AQE, shuffle-partition, executor-core, and serializer checks.
- Sample inputs and tests.

See [GitHub releases](https://github.com/vincentkanka/sparkscope/releases) and [PyPI](https://pypi.org/project/sparkscope/) for published artifacts.
