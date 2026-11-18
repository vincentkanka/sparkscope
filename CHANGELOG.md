# Changelog

Significant changes to SparkScope are documented here. Released versions are listed separately from unreleased development work.

## [Unreleased] — Planned v0.3.0: Automatic SparkSession Analysis

**Status:** Implemented on the `feature/reliable-config-rules` development branch; not yet published to PyPI as v0.3.0. Release date to be added after validation and publication.

### Added

- Read-only configuration collection from a classic PySpark `SparkSession` using `collect_spark_config()`.
- Selected runtime SQL settings and SparkContext configuration values, with source tracking and Spark version capture.
- Public Python API: `from sparkscope import analyze`, followed by `report = analyze(spark)`.
- `SparkReport` with `show()`, `to_dict()`, `severity_counts`, and `total_findings`.
- Optional `spark` dependency extra for PySpark, while preserving a lightweight installation for the JSON CLI.
- Unit tests for the collector, API, report serialization, and missing-configuration behavior.
- Local SparkSession demonstration (`examples/live_analysis.py`).
- Real local Spark integration tests prepared for release validation.

### Changed

- Distinguish omitted configuration values from explicitly disabled settings.
- Validate invalid AQE, executor-core, and shuffle-partition values more accurately.
- Make Java serializer guidance conditional on an explicitly configured Java serializer and clarify its relevance to applicable workloads.
- Clarify that shuffle partition and executor-core thresholds are heuristics rather than universal limits.
- Make broadcast-candidate guidance aware of join type and eligible build side, when equi-join keys are supplied.
- Add a low-severity warning for missing join-key metadata for applicable join types.
- Preserve existing `sparkscope analyze` and `sparkscope analyze-workload` commands.

### Removed

- Join-key/storage-partition-column mismatch warning (`JOIN_KEY_PARTITION_MISMATCH`), because a mismatch alone cannot establish a Spark shuffle or a performance problem.

### Validation and limitations

- Existing configuration and workload CLI commands remain supported.
- Real SparkSession analysis currently covers **selected configuration settings only**; no automatic DataFrame-plan or event-log analysis.
- Spark Connect-only sessions are not supported by the classic SparkContext-based collector.
- v0.3.0 is **not** yet a published PyPI release; do not rely on `pip install sparkscope` alone for the development API.

## [0.2.0] — Join and Partition Metadata Analyzer

### Added

- Metadata models for tables, joins, and workloads.
- JSON workload metadata parser and analysis orchestration.
- Heuristic detection of possible broadcast join opportunities based on manually supplied table sizes.
- Large-unpartitioned-table metadata rule.
- Initial join-key/storage-partition-column mismatch diagnostic (superseded and removed in the upcoming v0.3.0).
- `sparkscope analyze-workload` CLI command.
- Example workload metadata and automated tests.

### Notes

Version 0.2.0 expanded SparkScope beyond static configuration JSON analysis. It did **not** introduce automatic table statistics collection, Spark execution-plan inspection, event-log processing, or measured speedup estimates.

## [0.1.0] — Initial Configuration Analyzer

### Added

- Initial configuration analysis engine and `Finding` model.
- Checks for AQE settings, shuffle partition counts, executor cores, and serializer configuration.
- Severity-labelled diagnostic findings and actionable recommendations.
- `sparkscope analyze` CLI command for configuration JSON files.
- Sample configuration file, unit tests, and initial project documentation.

### Notes

The initial rules were intentionally heuristic. Several missing-value and recommendation-accuracy issues are addressed in the upcoming v0.3.0 improvements.

---

For source code and release history, see [SparkScope on GitHub](https://github.com/vincentkanka/sparkscope) and [SparkScope on PyPI](https://pypi.org/project/sparkscope/).
