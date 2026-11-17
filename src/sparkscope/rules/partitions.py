from sparkscope.models import Finding, WorkloadMetadata

LARGE_UNPARTITIONED_TABLE_THRESHOLD_MB = 10000

def check_shuffle_partitions(config: dict[str, str]) -> list[Finding]:
    raw_value = config.get("spark.sql.shuffle.partitions")

    if raw_value is None:
        return []

    try:
        partitions = int(raw_value)
    except (ValueError, TypeError):
        partitions = 0

    if partitions <= 0:
        return [
            Finding(
                rule_id="SHUFFLE_PARTITIONS_INVALID",
                severity="HIGH",
                title="Invalid shuffle partition count",
                description=(
                    "spark.sql.shuffle.partitions must be "
                    "a positive integer."
                ),
                recommendation=(
                    "Set a positive shuffle partition count, "
                    "then benchmark with representative workloads."
                ),
            )
        ]

    if partitions > 1000:
        return [
            Finding(
                rule_id="SHUFFLE_PARTITIONS_HIGH",
                severity="MEDIUM",
                title="Shuffle partition count may be excessive",
                description=(
                    f"Configured shuffle partitions: {partitions}. "
                    "Depending on shuffle size, this may increase "
                    "scheduling overhead and small-task counts."
                ),
                recommendation=(
                    "Review shuffle volume, AQE behavior, task metrics, "
                    "and partition sizes before changing this setting."
                ),
            )
        ]

    if partitions < 20:
        return [
            Finding(
                rule_id="SHUFFLE_PARTITIONS_LOW",
                severity="MEDIUM",
                title="Shuffle partition count may be too low",
                description=(
                    f"Configured shuffle partitions: {partitions}. "
                    "This may limit parallelism for some workloads."
                ),
                recommendation=(
                    "Evaluate partition counts against actual shuffle "
                    "volume and cluster resources."
                ),
            )
        ]

    return []

def check_large_unpartitioned_tables(workload: WorkloadMetadata) -> list[Finding]:
    findings: list[Finding] = []

    for table in workload.tables:
        if table.size_mb >= LARGE_UNPARTITIONED_TABLE_THRESHOLD_MB and not table.partition_columns:
            findings.append(
                Finding(
                    rule_id="LARGE_UNPARTITIONED_TABLE",
                    severity="MEDIUM",
                    title="Large table has no partition columns",
                    description=(
                        f"The table {table.name} is {table.size_mb:.1f} MB "
                        "but has no partition columns defined."
                    ),
                    recommendation=(
                        "Evaluate partitioning strategy based on common filter columns, "
                        "data freshness requirements, and query access patterns."
                    ),
                )
            )

    return findings