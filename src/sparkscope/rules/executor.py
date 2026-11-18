from sparkscope.models import Finding


def check_executor_cores(config: dict[str, str]) -> list[Finding]:
    raw_value = config.get("spark.executor.cores")

    if raw_value is None:
        return []

    try:
        cores = int(raw_value)
    except (ValueError, TypeError):
        cores = 0

    if cores <= 0:
        return [
            Finding(
                rule_id="EXECUTOR_CORES_INVALID",
                severity="HIGH",
                title="Invalid executor core count",
                description=(
                    "spark.executor.cores must be a positive integer."
                ),
                recommendation=(
                    "Configure a positive integer for executor cores."
                ),
            )
        ]

    if cores > 5:
        return [
            Finding(
                rule_id="EXECUTOR_CORES_HIGH",
                severity="MEDIUM",
                title="Executor core count may be high",
                description=(
                    f"Executor cores is set to {cores}. "
                    "Higher core counts may increase resource contention "
                    "for some workloads."
                ),
                recommendation=(
                    "Benchmark different executor sizes and review CPU, "
                    "memory, garbage collection, and cluster utilization. "
                    "This is a heuristic, not a universal limit."
                ),
            )
        ]

    return []


def check_serializer(config: dict[str, str]) -> list[Finding]:
    serializer = config.get("spark.serializer")

    if serializer is None:
        return []

    serializer = str(serializer).strip()

    if serializer == "org.apache.spark.serializer.KryoSerializer":
        return []

    if serializer != "org.apache.spark.serializer.JavaSerializer":
        return []

    return [
        Finding(
            rule_id="JAVA_SERIALIZER_USED",
            severity="LOW",
            title="JavaSerializer is configured",
            description=(
                "Java serialization may have higher overhead than Kryo "
                "for certain RDD workloads and complex objects."
            ),
            recommendation=(
                "Benchmark KryoSerializer where serialization overhead "
                "is significant. It may not improve DataFrame or SQL "
                "workloads that use Spark's internal binary format."
            ),
        )
    ]
