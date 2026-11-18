
from dataclasses import dataclass, field
from typing import Any


SQL_CONFIG_KEYS = (
    "spark.sql.adaptive.enabled",
    "spark.sql.shuffle.partitions",
)

SPARK_CONTEXT_CONFIG_KEYS = (
    "spark.executor.cores",
    "spark.serializer",
)


@dataclass(frozen=True)
class SparkSnapshot:
    spark_version: str
    config: dict[str, str]
    sources: dict[str, str] = field(default_factory=dict)


def collect_spark_config(spark: Any) -> SparkSnapshot:
    """
    Collect selected Spark configuration settings.

    Read-only: does not modify the session or execute jobs.
    Supports classic PySpark SparkSession objects.
    """
    if spark is None:
        raise ValueError("A SparkSession is required.")

    try:
        version = str(spark.version)
        context = spark.sparkContext
        context_config = dict(context.getConf().getAll())
        runtime_config = spark.conf
    except (AttributeError, TypeError, RuntimeError) as exc:
        raise ValueError(
            "SparkScope requires a classic PySpark SparkSession "
            "with an accessible SparkContext."
        ) from exc

    config: dict[str, str] = {}
    sources: dict[str, str] = {}

    # Collect runtime-resolved SQL settings.
    for key in SQL_CONFIG_KEYS:
        try:
            value = runtime_config.get(key)
        except Exception:
            # If Spark cannot resolve a setting, don't invent it.
            continue

        if value is not None:
            config[key] = str(value)
            sources[key] = "spark.conf"

    # Collect explicitly configured SparkContext settings.
    for key in SPARK_CONTEXT_CONFIG_KEYS:
        value = context_config.get(key)

        if value is not None:
            config[key] = str(value)
            sources[key] = "spark.sparkContext.getConf"

    return SparkSnapshot(
        spark_version=version,
        config=config,
        sources=sources,
    )
