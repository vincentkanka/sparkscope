
from typing import Any

from sparkscope.analyzer import analyze_config
from sparkscope.report import SparkReport
from sparkscope.spark_collector import collect_spark_config


def analyze(spark: Any) -> SparkReport:
    """
    Analyze the configuration of an existing SparkSession.

    The analysis is read-only. It does not change Spark
    configuration, execute DataFrame actions, or submit jobs.

    Parameters
    ----------
    spark:
        A classic PySpark SparkSession.

    Returns
    -------
    SparkReport:
        Structured report containing configuration findings.
    """
    snapshot = collect_spark_config(spark)

    findings = analyze_config(snapshot.config)

    return SparkReport(
        spark_version=snapshot.spark_version,
        config=dict(snapshot.config),
        sources=dict(snapshot.sources),
        findings=findings,
    )
