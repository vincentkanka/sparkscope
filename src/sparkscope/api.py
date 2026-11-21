
from typing import Any

from sparkscope.analyzer import analyze_config
from sparkscope.plan_analyzer import (
    analyze_physical_plan,
    collect_physical_plan,
    summarize_plan,
)
from sparkscope.report import SparkReport
from sparkscope.spark_collector import collect_spark_config


def analyze(
    spark: Any,
    dataframe: Any | None = None,
) -> SparkReport:
    """
    Analyze SparkSession configuration and, optionally,
    a DataFrame physical execution plan.

    No DataFrame actions are executed by SparkScope.

    Parameters
    ----------
    spark:
        An existing classic PySpark SparkSession.

    dataframe:
        Optional PySpark DataFrame whose planned physical
        execution operators should be inspected.

    Returns
    -------
    SparkReport
        Combined configuration and physical-plan findings.
    """
    snapshot = collect_spark_config(spark)

    findings = analyze_config(snapshot.config)
    plan_summary = None

    if dataframe is not None:
        plan_text = collect_physical_plan(dataframe)
        plan_summary = summarize_plan(plan_text)

        findings.extend(
            analyze_physical_plan(plan_summary)
        )

    return SparkReport(
        spark_version=snapshot.spark_version,
        config=dict(snapshot.config),
        sources=dict(snapshot.sources),
        findings=findings,
        plan_summary=plan_summary,
    )
