
import json

import pytest

from sparkscope import SparkReport, analyze
from sparkscope.spark_collector import collect_spark_config


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkScopeIntegrationTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2000")
        .getOrCreate()
    )

    try:
        yield session
    finally:
        session.stop()


def test_collects_real_spark_configuration(spark):
    snapshot = collect_spark_config(spark)

    assert snapshot.spark_version == spark.version
    assert snapshot.config["spark.sql.adaptive.enabled"] == "false"
    assert snapshot.config["spark.sql.shuffle.partitions"] == "2000"

    assert snapshot.sources["spark.sql.adaptive.enabled"] == (
        "spark.conf"
    )


def test_analyzes_real_spark_session(spark):
    report = analyze(spark)

    assert isinstance(report, SparkReport)
    assert report.spark_version == spark.version

    rule_ids = {finding.rule_id for finding in report.findings}

    assert "AQE_DISABLED" in rule_ids
    assert "SHUFFLE_PARTITIONS_HIGH" in rule_ids

    assert report.total_findings >= 2


def test_real_spark_report_is_json_serializable(spark):
    report = analyze(spark)

    serialized = json.dumps(report.to_dict())
    restored = json.loads(serialized)

    assert restored["spark_version"] == spark.version
    assert restored["total_findings"] >= 2


def test_analysis_does_not_change_spark_configuration(spark):
    before_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    before_partitions = spark.conf.get(
        "spark.sql.shuffle.partitions"
    )

    analyze(spark)

    assert spark.conf.get("spark.sql.adaptive.enabled") == (
        before_aqe
    )
    assert spark.conf.get("spark.sql.shuffle.partitions") == (
        before_partitions
    )
