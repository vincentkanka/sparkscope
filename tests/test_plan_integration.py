
import pytest

from sparkscope import analyze


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkScopePlanIntegration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    try:
        yield session
    finally:
        session.stop()


def test_real_broadcast_join_plan(spark):
    from pyspark.sql.functions import broadcast

    left = spark.range(10).selectExpr("id AS join_id")
    right = spark.range(2).selectExpr("id AS join_id")

    joined = left.join(
        broadcast(right),
        on="join_id",
        how="inner",
    )

    report = analyze(spark, dataframe=joined)

    assert report.plan_summary is not None

    counts = report.plan_summary.operator_counts

    assert counts.get("BroadcastHashJoin", 0) >= 1


def test_real_cartesian_product_plan(spark):
    left = spark.range(2)
    right = spark.range(3)

    joined = left.crossJoin(right)

    report = analyze(spark, dataframe=joined)

    assert report.plan_summary is not None

    counts = report.plan_summary.operator_counts

    # Spark may choose CartesianProduct or a broadcast
    # nested-loop strategy depending on its optimizer.
    assert (
        counts.get("CartesianProduct", 0)
        + counts.get("BroadcastNestedLoopJoin", 0)
    ) >= 1


def test_plan_analysis_does_not_execute_rows(spark):
    from pyspark.sql.functions import broadcast

    left = spark.range(10)
    right = spark.range(2)

    joined = left.join(
        broadcast(right),
        on="id",
        how="inner",
    )

    report = analyze(spark, dataframe=joined)

    assert report.plan_summary is not None
    assert report.spark_version == spark.version


def test_compare_real_join_plans(spark):
    from pyspark.sql.functions import broadcast
    from sparkscope import compare

    left = spark.range(10000).selectExpr(
        "id AS join_id"
    )

    right = spark.range(100).selectExpr(
        "id AS join_id"
    )

    original = left.join(
        right,
        on="join_id",
    )

    with_broadcast = left.join(
        broadcast(right),
        on="join_id",
    )

    before = analyze(spark, dataframe=original)
    after = analyze(spark, dataframe=with_broadcast)

    result = compare(before, after)

    assert result.before_spark_version == spark.version
    assert result.after_spark_version == spark.version

    assert after.plan_summary is not None

    assert (
        after.plan_summary.operator_counts.get(
            "BroadcastHashJoin", 0
        ) >= 1
    )

    assert isinstance(result.to_dict(), dict)
