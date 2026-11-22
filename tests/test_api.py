
import json

import pytest

from sparkscope import SparkReport, analyze


class FakeSparkConf:
    def __init__(self, values):
        self.values = values

    def getAll(self):
        return list(self.values.items())


class FakeSparkContext:
    def __init__(self, values):
        self.values = values

    def getConf(self):
        return FakeSparkConf(self.values)


class FakeRuntimeConfig:
    def __init__(self, values):
        self.values = values

    def get(self, key):
        if key not in self.values:
            raise KeyError(key)
        return self.values[key]


class FakeSparkSession:
    version = "3.5.0"

    def __init__(self, sql_config=None, context_config=None):
        self.conf = FakeRuntimeConfig(sql_config or {})
        self.sparkContext = FakeSparkContext(
            context_config or {}
        )


def test_analyze_returns_structured_report():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": "2000",
        },
        context_config={
            "spark.executor.cores": "8",
        },
    )

    report = analyze(spark)

    assert isinstance(report, SparkReport)
    assert report.spark_version == "3.5.0"
    assert report.total_findings == 3
    assert report.severity_counts == {
        "HIGH": 1,
        "MEDIUM": 2,
        "LOW": 0,
    }

    rule_ids = {
        finding.rule_id for finding in report.findings
    }

    assert rule_ids == {
        "AQE_DISABLED",
        "SHUFFLE_PARTITIONS_HIGH",
        "EXECUTOR_CORES_HIGH",
    }


def test_report_can_be_serialized_to_json():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "false",
        }
    )

    report = analyze(spark)
    payload = report.to_dict()

    encoded = json.dumps(payload)
    decoded = json.loads(encoded)

    assert decoded["spark_version"] == "3.5.0"
    assert decoded["total_findings"] == 1
    assert decoded["findings"][0]["rule_id"] == "AQE_DISABLED"


def test_report_show_displays_findings(capsys):
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "false",
        }
    )

    report = analyze(spark)
    report.show()

    output = capsys.readouterr().out

    assert "SparkScope Performance Report" in output
    assert "Spark Version: 3.5.0" in output
    assert "Adaptive Query Execution is disabled" in output


def test_no_findings_report():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
        },
        context_config={
            "spark.executor.cores": "4",
        },
    )

    report = analyze(spark)

    assert report.total_findings == 0
    assert report.severity_counts["HIGH"] == 0


def test_analyze_rejects_missing_session():
    with pytest.raises(ValueError, match="SparkSession is required"):
        analyze(None)


def test_report_does_not_collect_unrelated_settings():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.warehouse.dir": "/tmp/warehouse",
        }
    )

    report = analyze(spark)

    assert "spark.sql.warehouse.dir" not in report.config
    assert report.sources["spark.sql.adaptive.enabled"] == "spark.conf"


def test_analyze_with_dataframe_includes_plan_summary():
    class FakeDataFrame:
        def explain(self, mode="simple"):
            print(
                "== Physical Plan ==\n"
                "+- CartesianProduct\n"
            )

    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
        }
    )

    report = analyze(
        spark,
        dataframe=FakeDataFrame(),
    )

    assert report.plan_summary is not None

    assert report.plan_summary.operator_counts[
        "CartesianProduct"
    ] == 1

    rule_ids = {
        finding.rule_id
        for finding in report.findings
    }

    assert "PLAN_CARTESIAN_PRODUCT" in rule_ids

    payload = report.to_dict()

    assert (
        payload["plan_summary"]["operator_counts"]
        ["CartesianProduct"] == 1
    )


def test_existing_api_remains_backward_compatible():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "true",
        }
    )

    report = analyze(spark)

    assert report.plan_summary is None
    assert "plan_summary" not in report.to_dict()
