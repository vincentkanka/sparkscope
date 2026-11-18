
from sparkscope.analyzer import analyze_config
from sparkscope.spark_collector import collect_spark_config


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

    def __init__(self, sql_config, context_config):
        self.conf = FakeRuntimeConfig(sql_config)
        self.sparkContext = FakeSparkContext(context_config)


def test_collector_reads_selected_configurations():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": "2000",
            "spark.sql.warehouse.dir": "/tmp/warehouse",
        },
        context_config={
            "spark.executor.cores": "8",
            "spark.serializer": (
                "org.apache.spark.serializer.JavaSerializer"
            ),
        },
    )

    snapshot = collect_spark_config(spark)

    assert snapshot.spark_version == "3.5.0"
    assert len(snapshot.config) == 4
    assert snapshot.config["spark.sql.adaptive.enabled"] == "false"
    assert snapshot.config["spark.executor.cores"] == "8"

    # Unrelated settings should not be collected.
    assert "spark.sql.warehouse.dir" not in snapshot.config


def test_collected_config_works_with_existing_analyzer():
    spark = FakeSparkSession(
        sql_config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": "2000",
        },
        context_config={
            "spark.executor.cores": "8",
        },
    )

    snapshot = collect_spark_config(spark)
    findings = analyze_config(snapshot.config)

    rule_ids = {finding.rule_id for finding in findings}

    assert "AQE_DISABLED" in rule_ids
    assert "SHUFFLE_PARTITIONS_HIGH" in rule_ids
    assert "EXECUTOR_CORES_HIGH" in rule_ids


def test_missing_values_are_not_invented():
    spark = FakeSparkSession(
        sql_config={},
        context_config={},
    )

    snapshot = collect_spark_config(spark)

    assert snapshot.config == {}
    assert analyze_config(snapshot.config) == []


def test_none_session_is_rejected():
    try:
        collect_spark_config(None)
    except ValueError as exc:
        assert "SparkSession is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
