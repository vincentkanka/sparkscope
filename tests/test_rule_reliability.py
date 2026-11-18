
import pytest

from sparkscope.analyzer import analyze_config


def get_rule_ids(config):
    return {finding.rule_id for finding in analyze_config(config)}


def test_empty_config_has_no_false_positive_findings():
    assert analyze_config({}) == []


def test_aqe_missing_is_not_disabled():
    assert "AQE_DISABLED" not in get_rule_ids({})


def test_aqe_explicitly_disabled():
    assert "AQE_DISABLED" in get_rule_ids({
        "spark.sql.adaptive.enabled": "false"
    })


def test_aqe_invalid_value():
    assert "AQE_INVALID" in get_rule_ids({
        "spark.sql.adaptive.enabled": "maybe"
    })


def test_java_serializer_explicit():
    assert "JAVA_SERIALIZER_USED" in get_rule_ids({
        "spark.serializer": (
            "org.apache.spark.serializer.JavaSerializer"
        )
    })


def test_missing_serializer_does_not_raise_finding():
    assert "JAVA_SERIALIZER_USED" not in get_rule_ids({})


def test_kryo_serializer_is_not_flagged():
    assert "JAVA_SERIALIZER_USED" not in get_rule_ids({
        "spark.serializer": (
            "org.apache.spark.serializer.KryoSerializer"
        )
    })


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_invalid_executor_cores(value):
    assert "EXECUTOR_CORES_INVALID" in get_rule_ids({
        "spark.executor.cores": value
    })


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_invalid_shuffle_partitions(value):
    assert "SHUFFLE_PARTITIONS_INVALID" in get_rule_ids({
        "spark.sql.shuffle.partitions": value
    })


def test_valid_configuration_has_no_findings():
    config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.executor.cores": "4",
        "spark.serializer": (
            "org.apache.spark.serializer.KryoSerializer"
        ),
    }

    assert analyze_config(config) == []
