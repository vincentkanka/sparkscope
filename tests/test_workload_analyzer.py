
import pytest

from sparkscope.models import (
    JoinMetadata,
    TableMetadata,
    WorkloadMetadata,
)
from sparkscope.workload_analyzer import analyze_workload


def make_workload(join_type="inner", small_on="right",
                  join_keys=None):
    if join_keys is None:
        join_keys = ["customer_id"]

    large = TableMetadata(
        name="transactions",
        size_mb=50000,
        row_count=200000000,
        partition_columns=["transaction_date"],
    )

    small = TableMetadata(
        name="customers",
        size_mb=50,
        row_count=100000,
        partition_columns=[],
    )

    if small_on == "right":
        left_name, right_name = "transactions", "customers"
    else:
        left_name, right_name = "customers", "transactions"

    return WorkloadMetadata(
        tables=[large, small],
        joins=[
            JoinMetadata(
                left_table=left_name,
                right_table=right_name,
                join_type=join_type,
                join_keys=join_keys,
            )
        ],
    )


def has_broadcast_finding(workload):
    findings = analyze_workload(workload)
    return any(
        f.rule_id == "BROADCAST_JOIN_OPPORTUNITY"
        for f in findings
    )


@pytest.mark.parametrize(
    "join_type,small_on,expected",
    [
        ("inner", "right", True),
        ("inner", "left", True),
        ("left_outer", "right", True),
        ("left_outer", "left", False),
        ("right_outer", "left", True),
        ("right_outer", "right", False),
        ("left_semi", "right", True),
        ("left_anti", "right", True),
        ("full_outer", "right", False),
    ],
)
def test_broadcast_respects_join_type(
    join_type, small_on, expected
):
    workload = make_workload(join_type, small_on)
    assert has_broadcast_finding(workload) == expected


def test_no_broadcast_without_join_keys():
    workload = make_workload(join_keys=[])
    assert not has_broadcast_finding(workload)


def test_missing_join_keys_detected():
    workload = make_workload(join_keys=[])

    findings = analyze_workload(workload)
    rule_ids = {f.rule_id for f in findings}

    assert "JOIN_KEYS_MISSING" in rule_ids


def test_partition_mismatch_not_reported():
    workload = make_workload()

    findings = analyze_workload(workload)
    rule_ids = {f.rule_id for f in findings}

    assert "JOIN_KEY_PARTITION_MISMATCH" not in rule_ids


def test_unknown_join_type_not_assumed_inner():
    workload = make_workload(join_type="custom_join")

    assert not has_broadcast_finding(workload)
