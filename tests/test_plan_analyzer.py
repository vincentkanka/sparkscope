
import pytest

from sparkscope.plan_analyzer import (
    analyze_physical_plan,
    collect_physical_plan,
    extract_operator,
    summarize_plan,
)


SAMPLE_PLAN = """
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [customer_id#1], [customer_id#2], Inner
   :- Sort [customer_id#1 ASC NULLS FIRST]
   :  +- Exchange hashpartitioning(customer_id#1, 200)
   +- Sort [customer_id#2 ASC NULLS FIRST]
      +- Exchange hashpartitioning(customer_id#2, 200)
"""


class FakeDataFrame:
    def __init__(self, plan):
        self.plan = plan

    def explain(self, mode="simple"):
        assert mode == "simple"
        print(self.plan)


def test_collect_physical_plan():
    dataframe = FakeDataFrame(SAMPLE_PLAN)

    plan = collect_physical_plan(dataframe)

    assert "SortMergeJoin" in plan
    assert "Exchange" in plan


def test_extract_operator_from_spark_tree():
    assert extract_operator(
        "+- *(2) SortMergeJoin [id#1], [id#2], Inner"
    ) == "SortMergeJoin"

    assert extract_operator(
        "   :  +- Exchange hashpartitioning(id#1, 200)"
    ) == "Exchange"

    assert extract_operator(
        "+- BroadcastHashJoin [id#1], [id#2], Inner"
    ) == "BroadcastHashJoin"


def test_plan_summary():
    summary = summarize_plan(SAMPLE_PLAN)

    assert summary.is_adaptive
    assert summary.operator_counts["SortMergeJoin"] == 1
    assert summary.operator_counts["Exchange"] == 2


def test_sort_merge_join_finding():
    summary = summarize_plan(SAMPLE_PLAN)
    findings = analyze_physical_plan(summary)

    rule_ids = {finding.rule_id for finding in findings}

    assert "PLAN_SORT_MERGE_JOIN" in rule_ids


def test_cartesian_product_finding():
    summary = summarize_plan(
        "== Physical Plan ==\n"
        "+- CartesianProduct\n"
    )

    findings = analyze_physical_plan(summary)

    assert any(
        finding.rule_id == "PLAN_CARTESIAN_PRODUCT"
        for finding in findings
    )


def test_broadcast_nested_loop_finding():
    summary = summarize_plan(
        "== Physical Plan ==\n"
        "+- BroadcastNestedLoopJoin BuildRight, Inner\n"
    )

    findings = analyze_physical_plan(summary)

    assert any(
        finding.rule_id == "PLAN_BROADCAST_NESTED_LOOP"
        for finding in findings
    )


def test_broadcast_hash_join_is_not_flagged():
    summary = summarize_plan(
        "== Physical Plan ==\n"
        "+- BroadcastHashJoin [id#1], [id#2], Inner\n"
    )

    assert summary.operator_counts["BroadcastHashJoin"] == 1
    assert analyze_physical_plan(summary) == []


def test_operator_names_inside_expressions_are_ignored():
    summary = summarize_plan(
        "== Physical Plan ==\n"
        "+- Project [SortMergeJoin AS message#1]\n"
    )

    assert summary.operator_counts == {}


@pytest.mark.parametrize(
    "dataframe",
    [None, object()],
)
def test_invalid_dataframe(dataframe):
    with pytest.raises((ValueError, TypeError)):
        collect_physical_plan(dataframe)
