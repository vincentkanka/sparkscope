
import pytest

from sparkscope.plan_analyzer import (
    analyze_physical_plan,
    extract_operator,
    summarize_plan,
)


@pytest.mark.parametrize(
    "line,expected",
    [
        (
            "+- *(2) SortMergeJoin [a#1], [b#2], Inner",
            "SortMergeJoin",
        ),
        (
            "   :  +- Exchange hashpartitioning(a#1, 200)",
            "Exchange",
        ),
        (
            "+- BroadcastHashJoin [a#1], [b#2], Inner",
            "BroadcastHashJoin",
        ),
        (
            "+- *(1) BatchEvalPython [f#1], [x#2]",
            "BatchEvalPython",
        ),
        (
            "+- ArrowEvalPython [f#1], [x#2]",
            "ArrowEvalPython",
        ),
        (
            "   +- ReusedExchange [a#2]",
            "ReusedExchange",
        ),
        (
            "+- AQEShuffleRead coalesced",
            "AQEShuffleRead",
        ),
        (
            "+- Project [SortMergeJoin AS message#1]",
            None,
        ),
        (
            "== Physical Plan ==",
            None,
        ),
    ],
)
def test_plan_operator_parsing(line, expected):
    assert extract_operator(line) == expected


def test_adaptive_plan_not_final():
    plan = """
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [a#1], [b#2], Inner
   :- Exchange hashpartitioning(a#1, 200)
   +- ReusedExchange [b#2]
"""

    summary = summarize_plan(plan)

    assert summary.is_adaptive is True
    assert summary.is_final_plan is False
    assert summary.operator_counts == {
        "SortMergeJoin": 1,
        "Exchange": 1,
        "ReusedExchange": 1,
    }


def test_final_adaptive_plan():
    plan = """
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- BroadcastHashJoin [a#1], [b#2], Inner
"""

    summary = summarize_plan(plan)

    assert summary.is_adaptive
    assert summary.is_final_plan is True


def test_non_adaptive_plan():
    plan = """
== Physical Plan ==
+- BroadcastHashJoin [a#1], [b#2], Inner
"""

    summary = summarize_plan(plan)

    assert summary.is_adaptive is False
    assert summary.is_final_plan is None


def test_python_evaluation_finding():
    plan = """
== Physical Plan ==
+- BatchEvalPython [my_udf(value#1)], [pythonUDF0#2]
"""

    summary = summarize_plan(plan)
    findings = analyze_physical_plan(summary)

    assert any(
        finding.rule_id == "PLAN_PYTHON_EVALUATION"
        for finding in findings
    )


def test_exchange_alone_is_not_a_warning():
    plan = """
== Physical Plan ==
+- Exchange hashpartitioning(id#1, 200)
"""

    summary = summarize_plan(plan)

    assert summary.operator_counts["Exchange"] == 1
    assert analyze_physical_plan(summary) == []


def test_summary_serialization():
    summary = summarize_plan(
        "AdaptiveSparkPlan isFinalPlan=false\n"
        "+- BroadcastHashJoin [a#1], [b#2], Inner"
    )

    payload = summary.to_dict()

    assert payload["is_adaptive"] is True
    assert payload["is_final_plan"] is False
    assert payload["operator_counts"]["BroadcastHashJoin"] == 1
