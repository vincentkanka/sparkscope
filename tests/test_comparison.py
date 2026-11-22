
import json

import pytest

from sparkscope.comparison import compare
from sparkscope.models import Finding
from sparkscope.plan_analyzer import PlanSummary
from sparkscope.report import SparkReport


def make_report(
    operators,
    rule_ids=(),
    adaptive=False,
    final=None,
    version="3.5.0",
    config=None,
):
    return SparkReport(
        spark_version=version,
        config={} if config is None else config,
        findings=[
            Finding(
                rule_id=rule_id,
                severity="LOW",
                title=rule_id,
                description="Test finding",
                recommendation="Test recommendation",
            )
            for rule_id in rule_ids
        ],
        plan_summary=PlanSummary(
            operator_counts=operators,
            is_adaptive=adaptive,
            is_final_plan=final,
        ),
    )


def test_operator_deltas():
    before = make_report({
        "SortMergeJoin": 1,
        "Exchange": 2,
    })

    after = make_report({
        "BroadcastHashJoin": 1,
        "BroadcastExchange": 1,
    })

    result = compare(before, after)

    changes = {
        change.operator: change.delta
        for change in result.operator_changes
    }

    assert changes == {
        "SortMergeJoin": -1,
        "Exchange": -2,
        "BroadcastHashJoin": 1,
        "BroadcastExchange": 1,
    }

    assert result.has_changes


def test_no_changes():
    before = make_report({"Exchange": 1})
    after = make_report({"Exchange": 1})

    result = compare(before, after)

    assert result.has_changes is False
    assert result.operator_changes == []


def test_finding_multisets():
    before = make_report(
        {},
        ["A", "A", "B"],
    )

    after = make_report(
        {},
        ["A", "C", "C"],
    )

    result = compare(before, after)

    assert [
        (f.rule_id, f.count)
        for f in result.resolved_findings
    ] == [
        ("A", 1),
        ("B", 1),
    ]

    assert [
        (f.rule_id, f.count)
        for f in result.added_findings
    ] == [
        ("C", 2),
    ]


def test_invalid_input():
    with pytest.raises(TypeError):
        compare(None, None)

    with pytest.raises(ValueError):
        compare(
            SparkReport(
                spark_version="3.5.0",
                config={},
            ),
            make_report({}),
        )


def test_comparison_warnings():
    before = make_report(
        {},
        adaptive=True,
        final=False,
    )

    after = make_report(
        {},
        adaptive=True,
        final=None,
        version="4.0.0",
        config={"example": "changed"},
    )

    result = compare(before, after)

    assert len(result.warnings) == 4


def test_output_and_serialization(capsys):
    before = make_report(
        {"SortMergeJoin": 1},
        ["OLD_RULE"],
    )

    after = make_report(
        {"BroadcastHashJoin": 1},
        ["NEW_RULE"],
    )

    result = compare(before, after)

    result.show()

    output = capsys.readouterr().out

    assert "SortMergeJoin: 1 -> 0" in output
    assert "Resolved findings:" in output
    assert "New findings:" in output

    payload = json.loads(json.dumps(result.to_dict()))

    assert payload["has_changes"] is True
    assert len(payload["operator_changes"]) == 2
