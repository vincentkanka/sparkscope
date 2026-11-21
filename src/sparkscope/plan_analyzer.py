
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from typing import Any
import re

from sparkscope.models import Finding


OPERATORS = (
    "BroadcastNestedLoopJoin",
    "BroadcastHashJoin",
    "ShuffledHashJoin",
    "SortMergeJoin",
    "CartesianProduct",
    "BroadcastExchange",
    "ReusedExchange",
    "Exchange",
    "AQEShuffleRead",
    "BatchEvalPython",
    "ArrowEvalPython",
)

_OPERATOR_RE = re.compile(
    r"^("
    + "|".join(re.escape(name) for name in OPERATORS)
    + r")(?=\s|$|\[|\()"
)

_AQE_RE = re.compile(r"\bAdaptiveSparkPlan\b")

_FINAL_RE = re.compile(
    r"\bisFinalPlan=(true|false)\b",
    re.IGNORECASE,
)

_TREE_PREFIX_RE = re.compile(
    r"^(?:(?:\s|\||:|\+|-)*)"
    r"(?:\*?\(\d+\)\s*)?"
)


@dataclass(frozen=True)
class PlanSummary:
    operator_counts: dict[str, int]
    is_adaptive: bool
    plan_source: str = "DataFrame.explain(simple)"
    is_final_plan: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_counts": dict(self.operator_counts),
            "is_adaptive": self.is_adaptive,
            "plan_source": self.plan_source,
            "is_final_plan": self.is_final_plan,
        }


def collect_physical_plan(dataframe: Any) -> str:
    """
    Capture the DataFrame's simple physical plan.

    Does not trigger DataFrame actions.
    Planning may access catalog or source metadata.
    """
    if dataframe is None:
        raise ValueError("A DataFrame is required.")

    if not callable(getattr(dataframe, "explain", None)):
        raise TypeError(
            "Expected a DataFrame with an explain() method."
        )

    output = StringIO()

    try:
        with redirect_stdout(output):
            dataframe.explain(mode="simple")
    except Exception as exc:
        raise RuntimeError(
            "Unable to inspect the DataFrame physical plan."
        ) from exc

    plan = output.getvalue()

    if not plan.strip():
        raise ValueError(
            "DataFrame.explain() returned an empty plan."
        )

    return plan


def extract_operator(line: str) -> str | None:
    """
    Extract recognized operators from simple plan nodes.

    Handles tree markers and whole-stage codegen prefixes
    such as *(2), without matching names in expressions.
    """
    if not line.strip():
        return None

    if line.lstrip().startswith("=="):
        return None

    cleaned = _TREE_PREFIX_RE.sub("", line, count=1)

    match = _OPERATOR_RE.match(cleaned)

    return match.group(1) if match else None


def summarize_plan(plan: str) -> PlanSummary:
    counts: Counter[str] = Counter()

    for line in plan.splitlines():
        operator = extract_operator(line)

        if operator:
            counts[operator] += 1

    is_adaptive = bool(_AQE_RE.search(plan))

    final_match = (
        _FINAL_RE.search(plan)
        if is_adaptive
        else None
    )

    is_final = (
        final_match.group(1).lower() == "true"
        if final_match
        else None
    )

    return PlanSummary(
        operator_counts=dict(counts),
        is_adaptive=is_adaptive,
        is_final_plan=is_final,
    )


def analyze_physical_plan(
    summary: PlanSummary,
) -> list[Finding]:
    """
    Generate conservative diagnostic findings.

    Findings identify planned operators, not measured
    performance problems or guaranteed optimizations.
    """
    counts = summary.operator_counts
    findings: list[Finding] = []

    if counts.get("CartesianProduct", 0):
        findings.append(
            Finding(
                rule_id="PLAN_CARTESIAN_PRODUCT",
                severity="MEDIUM",
                title="Cartesian product detected in physical plan",
                description=(
                    "A CartesianProduct operator is present; "
                    "output size depends on input cardinalities."
                ),
                recommendation=(
                    "Verify that the cross join is intentional "
                    "and check expected output cardinality."
                ),
            )
        )

    if counts.get("BroadcastNestedLoopJoin", 0):
        findings.append(
            Finding(
                rule_id="PLAN_BROADCAST_NESTED_LOOP",
                severity="MEDIUM",
                title="Broadcast nested-loop join detected",
                description=(
                    "A BroadcastNestedLoopJoin is planned, "
                    "potentially expensive for some input sizes "
                    "and non-equality joins."
                ),
                recommendation=(
                    "Review join conditions, input cardinalities, "
                    "and runtime metrics before changing strategy."
                ),
            )
        )

    if counts.get("SortMergeJoin", 0):
        findings.append(
            Finding(
                rule_id="PLAN_SORT_MERGE_JOIN",
                severity="LOW",
                title="Sort-merge join observed",
                description=(
                    "SortMergeJoin is a normal Spark join "
                    "strategy, not inherently inefficient."
                ),
                recommendation=(
                    "Examine actual shuffle and sort costs "
                    "before considering another strategy."
                ),
            )
        )

    if (
        counts.get("BatchEvalPython", 0)
        or counts.get("ArrowEvalPython", 0)
    ):
        findings.append(
            Finding(
                rule_id="PLAN_PYTHON_EVALUATION",
                severity="LOW",
                title="Python evaluation operator observed",
                description=(
                    "The plan includes a Python evaluation "
                    "operator; performance impact is "
                    "workload-dependent."
                ),
                recommendation=(
                    "Compare built-in Spark SQL functions "
                    "with the current Python logic where "
                    "practical, then benchmark."
                ),
            )
        )

    return findings
