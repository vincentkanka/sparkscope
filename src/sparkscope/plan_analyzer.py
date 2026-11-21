
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from typing import Any
import re

from sparkscope.models import Finding


KNOWN_OPERATORS = (
    "SortMergeJoin",
    "BroadcastHashJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
    "BroadcastExchange",
    "Exchange",
)

OPERATOR_PATTERN = re.compile(
    r"^("
    + "|".join(KNOWN_OPERATORS)
    + r")(?=\s|$|\[|\()"
)


@dataclass(frozen=True)
class PlanSummary:
    operator_counts: dict[str, int]
    is_adaptive: bool
    plan_source: str = "DataFrame.explain(simple)"

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_counts": dict(self.operator_counts),
            "is_adaptive": self.is_adaptive,
            "plan_source": self.plan_source,
        }


def collect_physical_plan(dataframe: Any) -> str:
    """
    Capture DataFrame.explain(mode="simple").

    This does not execute DataFrame actions or collect rows.
    Planning may still access catalog or source metadata.

    Classic PySpark DataFrames are the initial target.
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
    Extract an operator name from a Spark plan line.

    Ignore tree prefixes and whole-stage codegen markers.
    Match only at the start of a plan node.
    """
    cleaned = re.sub(r"^[\s|:+\-]*", "", line)

    cleaned = re.sub(
        r"^\*\(\d+\)\s*",
        "",
        cleaned,
    )

    cleaned = re.sub(
        r"^\(\d+\)\s*",
        "",
        cleaned,
    )

    match = OPERATOR_PATTERN.match(cleaned)

    return match.group(1) if match else None


def summarize_plan(plan: str) -> PlanSummary:
    counts: Counter[str] = Counter()

    for line in plan.splitlines():
        operator = extract_operator(line)

        if operator is not None:
            counts[operator] += 1

    return PlanSummary(
        operator_counts=dict(counts),
        is_adaptive=any(
            line.lstrip().startswith("AdaptiveSparkPlan")
            for line in plan.splitlines()
        ),
    )


def analyze_physical_plan(
    summary: PlanSummary,
) -> list[Finding]:
    """
    Generate cautious findings from observed plan operators.

    These are diagnostic hints, not measured performance
    problems or guarantees of optimization benefits.
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
                    "Spark plans a CartesianProduct operation. "
                    "This may produce many output rows depending "
                    "on input cardinalities."
                ),
                recommendation=(
                    "Confirm the cross join is intentional. "
                    "Review expected output cardinality and "
                    "consider an appropriate join condition "
                    "if the operation is unintended."
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
                    "Spark plans a BroadcastNestedLoopJoin. "
                    "This can be costly for some input sizes "
                    "and non-equality join conditions."
                ),
                recommendation=(
                    "Review the join condition, estimated "
                    "cardinalities, and physical plan. "
                    "Compare alternative strategies when "
                    "appropriate."
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
                    "The physical plan contains a SortMergeJoin. "
                    "This is a common Spark join strategy and "
                    "is not inherently inefficient."
                ),
                recommendation=(
                    "Inspect exchange and sort operators, "
                    "table statistics, and runtime metrics "
                    "before considering another join strategy."
                ),
            )
        )

    return findings
