
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from sparkscope.report import SparkReport


@dataclass(frozen=True)
class OperatorChange:
    operator: str
    before: int
    after: int

    @property
    def delta(self) -> int:
        return self.after - self.before


@dataclass(frozen=True)
class FindingChange:
    rule_id: str
    severity: str
    count: int


@dataclass(frozen=True)
class PlanComparison:
    before_spark_version: str
    after_spark_version: str
    operator_changes: list[OperatorChange]
    added_findings: list[FindingChange]
    resolved_findings: list[FindingChange]
    warnings: list[str]

    @property
    def has_changes(self) -> bool:
        return bool(
            self.operator_changes
            or self.added_findings
            or self.resolved_findings
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "before_spark_version": self.before_spark_version,
            "after_spark_version": self.after_spark_version,
            "operator_changes": [
                {**asdict(change), "delta": change.delta}
                for change in self.operator_changes
            ],
            "added_findings": [
                asdict(f) for f in self.added_findings
            ],
            "resolved_findings": [
                asdict(f) for f in self.resolved_findings
            ],
            "warnings": list(self.warnings),
            "has_changes": self.has_changes,
        }

    def show(self) -> None:
        print("\nSparkScope Execution-Plan Comparison")
        print("=" * 36)
        print(
            f"Spark versions: {self.before_spark_version} -> "
            f"{self.after_spark_version}"
        )

        if not self.operator_changes:
            print("\nNo recognized operator-count changes.")
        else:
            print("\nOperator changes (before -> after):")
            for change in self.operator_changes:
                print(
                    f"  {change.operator}: "
                    f"{change.before} -> {change.after} "
                    f"(delta {change.delta:+d})"
                )

        for label, changes in (
            ("New findings", self.added_findings),
            ("Resolved findings", self.resolved_findings),
        ):
            if changes:
                print(f"\n{label}:")
                for change in changes:
                    print(
                        f"  {change.rule_id} [{change.severity}] "
                        f"x{change.count}"
                    )

        if self.warnings:
            print("\nComparison cautions:")
            for warning in self.warnings:
                print(f"  - {warning}")

        print(
            "\nPlan differences do not establish "
            "runtime speedups."
        )


def compare(
    before: SparkReport,
    after: SparkReport,
) -> PlanComparison:
    """
    Compare two already collected SparkScope reports.

    Does not run Spark jobs or inspect DataFrames.
    """
    if not isinstance(before, SparkReport) or not isinstance(
        after, SparkReport
    ):
        raise TypeError(
            "compare() requires two SparkReport objects."
        )

    if (
        before.plan_summary is None
        or after.plan_summary is None
    ):
        raise ValueError(
            "Both reports must contain plan summaries. "
            "Call analyze(spark, dataframe=...) "
            "for each DataFrame."
        )

    old_counts = before.plan_summary.operator_counts
    new_counts = after.plan_summary.operator_counts

    changes = [
        OperatorChange(
            name,
            old_counts.get(name, 0),
            new_counts.get(name, 0),
        )
        for name in sorted(
            old_counts.keys() | new_counts.keys()
        )
        if old_counts.get(name, 0) != new_counts.get(name, 0)
    ]

    def finding_counts(
        report: SparkReport,
    ) -> Counter[tuple[str, str]]:
        return Counter(
            (f.rule_id, f.severity)
            for f in report.findings
        )

    old_findings = finding_counts(before)
    new_findings = finding_counts(after)

    def finding_deltas(
        left: Counter[tuple[str, str]],
        right: Counter[tuple[str, str]],
    ) -> list[FindingChange]:
        return [
            FindingChange(rule_id, severity, count)
            for (rule_id, severity), count
            in sorted((left - right).items())
        ]

    warnings: list[str] = []

    if before.spark_version != after.spark_version:
        warnings.append(
            "Spark versions differ; operator counts "
            "may not be directly comparable."
        )

    if before.config != after.config:
        warnings.append(
            "Collected Spark configuration differs "
            "between reports."
        )

    for label, report in (
        ("Before", before),
        ("After", after),
    ):
        if (
            report.plan_summary.is_adaptive
            and report.plan_summary.is_final_plan is not True
        ):
            warnings.append(
                f"{label} report contains an adaptive plan "
                "not confirmed final; Spark may change its "
                "execution strategy at runtime."
            )

    return PlanComparison(
        before_spark_version=before.spark_version,
        after_spark_version=after.spark_version,
        operator_changes=changes,
        added_findings=finding_deltas(
            new_findings, old_findings
        ),
        resolved_findings=finding_deltas(
            old_findings, new_findings
        ),
        warnings=warnings,
    )
