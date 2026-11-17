
from dataclasses import asdict, dataclass, field
from typing import Any

from sparkscope.models import Finding


SEVERITY_ORDER = {
    "HIGH": 0,
    "MEDIUM": 1,
    "LOW": 2,
}


@dataclass
class SparkReport:
    spark_version: str
    config: dict[str, str]
    findings: list[Finding] = field(default_factory=list)
    sources: dict[str, str] = field(default_factory=dict)

    @property
    def total_findings(self) -> int:
        return len(self.findings)

    @property
    def severity_counts(self) -> dict[str, int]:
        counts = {
            "HIGH": 0,
            "MEDIUM": 0,
            "LOW": 0,
        }

        for finding in self.findings:
            counts[finding.severity] += 1

        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "spark_version": self.spark_version,
            "config": dict(self.config),
            "sources": dict(self.sources),
            "total_findings": self.total_findings,
            "severity_counts": self.severity_counts,
            "findings": [
                asdict(finding)
                for finding in self.findings
            ],
        }

    def show(self) -> None:
        print("\nSparkScope Performance Report")
        print("=" * 29)
        print(f"Spark Version: {self.spark_version}")
        print(f"Settings Collected: {len(self.config)}")
        print(f"Total Findings: {self.total_findings}")

        counts = self.severity_counts
        print(
            f"Severity: HIGH={counts['HIGH']} | "
            f"MEDIUM={counts['MEDIUM']} | "
            f"LOW={counts['LOW']}"
        )

        if not self.findings:
            print("\nNo issues detected by the enabled rules.")
            print(
                "Note: This does not guarantee that "
                "the workload is fully optimized."
            )
            return

        ordered = sorted(
            self.findings,
            key=lambda f: SEVERITY_ORDER[f.severity],
        )

        for index, finding in enumerate(ordered, start=1):
            print(f"\n{index}. [{finding.severity}] {finding.title}")
            print(f"   Rule: {finding.rule_id}")
            print(f"   Description: {finding.description}")
            print(f"   Recommendation: {finding.recommendation}")
