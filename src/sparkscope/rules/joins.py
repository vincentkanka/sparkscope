
from sparkscope.models import Finding, WorkloadMetadata


BROADCAST_SIZE_THRESHOLD_MB = 100
LARGE_TABLE_SIZE_THRESHOLD_MB = 1000

JOIN_TYPE_ALIASES = {
    "inner": "inner",
    "left": "left_outer",
    "left_outer": "left_outer",
    "leftouter": "left_outer",
    "right": "right_outer",
    "right_outer": "right_outer",
    "rightouter": "right_outer",
    "full": "full_outer",
    "outer": "full_outer",
    "full_outer": "full_outer",
    "fullouter": "full_outer",
    "left_semi": "left_semi",
    "leftsemi": "left_semi",
    "semi": "left_semi",
    "left_anti": "left_anti",
    "leftanti": "left_anti",
    "anti": "left_anti",
}

ALLOWED_BROADCAST_SIDES = {
    "inner": {"left", "right"},
    "left_outer": {"right"},
    "right_outer": {"left"},
    "left_semi": {"right"},
    "left_anti": {"right"},
}


def check_broadcast_join_opportunities(
    workload: WorkloadMetadata,
) -> list[Finding]:
    findings = []
    tables = {table.name: table for table in workload.tables}

    for join in workload.joins:
        left = tables.get(join.left_table)
        right = tables.get(join.right_table)

        if left is None or right is None:
            continue

        join_type = JOIN_TYPE_ALIASES.get(
            join.join_type.strip().lower().replace(" ", "_")
        )

        if join_type not in ALLOWED_BROADCAST_SIDES:
            continue

        # Without equi-join keys, we cannot suggest
        # a broadcast hash join.
        if not join.join_keys:
            continue

        candidates = [
            ("left", left, right),
            ("right", right, left),
        ]

        for side, small, large in candidates:
            if side not in ALLOWED_BROADCAST_SIDES[join_type]:
                continue

            if not (
                0 <= small.size_mb <= BROADCAST_SIZE_THRESHOLD_MB
                and large.size_mb >= LARGE_TABLE_SIZE_THRESHOLD_MB
            ):
                continue

            findings.append(
                Finding(
                    rule_id="BROADCAST_JOIN_OPPORTUNITY",
                    severity="MEDIUM",
                    title="Broadcast join opportunity detected",
                    description=(
                        f"Table '{small.name}' ({small.size_mb:.1f} MB) "
                        f"is joined with '{large.name}' "
                        f"({large.size_mb:.1f} MB) using "
                        f"a {join_type} join. Broadcasting the "
                        f"{side} side may be worth evaluating."
                    ),
                    recommendation=(
                        f"Check whether Spark already broadcasts "
                        f"'{small.name}'. Review the physical plan, "
                        f"runtime memory requirements, join-key "
                        f"statistics, and the configured automatic "
                        f"broadcast threshold before applying a "
                        f"broadcast hint. The 100 MB screening limit "
                        f"is a heuristic, not a safe broadcast size."
                    ),
                )
            )

    return findings


def check_missing_join_keys(
    workload: WorkloadMetadata,
) -> list[Finding]:
    findings = []

    equi_join_types = {
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
        "left_semi",
        "left_anti",
    }

    for join in workload.joins:
        join_type = JOIN_TYPE_ALIASES.get(
            join.join_type.strip().lower().replace(" ", "_")
        )

        if join_type in equi_join_types and not join.join_keys:
            findings.append(
                Finding(
                    rule_id="JOIN_KEYS_MISSING",
                    severity="LOW",
                    title="Join key metadata is missing",
                    description=(
                        f"The join between '{join.left_table}' "
                        f"and '{join.right_table}' has no join "
                        f"keys in the supplied metadata."
                    ),
                    recommendation=(
                        "Provide join keys if this is an equi-join. "
                        "For non-equi joins, inspect the physical "
                        "execution plan separately."
                    ),
                )
            )

    return findings
