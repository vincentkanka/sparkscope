
from sparkscope.models import Finding


def check_aqe_enabled(config: dict[str, str]) -> list[Finding]:
    raw_value = config.get("spark.sql.adaptive.enabled")

    # Missing configuration does not prove AQE is disabled.
    if raw_value is None:
        return []

    value = str(raw_value).strip().lower()

    if value == "true":
        return []

    if value not in {"true", "false"}:
        return [
            Finding(
                rule_id="AQE_INVALID",
                severity="HIGH",
                title="Invalid AQE configuration",
                description=(
                    f"spark.sql.adaptive.enabled has an invalid value: "
                    f"{raw_value!r}."
                ),
                recommendation=(
                    "Set spark.sql.adaptive.enabled to true or false."
                ),
            )
        ]

    return [
        Finding(
            rule_id="AQE_DISABLED",
            severity="HIGH",
            title="Adaptive Query Execution is disabled",
            description=(
                "AQE is explicitly disabled. This can prevent "
                "runtime optimization of certain Spark SQL operations."
            ),
            recommendation=(
                "Evaluate enabling spark.sql.adaptive.enabled and "
                "benchmark representative workloads."
            ),
        )
    ]
