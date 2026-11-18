
import json

from pyspark.sql import SparkSession

from sparkscope import analyze


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkScopeLiveAnalysis")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2000")
        .getOrCreate()
    )

    try:
        report = analyze(spark)

        # Human-readable report
        report.show()

        # Programmatic access
        print("\nFindings by severity:")
        print(report.severity_counts)

        # JSON-compatible report
        with open(
            "sparkscope-report.json",
            "w",
            encoding="utf-8",
        ) as output:
            json.dump(report.to_dict(), output, indent=2)

        print("\nSaved: sparkscope-report.json")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
