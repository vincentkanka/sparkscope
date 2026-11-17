
from pyspark.sql import SparkSession

from sparkscope.analyzer import analyze_config
from sparkscope.spark_collector import collect_spark_config


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SparkScopeDemo")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2000")
    .getOrCreate()
)

try:
    snapshot = collect_spark_config(spark)

    print(f"Spark version: {snapshot.spark_version}")
    print("\nCollected settings:")

    for key, value in sorted(snapshot.config.items()):
        print(f"{key} = {value}")

    findings = analyze_config(snapshot.config)

    print("\nAnalysis findings:")

    for finding in findings:
        print(f"[{finding.severity}] {finding.title}")
        print(f"  {finding.recommendation}")

finally:
    spark.stop()
