
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from sparkscope import analyze, compare


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkScopePlanComparison")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            "-1",
        )
        .getOrCreate()
    )

    try:
        transactions = spark.range(10000).selectExpr(
            "id AS transaction_id",
            "id % 100 AS customer_id",
        )

        customers = spark.range(100).selectExpr(
            "id AS customer_id",
        )

        original_df = transactions.join(
            customers,
            on="customer_id",
            how="inner",
        )

        optimized_df = transactions.join(
            broadcast(customers),
            on="customer_id",
            how="inner",
        )

        before = analyze(
            spark,
            dataframe=original_df,
        )

        after = analyze(
            spark,
            dataframe=optimized_df,
        )

        print("\nBEFORE")
        before.show()

        print("\nAFTER")
        after.show()

        comparison = compare(before, after)

        comparison.show()

        with open(
            "sparkscope-comparison.json",
            "w",
            encoding="utf-8",
        ) as output:
            json.dump(
                comparison.to_dict(),
                output,
                indent=2,
            )

        print("\nSaved: sparkscope-comparison.json")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
