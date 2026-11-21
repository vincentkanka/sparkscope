
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from sparkscope import analyze


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkScopePlanAnalysis")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    try:
        transactions = spark.createDataFrame(
            [
                (1, 101, 100.0),
                (2, 102, 200.0),
                (3, 101, 150.0),
            ],
            ["transaction_id", "customer_id", "amount"],
        )

        customers = spark.createDataFrame(
            [
                (101, "Alice"),
                (102, "Bob"),
            ],
            ["customer_id", "customer_name"],
        )

        joined = transactions.join(
            broadcast(customers),
            on="customer_id",
            how="inner",
        )

        # Analyze the plan without collecting data.
        report = analyze(
            spark,
            dataframe=joined,
        )

        report.show()

        print("\nStructured plan summary:")

        if report.plan_summary is not None:
            print(report.plan_summary.to_dict())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
