from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    max,
    min,
)


INPUT_PATH = "data/logs/server_logs.csv"


def main():

    spark = (
        SparkSession.builder
        .appName("LogAnomalyAnalysis")
        .master("local[*]")
        .getOrCreate()
    )

    print("\n=== Spark Log Analysis ===\n")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )

    print("Schema:")
    df.printSchema()

    print("\nSample logs:")
    df.show(10, truncate=False)

    print("\n=== Status Code Distribution ===")

    (
        df.groupBy("status_code")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .show()
    )

    print("\n=== Endpoint Statistics ===")

    (
        df.groupBy("endpoint")
        .agg(
            count("*").alias("requests"),
            avg("response_time").alias("avg_response_time"),
            max("response_time").alias("max_response_time"),
            min("response_time").alias("min_response_time"),
        )
        .orderBy(desc("requests"))
        .show(truncate=False)
    )

    print("\n=== Slow Requests ===")

    (
        df.filter(col("response_time") > 1000)
        .orderBy(desc("response_time"))
        .show(20, truncate=False)
    )

    print("\n=== Server Errors ===")

    (
        df.filter(col("status_code") >= 500)
        .orderBy(desc("status_code"))
        .show(20, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()