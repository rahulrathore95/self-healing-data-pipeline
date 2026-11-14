"""PySpark + Delta Lake variant of the self-healing loop, for Databricks.

This is the reference implementation for a real cluster: quality gates as
DataFrame checks, quarantine as a Delta table, idempotent load as MERGE INTO.
It is intentionally not part of the local test suite (no Spark dependency);
run it as a Databricks job with the `healer` package on the cluster.

    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark/databricks_job.py
"""
from __future__ import annotations

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:  # local dev / CI without Spark
    DataFrame = None
    SparkSession = None
    F = None

REQUIRED_COLUMNS = ["order_id", "customer_id", "order_ts", "amount", "currency"]
MAX_NULL_RATE = 0.01
EXPECTED_ROWS = 500


def quality_issues(df: "DataFrame") -> list[str]:
    """The same gates as healer.quality, expressed in Spark."""
    issues = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        issues.append(f"MISSING_COLUMN: {missing}")
        return issues  # further checks are meaningless without the schema
    total = df.count()
    if total == 0:
        return ["VOLUME_DROP: empty batch"]
    for col in REQUIRED_COLUMNS:
        null_rate = df.filter(F.col(col).isNull()).count() / total
        if null_rate > MAX_NULL_RATE:
            issues.append(f"NULL_SPIKE: {col} at {null_rate:.1%}")
    dup_rate = (total - df.dropDuplicates(["order_id"]).count()) / total
    if dup_rate > 0:
        issues.append(f"DUPLICATE_KEYS: order_id at {dup_rate:.1%}")
    if total < 0.5 * EXPECTED_ROWS:
        issues.append(f"VOLUME_DROP: {total} rows vs expected {EXPECTED_ROWS}")
    if df.filter(F.col("amount").cast("double").isNull() & F.col("amount").isNotNull()).count():
        issues.append("TYPE_MISMATCH: amount not numeric")
    return issues


def load_delta(spark: "SparkSession", df: "DataFrame", target: str) -> None:
    """Idempotent MERGE INTO - the Delta Lake equivalent of the SQLite upsert."""
    df.createOrReplaceTempView("stg_orders")
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING stg_orders AS s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    spark = SparkSession.builder.appName("self-healing-orders").getOrCreate()
    source_path = "dbfs:/landing/orders/"
    target = "main.silver.orders"
    quarantine = "main.silver.orders_quarantine"

    batch = spark.read.format("delta").load(source_path)
    issues = quality_issues(batch)

    if any(i.startswith("NULL_SPIKE") for i in issues):
        # same playbook as the local runner: quarantine, load the valid rest
        required = [c for c in REQUIRED_COLUMNS if c in batch.columns]
        bad = batch.filter(" OR ".join(f"{c} IS NULL" for c in required))
        bad.write.format("delta").mode("append").saveAsTable(quarantine)
        batch = batch.dropna(subset=required)
        issues = quality_issues(batch)

    if any(i.startswith("DUPLICATE_KEYS") for i in issues):
        batch = batch.dropDuplicates(["order_id"])
        issues = quality_issues(batch)

    if issues:
        raise RuntimeError(f"unhealed quality issues, escalating: {issues}")
    load_delta(spark, batch, target)


if __name__ == "__main__":
    main()
