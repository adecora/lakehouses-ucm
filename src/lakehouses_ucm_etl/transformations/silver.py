from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="farmia_silver.events_curated", cluster_by=["event_ts", "customer_id"])
@dp.expect_or_drop("valid_events_record", "customer_id IS NOT NULL AND event_ts IS NOT NULL")
def events_curated():
    return spark.readStream.table("farmia_bronze.events_raw").select(
        F.col("event_id").cast("string").alias("event_id"),
        F.col("event_ts").cast("timestamp").alias("event_ts"),
        F.col("session_id").cast("string").alias("session_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("event_type").cast("string").alias("event_type"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("channel").cast("string").alias("channel"),
    )


@dp.table(name="farmia_silver.events_quarantine")
def events_quarantine():
    return (
        spark.readStream.table("farmia_bronze.events_raw")
        .select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("event_ts").cast("timestamp").alias("event_ts"),
            F.col("session_id").cast("string").alias("session_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("event_type").cast("string").alias("event_type"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("channel").cast("string").alias("channel"),
        )
        .filter("customer_id IS NULL OR event_ts IS NULL")
    )


rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_total": "order_total_eur IS NOT NULL AND order_total_eur >= 0",
    "valid_cdc_operation": "op_type IN ('I', 'U', 'D') AND op_ts IS NOT NULL AND sequence_num IS NOT NULL",
}
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))


@dp.temporary_view(
    name="orders_clean",
)
@dp.expect_all_or_drop(rules)
def orders_clean():
    return spark.readStream.table("farmia_bronze.orders_raw").select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("order_status").cast("string").alias("order_status"),
        F.col("sales_channel").cast("string").alias("sales_channel"),
        F.col("order_total_eur").cast("decimal(12,2)").alias("order_total_eur"),
        F.col("item_count").cast("int").alias("item_count"),
        F.col("op_type").cast("string").alias("op_type"),
        F.col("op_ts").cast("timestamp").alias("op_ts"),
        F.col("sequence_num").cast("long").alias("sequence_num"),
        F.col("is_deleted").cast("boolean").alias("is_deleted"),
    )


dp.create_streaming_table(
    name="farmia_silver.orders_curated",
)

dp.create_auto_cdc_flow(
    target="farmia_silver.orders_curated",
    source="orders_clean",
    keys=["order_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("op_type = 'D'"),
    except_column_list=[
        "op_type",
        "sequence_num",
        "is_deleted",
    ],
    stored_as_scd_type="2",
)


@dp.table(
    name="farmia_silver.orders_quarantine",
)
def orders_quarantine():
    return (
        spark.readStream.table("farmia_bronze.orders_raw")
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("order_status").cast("string").alias("order_status"),
            F.col("sales_channel").cast("string").alias("sales_channel"),
            F.col("order_total_eur").cast("decimal(12,2)").alias("order_total_eur"),
            F.col("item_count").cast("int").alias("item_count"),
            F.col("op_type").cast("string").alias("op_type"),
            F.col("op_ts").cast("timestamp").alias("op_ts"),
            F.col("sequence_num").cast("long").alias("sequence_num"),
            F.col("is_deleted").cast("boolean").alias("is_deleted"),
        )
        .filter(quarantine_rules)
    )
