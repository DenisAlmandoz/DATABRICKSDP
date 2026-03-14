from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


# -------------------------
# BRONZE: raw CDC ingestion
# -------------------------
@dp.table(name="bronze_users_cdc")
def bronze_users_cdc():
    return spark.read.table("learning.sdp_demo.users_cdc_events")


# --------------------------------------------------------
# SILVER: latest active user state + quality expectation
# - keep latest event per user_id by event_ts
# - remove deleted records
# - drop rows with null email
# --------------------------------------------------------
@dp.table(name="silver_users")
@dp.expect_or_drop("email_not_null", "email IS NOT NULL")
def silver_users():
    src = spark.read.table("bronze_users_cdc")

    ranked = src.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("event_ts").desc())
        ),
    )

    return ranked.filter((F.col("rn") == 1) & (F.col("op") != F.lit("D"))).drop("rn")


# ----------------------------------------------
# GOLD: business-friendly aggregate for reporting
# ----------------------------------------------
@dp.table(name="gold_user_booking_summary")
def gold_user_booking_summary():
    users = spark.read.table("silver_users")
    bookings = spark.read.table("learning.sdp_demo.bookings_src")

    return (
        users.join(bookings, "user_id", "left")
        .groupBy("user_id", "name")
        .agg(
            F.count("booking_id").alias("booking_count"),
            F.coalesce(F.sum("amount"), F.lit(0)).alias("total_amount"),
        )
        .orderBy(F.col("booking_count").desc())
    )
