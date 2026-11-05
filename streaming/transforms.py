"""
The data logic for AdPulse, kept separate from the streaming job so it can
be unit tested on small, ordinary DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("campaign_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("user_id", StringType()),
    StructField("device", StringType()),
    StructField("cost_usd", DoubleType()),
])

VALID_TYPES = ["impression", "click"]


def parse_events(raw: DataFrame) -> DataFrame:
    """Turn raw JSON text (column `value`) into typed columns."""
    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    return (parsed.select("e.*")
            .withColumn("event_time", F.to_timestamp("event_time")))


def is_valid():
    """Data quality rule: a usable event has an id, a known type,
    a campaign, and a timestamp."""
    return (F.col("event_id").isNotNull()
            & F.col("event_type").isin(VALID_TYPES)
            & F.col("campaign_id").isNotNull()
            & F.col("event_time").isNotNull())


def clean(events: DataFrame) -> DataFrame:
    """Keep only valid events and fill a missing cost with 0."""
    return events.filter(is_valid()).fillna({"cost_usd": 0.0})


def campaign_metrics(events: DataFrame, window: str = "1 minute") -> DataFrame:
    """Impressions, clicks, click through rate and spend per campaign,
    per time window."""
    agg = (events
           .groupBy(F.window("event_time", window).alias("w"), "campaign_id")
           .agg(F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
                F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
                F.round(F.sum("cost_usd"), 2).alias("spend_usd")))
    return (agg
            .withColumn("ctr", F.when(F.col("impressions") > 0,
                                      F.round(F.col("clicks") / F.col("impressions"), 4))
                        .otherwise(F.lit(0.0)))
            .select(F.col("w.start").alias("window_start"),
                    F.col("w.end").alias("window_end"),
                    "campaign_id", "impressions", "clicks", "ctr", "spend_usd"))
