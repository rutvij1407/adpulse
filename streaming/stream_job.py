"""
AdPulse streaming job (PySpark Structured Streaming).

Reads ad events, removes broken records and duplicates, and writes
per minute campaign metrics (impressions, clicks, CTR, spend) to Parquet.

With Kafka:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      streaming/stream_job.py --source kafka

Without Kafka (reads JSON files that the producer writes):
  python streaming/stream_job.py --source files --input data/incoming
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from transforms import campaign_metrics, clean, is_valid, parse_events  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="ad-events")
    p.add_argument("--input", default="data/incoming")
    p.add_argument("--output", default="data/metrics")
    p.add_argument("--bad-output", default="data/bad_records")
    p.add_argument("--checkpoint", default="data/checkpoints")
    p.add_argument("--once", action="store_true",
                   help="process everything available, then stop (good for tests)")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("AdPulseStream")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "latest")
               .load())
    else:
        raw = (spark.readStream.format("text")
               .option("maxFilesPerTrigger", 50)
               .load(args.input))

    events = parse_events(raw)

    # 1. Broken records go to their own folder, so nothing disappears silently.
    bad = events.filter(~is_valid())
    bad_query = (bad.writeStream.format("json")
                 .option("path", args.bad_output)
                 .option("checkpointLocation", os.path.join(args.checkpoint, "bad"))
                 .outputMode("append"))

    # 2. Good records: wait up to 5 minutes for late events (the watermark),
    #    drop duplicate sends by event_id, then count per minute.
    good = (clean(events)
            .withWatermark("event_time", "5 minutes")
            .dropDuplicates(["event_id", "event_time"]))
    metrics = campaign_metrics(good, "1 minute")

    metrics_query = (metrics.writeStream.format("parquet")
                     .option("path", args.output)
                     .option("checkpointLocation", os.path.join(args.checkpoint, "metrics"))
                     .outputMode("append"))

    if args.once:
        bad_query = bad_query.trigger(availableNow=True)
        metrics_query = metrics_query.trigger(availableNow=True)
    else:
        bad_query = bad_query.trigger(processingTime="10 seconds")
        metrics_query = metrics_query.trigger(processingTime="10 seconds")

    q1 = bad_query.start()
    q2 = metrics_query.start()
    q1.awaitTermination()
    q2.awaitTermination()


if __name__ == "__main__":
    main()
