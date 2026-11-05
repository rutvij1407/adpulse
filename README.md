# AdPulse: real time ad click analytics

AdPulse watches a live stream of ad events and shows, minute by minute, how well each ad campaign is doing: how many people saw it, how many clicked, the click through rate (CTR), and how much money was spent.

It uses **Kafka** for the live stream, **PySpark Structured Streaming** to process it, **Parquet** for storage, a **Scala Spark** job for the daily report, and **Streamlit** for the dashboard.

```
event_producer.py  ──>  Kafka topic "ad-events"  ──>  stream_job.py (PySpark)  ──>  data/metrics (Parquet)
  (fake ad server)                                     cleans, dedupes, counts         │
                                                       per campaign per minute         ├──> dashboard/app.py (Streamlit, live)
                                                                                        └──> CampaignReport.scala (daily summary)
```

## The simple idea

Think of a shop counter that counts two things all day: how many people walked past an ad, and how many stopped and clicked it. Every minute, it writes the totals in a notebook. At the end of the day, someone reads the notebook and says which ad worked best. Kafka is the conveyor belt that brings the events in, Spark is the counter, Parquet is the notebook, and the Scala job is the person writing the end of day report.

## What it handles

Real data is messy, so the producer adds three problems on purpose, and the streaming job handles each one:

| Problem | How often | How AdPulse handles it |
| --- | --- | --- |
| Broken messages | about 0.5% | Kept out of the metrics and saved to `data/bad_records` so nothing disappears silently |
| Duplicate sends | about 1% | Removed with `dropDuplicates` on `event_id` |
| Late events | about 1%, up to 4 minutes late | A 5 minute **watermark** waits for them before a minute is finalized |

## Run it

Requires Python 3.9 to 3.12 (PySpark 3.5 does not support 3.13 or newer) and Java 17.

Install once:

```bash
pip install -r requirements.txt
```

### Option A: quick demo, no Kafka

```bash
./run_local_demo.sh             # makes 12 minutes of fake data and processes it
streamlit run dashboard/app.py  # open the dashboard
```

### Option B: real streaming with Kafka (needs Docker Desktop)

Use four terminal windows:

```bash
# 1. start Kafka
docker compose up -d

# 2. start the streaming job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/stream_job.py --source kafka

# 3. start sending ad events (200 per second)
python producer/event_producer.py --sink kafka --rate 200

# 4. open the live dashboard
streamlit run dashboard/app.py
```

Metrics for a minute appear once the 5 minute watermark has passed, so give it about 6 minutes before the first numbers show.

### The Scala report

```bash
cd scala && sbt package && cd ..
spark-submit --class adpulse.CampaignReport scala/target/scala-2.12/adpulse_2.12-0.1.0.jar data/metrics data/campaign_report
```

It prints and saves a table with each campaign's totals, CTR, cost per click, its best minute, and its CTR rank.

### Tests

```bash
pytest tests
```

## Results from a test run

Twelve simulated minutes, about 150,000 events. The job recovered each campaign's true CTR closely:

| Campaign | True CTR | Measured CTR |
| --- | --- | --- |
| cmp_music_trial | 6.0% | 5.83% |
| cmp_spring_sale | 4.5% | 4.56% |
| cmp_travel_deals | 3.5% | 3.55% |
| cmp_new_phone | 3.0% | 3.16% |
| cmp_fitness_app | 2.0% | 1.82% |

## Files

| File | What it does |
| --- | --- |
| `producer/event_producer.py` | Fake ad server that sends impressions and clicks to Kafka (or to files) |
| `streaming/transforms.py` | The data logic: parse, validate, count, CTR |
| `streaming/stream_job.py` | The streaming job: Kafka in, clean metrics out |
| `scala/.../CampaignReport.scala` | Scala Spark batch job for the campaign summary |
| `dashboard/app.py` | Live Streamlit dashboard |
| `tests/test_transforms.py` | Unit tests for the data logic |
| `docker-compose.yml` | One Kafka broker for your laptop |
