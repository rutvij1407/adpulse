"""
AdPulse event producer.

Pretends to be an ad server. It creates fake ad events (an ad was shown,
or an ad was clicked) and sends them to Kafka, one JSON message per event.

Run with Kafka:   python producer/event_producer.py --sink kafka
Run without it:   python producer/event_producer.py --sink files --out data/incoming
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

# Each campaign has its own "true" click rate, so the dashboard has
# something real to find (some campaigns work better than others).
CAMPAIGNS = {
    "cmp_spring_sale": 0.045,
    "cmp_new_phone": 0.030,
    "cmp_music_trial": 0.060,
    "cmp_fitness_app": 0.020,
    "cmp_travel_deals": 0.035,
}
DEVICES = ["iphone", "ipad", "mac", "watch", "tv"]
COST_PER_CLICK = {"cmp_spring_sale": 0.80, "cmp_new_phone": 1.50,
                  "cmp_music_trial": 0.40, "cmp_fitness_app": 0.60,
                  "cmp_travel_deals": 1.10}


def make_events(now):
    """One ad impression, sometimes followed by a click on the same ad."""
    campaign = random.choice(list(CAMPAIGNS))
    base = {
        "campaign_id": campaign,
        "ad_id": f"{campaign}_ad{random.randint(1, 3)}",
        "user_id": f"u{random.randint(1, 5000)}",
        "device": random.choice(DEVICES),
    }
    events = [dict(base, event_id=str(uuid.uuid4()), event_type="impression",
                   event_time=now.isoformat(), cost_usd=0.0)]
    if random.random() < CAMPAIGNS[campaign]:
        click_time = now + timedelta(seconds=random.randint(1, 20))
        events.append(dict(base, event_id=str(uuid.uuid4()), event_type="click",
                           event_time=click_time.isoformat(),
                           cost_usd=COST_PER_CLICK[campaign]))
    return events


def add_real_world_mess(events):
    """Real data is messy. About 1% of messages are duplicates, 1% arrive
    late, and 0.5% are broken, so the streaming job has to handle all three."""
    out = []
    for e in events:
        r = random.random()
        if r < 0.005:
            out.append({"bad": "record"})              # broken message
            continue
        if r < 0.015:
            late = datetime.fromisoformat(e["event_time"]) - timedelta(minutes=random.randint(1, 4))
            e = dict(e, event_time=late.isoformat())   # late arrival
        out.append(e)
        if random.random() < 0.01:
            out.append(dict(e))                         # duplicate send
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sink", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="ad-events")
    p.add_argument("--out", default="data/incoming")
    p.add_argument("--rate", type=int, default=200, help="impressions per second")
    p.add_argument("--seconds", type=int, default=0, help="0 means run forever")
    p.add_argument("--simulate-minutes", type=int, default=0,
                   help="files mode only: write this many minutes of data instantly")
    args = p.parse_args()

    producer = None
    if args.sink == "kafka":
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode())
    else:
        os.makedirs(args.out, exist_ok=True)

    sent, batch_no = 0, 0
    start = time.time()
    clock = datetime.now(timezone.utc)
    seconds_to_run = args.simulate_minutes * 60 if args.simulate_minutes else args.seconds

    while True:
        if args.simulate_minutes:
            clock += timedelta(seconds=1)          # fast forward fake time
        else:
            clock = datetime.now(timezone.utc)
        batch = []
        for _ in range(args.rate):
            batch.extend(make_events(clock))
        batch = add_real_world_mess(batch)

        if producer:
            for e in batch:
                producer.send(args.topic, e)
        else:
            path = os.path.join(args.out, f"batch_{batch_no:06d}.json")
            with open(path + ".tmp", "w") as f:
                f.write("\n".join(json.dumps(e) for e in batch))
            os.rename(path + ".tmp", path)        # atomic, so Spark never reads half a file
        sent += len(batch)
        batch_no += 1

        if seconds_to_run and batch_no >= seconds_to_run:
            break
        if not args.simulate_minutes:
            time.sleep(max(0.0, batch_no - (time.time() - start)))

    if producer:
        producer.flush()
    print(f"sent {sent} events in {batch_no} batches")


if __name__ == "__main__":
    main()
