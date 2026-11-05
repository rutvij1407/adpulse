import json
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streaming"))
from transforms import campaign_metrics, clean, parse_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def raw(spark, events):
    return spark.createDataFrame([(json.dumps(e),) for e in events], ["value"])


def ev(i, kind, minute, campaign="cmp_a", cost=0.0):
    return {"event_id": f"e{i}", "event_type": kind,
            "event_time": f"2026-11-01T10:{minute:02d}:30+00:00",
            "campaign_id": campaign, "ad_id": "ad1", "user_id": "u1",
            "device": "iphone", "cost_usd": cost}


def test_broken_records_are_removed(spark):
    events = [ev(1, "impression", 0), {"bad": "record"}, dict(ev(2, "impression", 0), event_type="hover")]
    assert clean(parse_events(raw(spark, events))).count() == 1


def test_ctr_and_spend_per_window(spark):
    events = [ev(1, "impression", 0), ev(2, "impression", 0), ev(3, "impression", 0),
              ev(4, "impression", 0), ev(5, "click", 0, cost=0.5),
              ev(6, "impression", 1)]
    rows = {r["window_start"].minute: r for r in
            campaign_metrics(clean(parse_events(raw(spark, events)))).collect()}
    assert rows[0]["impressions"] == 4
    assert rows[0]["clicks"] == 1
    assert rows[0]["ctr"] == 0.25
    assert rows[0]["spend_usd"] == 0.5
    assert rows[1]["clicks"] == 0 and rows[1]["ctr"] == 0.0
