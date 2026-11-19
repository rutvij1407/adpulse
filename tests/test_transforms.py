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


def test_missing_cost_becomes_zero(spark):
    event = ev(1, "click", 0)
    event.pop("cost_usd")
    rows = clean(parse_events(raw(spark, [event]))).collect()
    assert rows[0]["cost_usd"] == 0.0


def test_non_json_text_is_dropped(spark):
    df = spark.createDataFrame([("not json at all",), (json.dumps(ev(1, "impression", 0)),)], ["value"])
    assert clean(parse_events(df)).count() == 1


def test_campaigns_are_counted_separately(spark):
    events = [ev(1, "impression", 0, "cmp_a"), ev(2, "impression", 0, "cmp_b"),
              ev(3, "click", 0, "cmp_b", cost=1.25)]
    rows = {r["campaign_id"]: r for r in campaign_metrics(clean(parse_events(raw(spark, events)))).collect()}
    assert rows["cmp_a"]["clicks"] == 0
    assert rows["cmp_b"]["clicks"] == 1 and rows["cmp_b"]["ctr"] == 1.0
    assert rows["cmp_b"]["spend_usd"] == 1.25


def test_clicks_without_impressions_do_not_divide_by_zero(spark):
    rows = campaign_metrics(clean(parse_events(raw(spark, [ev(1, "click", 0)])))).collect()
    assert rows[0]["impressions"] == 0 and rows[0]["ctr"] == 0.0
