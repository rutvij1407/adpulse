"""
AdPulse live dashboard. Reads the Parquet metrics the streaming job writes
and refreshes every few seconds.

Run:  streamlit run dashboard/app.py
"""

import os
import time

import pandas as pd
import streamlit as st

METRICS_PATH = "data/metrics"
REFRESH_SECONDS = 10

st.set_page_config(page_title="AdPulse", layout="wide")
st.title("AdPulse: live ad campaign metrics")


def load():
    try:
        df = pd.read_parquet(METRICS_PATH)
    except Exception:
        return pd.DataFrame()
    return df.sort_values("window_start")


df = load()
if df.empty:
    st.info("No metrics yet. Start the producer and the streaming job, then wait a minute.")
else:
    totals = df.groupby("campaign_id")[["impressions", "clicks", "spend_usd"]].sum()
    totals["ctr"] = (totals["clicks"] / totals["impressions"]).round(4)
    totals["cost_per_click"] = (totals["spend_usd"] / totals["clicks"]).round(2)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Impressions", f"{int(totals.impressions.sum()):,}")
    c2.metric("Clicks", f"{int(totals.clicks.sum()):,}")
    c3.metric("Overall CTR", f"{totals.clicks.sum() / totals.impressions.sum():.2%}")
    c4.metric("Spend", f"${totals.spend_usd.sum():,.2f}")

    st.subheader("Click through rate by campaign, per minute")
    ctr = df.pivot_table(index="window_start", columns="campaign_id", values="ctr")
    st.line_chart(ctr)

    st.subheader("Campaign totals")
    st.dataframe(totals.sort_values("ctr", ascending=False), width="stretch")

    st.caption(f"Last window: {df.window_end.max()} UTC. Refreshes every {REFRESH_SECONDS} seconds.")

if os.environ.get("ADPULSE_NO_REFRESH") != "1":
    time.sleep(REFRESH_SECONDS)
    st.rerun()
