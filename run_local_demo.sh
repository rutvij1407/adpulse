#!/usr/bin/env bash
# Full demo with no Kafka: makes 12 minutes of fake ad data, runs the
# streaming job over it once, then runs the Scala report.
set -e
rm -rf data
python producer/event_producer.py --sink files --out data/incoming --rate 200 --simulate-minutes 12
python streaming/stream_job.py --source files --once
echo "Metrics written to data/metrics. Start the dashboard with: streamlit run dashboard/app.py"
