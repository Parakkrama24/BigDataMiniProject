"""
Observability demo #1: kill the producer and watch the alert fire.

What this shows for the report/demo video:
  1. The producer is running and vitals_last_event_unix_seconds is advancing.
  2. We kill the producer process (simulating a crash).
  3. Prometheus's VitalsProducerDown alert rule transitions
     inactive -> pending -> firing once the gauge stops advancing for long
     enough (60s threshold + 30s "for" duration, see observability/alerts.yml).
  4. We restart the producer and show the alert clear back to inactive.

Run from the project root:
    python scripts/demo_kill_producer.py

Requires: the Docker stack up (docker compose up -d) so Prometheus is
scraping http://localhost:8001/metrics.
"""

import json
import subprocess
import sys
import time
import urllib.request

PROMETHEUS_URL = "http://localhost:9090/api/v1/alerts"
# VitalsProducerDown, not NoVitalsReceived: when the producer process dies its
# scrape fails and Prometheus marks vitals_last_event_unix_seconds stale, so
# expressions over that gauge return no data and can never fire. `up` is
# synthesised for every target and reliably drops to 0, so it is what actually
# catches a crash. NoVitalsReceived covers the other case -- alive but stalled.
ALERT_NAME = "VitalsProducerDown"
POLL_SECONDS = 5
MAX_POLLS = 24  # 24 * 5s = 120s, comfortably past the 90s (60s+30s) threshold


def get_alert_state(alert_name: str) -> str | None:
    """Return 'inactive' / 'pending' / 'firing' for the named alert, or None
    if Prometheus doesn't know about it yet (e.g. it's still 'inactive' and
    not returned by /alerts, which only lists pending/firing alerts)."""
    with urllib.request.urlopen(PROMETHEUS_URL, timeout=5) as resp:
        data = json.loads(resp.read())
    for alert in data["data"]["alerts"]:
        if alert["labels"].get("alertname") == alert_name:
            return alert["state"]
    return "inactive"  # not listed at all means it's not pending/firing


def watch_until(target_states: set, label: str) -> None:
    for i in range(MAX_POLLS):
        state = get_alert_state(ALERT_NAME)
        print(f"  [{i + 1:>2}/{MAX_POLLS}] {ALERT_NAME} state = {state}")
        if state in target_states:
            print(f"  -> reached {label} (state={state})")
            return
        time.sleep(POLL_SECONDS)
    print(f"  !! gave up waiting for {label} after {MAX_POLLS * POLL_SECONDS}s")


def main():
    print("== Starting producer ==")
    producer = subprocess.Popen([sys.executable, "-m", "ingestion.producer"])
    time.sleep(5)
    print(f"Producer running (pid={producer.pid}). Check alert is inactive:")
    print("  state =", get_alert_state(ALERT_NAME))

    print("\n== Killing producer to simulate an outage ==")
    producer.terminate()
    try:
        producer.wait(timeout=5)
    except subprocess.TimeoutExpired:
        producer.kill()
    print("Producer stopped. No more vitals will reach Kafka.")

    print(f"\n== Watching for {ALERT_NAME} to fire ==")
    print("(takes ~50s: the 30s for-duration plus rule evaluation)")
    watch_until({"firing"}, "FIRING")
    print(f"See it live at http://localhost:9090/alerts or the Grafana dashboard's 'Active alerts' panel.")

    input("\nPress Enter to restart the producer and clear the alert...")

    print("\n== Restarting producer ==")
    producer = subprocess.Popen([sys.executable, "-m", "ingestion.producer"])
    print(f"Producer running again (pid={producer.pid}).")

    print(f"\n== Watching for {ALERT_NAME} to clear ==")
    watch_until({"inactive"}, "CLEARED")

    print("\nDemo complete. Producer is still running -- press Ctrl+C to stop it.")
    try:
        producer.wait()
    except KeyboardInterrupt:
        producer.terminate()


if __name__ == "__main__":
    main()
