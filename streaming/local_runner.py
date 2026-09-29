from __future__ import annotations

import argparse
import json
from pathlib import Path

from streaming.processing import aggregate_windows, clean_event, deduplicate_events, detect_alerts


def process_jsonl(input_path: Path, output_path: Path) -> tuple[int, int]:
    cleaned = []
    rejected = 0
    with input_path.open("r", encoding="utf-8") as input_file:
        for line in input_file:
            try:
                event = clean_event(json.loads(line))
            except json.JSONDecodeError:
                event = None
            if event is None:
                rejected += 1
            else:
                cleaned.append(event)

    unique_events = deduplicate_events(cleaned)
    aggregates = aggregate_windows(unique_events)
    with output_path.open("w", encoding="utf-8") as output_file:
        for aggregate in aggregates:
            serializable = dict(aggregate)
            serializable["window_start"] = serializable["window_start"].isoformat()
            serializable["window_end"] = serializable["window_end"].isoformat()
            output_file.write(json.dumps(serializable) + "\n")
    return len(unique_events), rejected


def main() -> None:
    parser = argparse.ArgumentParser(description="Process sample vitals JSONL without Kafka or Spark")
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    accepted, rejected = process_jsonl(args.input, args.output)
    print(f"accepted={accepted} rejected={rejected}")


if __name__ == "__main__":
    main()
