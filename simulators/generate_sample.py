import itertools
import json
from pathlib import Path
from simulators.vitals_simulator import stream_vitals


def main(n: int = 100):
    Path("docs/samples").mkdir(parents=True, exist_ok=True)
    with open("docs/samples/vitals_sample.jsonl", "w", encoding="utf-8") as f:
        for reading in itertools.islice(stream_vitals(), n):
            f.write(json.dumps(reading) + "\n")


if __name__ == "__main__":
    main()
