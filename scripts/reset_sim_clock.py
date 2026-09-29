"""
Resets the simulated clock's anchor so a fresh demo/dev session starts at
day 0 (sim_start_date) instead of wherever real_start left off drifting to.

Why this is needed: config/settings.yaml's sim_clock.real_start is a fixed
timestamp. Every second that passes after it (times the 288x speedup, by
default) pushes the simulated calendar further forward. If you don't reset
it before a demo, current_sim_date() may already be months or years past
sim_start_date -- harmless, but confusing to show graders.

Run from the project root, right before starting a demo/dev session:
    python scripts/reset_sim_clock.py
"""

from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SETTINGS_PATH = _REPO_ROOT / "config" / "settings.yaml"


def main():
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    text = _SETTINGS_PATH.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    updated = False
    for i, line in enumerate(lines):
        if line.strip().startswith("real_start:"):
            lines[i] = f'  real_start: "{now_iso}"\n'
            updated = True
            break

    if not updated:
        raise SystemExit("Could not find 'real_start:' in config/settings.yaml -- edit it manually.")

    _SETTINGS_PATH.write_text("".join(lines), encoding="utf-8")
    print(f"real_start set to {now_iso}")
    print("The simulated clock now starts fresh from sim_start_date on the next run.")


if __name__ == "__main__":
    main()
