from datetime import datetime, timedelta, timezone

from common.config import load_settings


def _anchor():
    settings = load_settings()["sim_clock"]

    real_start_str = settings["real_start"].replace("Z", "+00:00")
    real_start = datetime.fromisoformat(real_start_str)

    sim_start_date = datetime.fromisoformat(settings["sim_start_date"]).replace(tzinfo=timezone.utc)

    sim_day_seconds = float(settings["sim_day_seconds"])

    return real_start, sim_start_date, sim_day_seconds


def now() -> datetime:
    real_start, sim_start_date, sim_day_seconds = _anchor()

    current_real_time = datetime.now(timezone.utc)
    elapsed_real_seconds = (current_real_time - real_start).total_seconds()

    speedup = 86400 / sim_day_seconds

    return sim_start_date + timedelta(seconds=elapsed_real_seconds * speedup)


def current_sim_date():
    return now().date()