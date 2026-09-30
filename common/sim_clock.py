"""The shared simulated clock.

The project compresses time: one simulated day passes every
sim_clock.sim_day_seconds real seconds (default 300 = 5 minutes, so 288x
real time). Every service that needs to know "what simulated day is it"
must go through this module and the same anchor in config/settings.yaml,
or their dates drift apart and the pipeline stages stop lining up.

    speedup       = 86400 / sim_day_seconds
    simulated_now = sim_start_date + speedup * (real_now - real_start)

All datetimes here are timezone-aware UTC.
"""

from datetime import date, datetime, timedelta, timezone

from common.config import load_settings

SECONDS_PER_DAY = 86400


def _anchor():
    settings = load_settings()["sim_clock"]

    real_start_str = settings["real_start"].replace("Z", "+00:00")
    real_start = datetime.fromisoformat(real_start_str)

    sim_start_date = datetime.fromisoformat(settings["sim_start_date"]).replace(tzinfo=timezone.utc)

    sim_day_seconds = float(settings["sim_day_seconds"])

    return real_start, sim_start_date, sim_day_seconds


def now(real_now: datetime | None = None) -> datetime:
    """The simulated datetime corresponding to a real instant.

    Args:
        real_now: the real UTC instant to translate. Defaults to right now.
            Pass an explicit value when you need a deterministic result --
            for example Airflow translating a run's logical date, where
            using the wall clock would make the same run produce a
            different simulated date on every retry.
    """
    real_start, sim_start_date, sim_day_seconds = _anchor()

    if real_now is None:
        real_now = datetime.now(timezone.utc)
    elif real_now.tzinfo is None:
        real_now = real_now.replace(tzinfo=timezone.utc)
    else:
        real_now = real_now.astimezone(timezone.utc)

    elapsed_real_seconds = (real_now - real_start).total_seconds()

    speedup = SECONDS_PER_DAY / sim_day_seconds

    return sim_start_date + timedelta(seconds=elapsed_real_seconds * speedup)


def current_sim_date() -> date:
    """The simulated calendar date right now."""
    return now().date()


def sim_date_for(real_ts: datetime) -> date:
    """The simulated calendar date that a given real instant maps to.

    Deterministic: the same real timestamp always yields the same simulated
    date, which is what makes an Airflow run idempotent across retries and
    safe to backfill.
    """
    return now(real_ts).date()


def real_window_for(sim_date: date) -> tuple[datetime, datetime]:
    """The [start, end) real-world UTC window during which sim_date is "today".

    Useful for reasoning about when a simulated day's data is complete.
    """
    real_start, sim_start_date, sim_day_seconds = _anchor()

    sim_midnight = datetime(sim_date.year, sim_date.month, sim_date.day, tzinfo=timezone.utc)
    days_since_sim_start = (sim_midnight - sim_start_date).total_seconds() / SECONDS_PER_DAY

    window_start = real_start + timedelta(seconds=days_since_sim_start * sim_day_seconds)
    window_end = window_start + timedelta(seconds=sim_day_seconds)
    return window_start, window_end
