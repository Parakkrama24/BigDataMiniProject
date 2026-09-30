"""Keeps MemoryRepository and PostgresRepository interchangeable.

api.main.create_app() swaps one for the other based on API_REPOSITORY, and
the routers call the repository without knowing which it got. If the two
drift apart, the API silently works in tests (memory) and breaks in
deployment (postgres), so this pins the shared surface.

These tests never connect to a database: PostgresRepository is constructed
with open=False pooling, so instantiating it is offline.
"""

import inspect
import os

import pytest

from api.main import create_app
from api.postgres_repository import PostgresRepository
from api.repository import MemoryRepository

# Everything the routers and the streaming local_runner rely on.
REQUIRED_METHODS = (
    "upsert_live",
    "record_alert",
    "resolve_alert",
    "latest_live",
    "patient_alerts",
    "last_data_received",
)


@pytest.mark.parametrize("name", REQUIRED_METHODS)
def test_both_repositories_expose_the_same_methods(name):
    memory = getattr(MemoryRepository, name, None)
    postgres = getattr(PostgresRepository, name, None)

    assert callable(memory), f"MemoryRepository is missing {name}"
    assert callable(postgres), f"PostgresRepository is missing {name}"

    memory_args = list(inspect.signature(memory).parameters)
    postgres_args = list(inspect.signature(postgres).parameters)
    assert memory_args == postgres_args, f"{name} signatures differ: {memory_args} vs {postgres_args}"


def test_both_repositories_expose_patients_for_the_404_check():
    # api/routers/alerts.py does `patient_id not in repository.patients`.
    assert isinstance(MemoryRepository().patients, set)
    assert isinstance(PostgresRepository.patients, property)


def test_create_app_defaults_to_memory_repository(monkeypatch):
    monkeypatch.delenv("API_REPOSITORY", raising=False)

    app = create_app()

    assert isinstance(app.state.repository, MemoryRepository)


def test_create_app_honours_postgres_backend(monkeypatch):
    monkeypatch.setenv("API_REPOSITORY", "postgres")
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused:unused@localhost:1/unused")

    app = create_app()

    # Constructed but never connected -- the pool opens lazily on first query.
    assert isinstance(app.state.repository, PostgresRepository)


def test_create_app_rejects_an_unknown_backend(monkeypatch):
    monkeypatch.setenv("API_REPOSITORY", "mysql")

    with pytest.raises(ValueError, match="Unknown API_REPOSITORY"):
        create_app()
