"""Shared test fixtures."""

import json
from pathlib import Path

import pytest

SAMPLE_EVENTS_DIR = Path(__file__).parent / "sample_events"


def load_sample(name: str) -> dict:
    """Load a sample event fixture."""
    with open(SAMPLE_EVENTS_DIR / name) as f:
        return json.load(f)


@pytest.fixture
def oracle_snapshot():
    return load_sample("oracle_snapshot.json")


@pytest.fixture
def oracle_insert():
    return load_sample("oracle_insert.json")


@pytest.fixture
def oracle_update():
    return load_sample("oracle_update.json")


@pytest.fixture
def oracle_delete():
    return load_sample("oracle_delete.json")


@pytest.fixture
def oracle_ddl():
    return load_sample("oracle_ddl.json")
