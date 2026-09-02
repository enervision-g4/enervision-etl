import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIRECTORY = Path(__file__).parent / "fixtures"


def load_api_fixture(fixture_name: str) -> Any:
    return json.loads((FIXTURES_DIRECTORY / fixture_name).read_text(encoding="utf-8"))


@pytest.fixture
def good_reading_payload() -> dict[str, Any]:
    return load_api_fixture("reading_good.json")


@pytest.fixture
def partial_reading_payload() -> dict[str, Any]:
    return load_api_fixture("reading_partial.json")


@pytest.fixture
def critical_reading_payload() -> dict[str, Any]:
    return load_api_fixture("reading_critical.json")


@pytest.fixture
def degraded_series_payload() -> list[dict[str, Any]]:
    return load_api_fixture("readings_degraded_series.json")


@pytest.fixture
def site_registry_payload() -> list[dict[str, Any]]:
    return load_api_fixture("sites.json")
