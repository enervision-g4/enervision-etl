import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog

FIXTURES_DIRECTORY = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def isolated_logging() -> Iterator[None]:
    """Empeche la configuration de journalisation d'un test de fuir dans les suivants.

    configure_logging retient le flux de sortie qu'on lui donne, et cette configuration
    est globale au processus. Tout test qui lance une commande, ou qui capture stderr,
    laisserait sinon derriere lui un flux que pytest refermera, et les tests suivants
    ecriraient dans un fichier ferme.
    """
    yield
    structlog.reset_defaults()


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


@pytest.fixture
def active_alerts_payload() -> list[dict[str, Any]]:
    return load_api_fixture("alerts_active.json")
