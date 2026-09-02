from typing import Any

import pytest

from enervision_contracts.site import Site
from enervision_etl.extract.site_selection import (
    UnknownConfiguredSiteError,
    resolve_site_identifiers,
)


@pytest.fixture
def registry(site_registry_payload: list[dict[str, Any]]) -> list[Site]:
    return [Site.model_validate(payload) for payload in site_registry_payload]


def test_an_empty_configuration_collects_every_exposed_site(registry: list[Site]) -> None:
    assert resolve_site_identifiers([], registry) == ["SITE001", "SITE002", "SITE003"]


def test_an_explicit_list_restricts_the_collection(registry: list[Site]) -> None:
    assert resolve_site_identifiers(["SITE003", "SITE001"], registry) == [
        "SITE001",
        "SITE003",
    ]


def test_the_registry_order_is_preserved(registry: list[Site]) -> None:
    # L'ordre de collecte ne doit pas dependre de l'ordre de saisie dans la configuration.
    assert resolve_site_identifiers(["SITE003", "SITE002"], registry) == [
        "SITE002",
        "SITE003",
    ]


def test_duplicates_in_the_configuration_are_collapsed(registry: list[Site]) -> None:
    assert resolve_site_identifiers(["SITE002", "SITE002"], registry) == ["SITE002"]


def test_site_identifiers_are_matched_regardless_of_case(registry: list[Site]) -> None:
    assert resolve_site_identifiers(["site002"], registry) == ["SITE002"]


def test_a_site_absent_from_the_api_is_rejected(registry: list[Site]) -> None:
    with pytest.raises(UnknownConfiguredSiteError) as raised:
        resolve_site_identifiers(["SITE002", "SITE404", "SITE999"], registry)

    assert raised.value.unknown_site_ids == ["SITE404", "SITE999"]


def test_an_empty_registry_is_rejected() -> None:
    # Collecter zero site serait une panne silencieuse : le service tournerait sans rien faire.
    with pytest.raises(ValueError):
        resolve_site_identifiers([], [])


def test_an_empty_registry_is_rejected_even_with_an_explicit_list() -> None:
    with pytest.raises(ValueError):
        resolve_site_identifiers(["SITE002"], [])
