from typing import Any

import pytest
from pydantic import ValidationError

from enervision_contracts.site import Site


def test_site_registry_is_parsed(site_registry_payload: list[dict[str, Any]]) -> None:
    sites = [Site.model_validate(payload) for payload in site_registry_payload]

    assert [site.site_id for site in sites] == ["SITE001", "SITE002", "SITE003"]
    assert sites[1].capacity_kw == 1000
    assert sites[2].site_type == "datacenter"


def test_zero_capacity_is_rejected(site_registry_payload: list[dict[str, Any]]) -> None:
    invalid_site_payload = site_registry_payload[0] | {"capacity_kw": 0}

    with pytest.raises(ValidationError):
        Site.model_validate(invalid_site_payload)


def test_site_is_immutable(site_registry_payload: list[dict[str, Any]]) -> None:
    site = Site.model_validate(site_registry_payload[0])

    with pytest.raises(ValidationError):
        site.capacity_kw = 999
