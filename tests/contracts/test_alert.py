from typing import Any

import pytest
from pydantic import ValidationError

from enervision_contracts.alert import Alert


def test_active_alerts_are_parsed(active_alerts_payload: list[dict[str, Any]]) -> None:
    alerts = [Alert.model_validate(payload) for payload in active_alerts_payload]

    assert [alert.site_id for alert in alerts] == ["SITE002", "SITE001", "SITE003"]
    assert alerts[0].severity == "critical"
    assert alerts[0].type == "outage"
    assert alerts[0].value == 812.5


def test_the_native_identifier_stays_a_string(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    # L'API numerote ses alertes en clair et non en UUID. Cet identifiant est la cle
    # d'idempotence du consumer : le convertir detruirait la deduplication.
    alert = Alert.model_validate(active_alerts_payload[0])

    assert alert.alert_id == "ALR-SITE002-1718458320"


def test_an_alert_without_measured_value_is_accepted(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    alert = Alert.model_validate(
        active_alerts_payload[0] | {"value": None, "threshold": None}
    )

    assert alert.value is None
    assert alert.threshold is None


def test_an_unknown_severity_is_accepted_but_flagged(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    # La liste des severites n'est pas fermee : une valeur inedite doit atteindre la
    # base plutot que d'etre rejetee, quitte a etre signalee en supervision.
    alert = Alert.model_validate(active_alerts_payload[0] | {"severity": "fatal"})

    assert alert.severity == "fatal"
    assert not alert.has_known_severity()


def test_a_documented_severity_is_recognized(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    alert = Alert.model_validate(active_alerts_payload[0])

    assert alert.has_known_severity()


def test_an_unknown_field_is_preserved(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    alert = Alert.model_validate(active_alerts_payload[0] | {"acknowledged_by": "ops"})

    assert alert.acknowledged_by == "ops"


def test_alert_is_immutable(active_alerts_payload: list[dict[str, Any]]) -> None:
    alert = Alert.model_validate(active_alerts_payload[0])

    with pytest.raises(ValidationError):
        alert.severity = "low"
