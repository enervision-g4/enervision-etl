from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from enervision_contracts.alert import Alert
from enervision_contracts.energy_reading import EnergyReading
from enervision_etl.transform.normalization import (
    compute_load_percent,
    normalize_alert,
    normalize_reading,
    to_utc,
)

PARIS = "Europe/Paris"


def test_naive_timestamp_declared_utc_is_only_made_explicit() -> None:
    normalized = to_utc(datetime(2024, 6, 15, 14, 32, 0, 123456), "UTC")

    assert normalized.tzinfo == UTC
    assert normalized.hour == 14
    assert normalized.microsecond == 123456


def test_naive_summer_timestamp_declared_paris_is_shifted_by_two_hours() -> None:
    normalized = to_utc(datetime(2024, 6, 15, 14, 32, 0), PARIS)

    assert normalized.tzinfo == UTC
    assert normalized.hour == 12


def test_naive_winter_timestamp_declared_paris_is_shifted_by_one_hour() -> None:
    normalized = to_utc(datetime(2024, 1, 15, 14, 32, 0), PARIS)

    assert normalized.tzinfo == UTC
    assert normalized.hour == 13


def test_already_aware_timestamp_ignores_the_declared_source_timezone() -> None:
    # Un horodatage deja situe porte son propre decalage : le fuseau suppose pour les
    # horodatages naifs ne doit pas le reinterpreter.
    aware_in_paris = datetime(2024, 6, 15, 14, 32, tzinfo=ZoneInfo(PARIS))

    normalized = to_utc(aware_in_paris, "UTC")

    assert normalized.tzinfo == UTC
    assert normalized.hour == 12


def test_conversion_is_idempotent() -> None:
    once = to_utc(datetime(2024, 6, 15, 14, 32), PARIS)
    twice = to_utc(once, PARIS)

    assert once == twice


def test_unknown_timezone_is_rejected() -> None:
    with pytest.raises(ValueError):
        to_utc(datetime(2024, 6, 15, 14, 32), "Mars/Olympus_Mons")


def test_load_percent_is_a_percentage_of_capacity() -> None:
    assert compute_load_percent(87.34, 200) == 43.67


def test_load_percent_of_a_missing_measurement_stays_missing() -> None:
    assert compute_load_percent(None, 200) is None


def test_load_percent_above_one_hundred_is_allowed() -> None:
    # Une surcharge est un evenement metier reel, la valeur ne doit pas etre plafonnee.
    assert compute_load_percent(812.5, 720) == 112.85


@pytest.mark.parametrize("invalid_capacity", [0, -1])
def test_non_positive_capacity_is_rejected(invalid_capacity: float) -> None:
    with pytest.raises(ValueError):
        compute_load_percent(87.34, invalid_capacity)


def test_normalize_reading_converts_the_timestamp(
    good_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(good_reading_payload)

    normalized = normalize_reading(reading, PARIS)

    assert normalized.timestamp.tzinfo == UTC
    assert normalized.timestamp.hour == 12


def test_normalize_reading_never_touches_a_missing_measurement(
    critical_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(critical_reading_payload)

    normalized = normalize_reading(reading, "UTC")

    assert normalized.consumption_kw is None
    assert normalized.temperature_celsius is None
    assert normalized.null_reasons == ["network_loss"]
    assert normalized.data_quality == "critical"


def test_normalize_reading_preserves_measured_values(
    partial_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(partial_reading_payload)

    normalized = normalize_reading(reading, "UTC")

    assert normalized.consumption_kw == 91.20
    assert normalized.humidity_percent == 60.2
    assert normalized.temperature_celsius is None


def test_normalize_reading_returns_a_new_immutable_instance(
    good_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(good_reading_payload)

    normalized = normalize_reading(reading, PARIS)

    assert normalized is not reading
    assert reading.timestamp.tzinfo is None


def test_normalize_alert_converts_the_timestamp(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    alert = Alert.model_validate(active_alerts_payload[0])

    normalized = normalize_alert(alert, PARIS)

    assert normalized.timestamp.tzinfo == UTC
    assert normalized.timestamp.hour == 12


def test_normalize_alert_preserves_every_other_field(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    alert = Alert.model_validate(active_alerts_payload[0])

    normalized = normalize_alert(alert, "UTC")

    assert normalized.alert_id == "ALR-SITE002-1718458320"
    assert normalized.severity == "critical"
    assert normalized.type == "outage"
    assert normalized.value == 812.5
    assert normalized.threshold == 720.0


def test_normalize_alert_keeps_an_absent_measurement_null(
    active_alerts_payload: list[dict[str, Any]],
) -> None:
    # Une alerte sans mesure associee ne doit pas ressortir a zero kW.
    alert = Alert.model_validate(
        active_alerts_payload[0] | {"value": None, "threshold": None}
    )

    normalized = normalize_alert(alert, "UTC")

    assert normalized.value is None
    assert normalized.threshold is None
