from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from enervision_etl.contracts.energy_reading import (
    MEASUREMENT_FIELD_NAMES,
    EnergyReading,
)


def test_good_reading_is_parsed(good_reading_payload: dict[str, Any]) -> None:
    reading = EnergyReading.model_validate(good_reading_payload)

    assert reading.site_id == "SITE001"
    assert reading.consumption_kw == 87.34
    assert reading.data_quality == "good"
    assert reading.null_reasons == []
    assert reading.missing_measurement_fields() == ()


def test_partial_reading_keeps_the_failing_sensor_null(
    partial_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(partial_reading_payload)

    assert reading.temperature_celsius is None
    assert reading.consumption_kw == 91.20
    assert reading.humidity_percent == 60.2
    assert reading.null_reasons == ["temperature_sensor_failure"]
    assert reading.missing_measurement_fields() == ("temperature_celsius",)


def test_critical_reading_keeps_every_sensor_null(
    critical_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(critical_reading_payload)

    for field_name in MEASUREMENT_FIELD_NAMES:
        assert getattr(reading, field_name) is None

    assert reading.data_quality == "critical"
    assert reading.missing_measurement_fields() == MEASUREMENT_FIELD_NAMES


def test_null_survives_a_full_serialization_round_trip(
    critical_reading_payload: dict[str, Any],
) -> None:
    reading = EnergyReading.model_validate(critical_reading_payload)
    serialized_reading = reading.model_dump(mode="json")

    for field_name in MEASUREMENT_FIELD_NAMES:
        assert serialized_reading[field_name] is None, (
            f"{field_name} was silently replaced, the golden rule is violated"
        )


def test_degraded_series_is_parsed(degraded_series_payload: list[dict[str, Any]]) -> None:
    readings = [EnergyReading.model_validate(payload) for payload in degraded_series_payload]

    assert len(readings) == 2
    assert readings[1].consumption_kw is None
    assert readings[1].temperature_celsius == 18.5
    assert readings[1].null_reasons == [
        "consumption_sensor_failure",
        "electrical_sensor_failure",
    ]


def test_unknown_null_reason_is_accepted(good_reading_payload: dict[str, Any]) -> None:
    good_reading_payload["null_reasons"] = ["cosmic_ray_interference"]

    reading = EnergyReading.model_validate(good_reading_payload)

    assert reading.null_reasons == ["cosmic_ray_interference"]


def test_unknown_data_quality_is_accepted(good_reading_payload: dict[str, Any]) -> None:
    good_reading_payload["data_quality"] = "suspicious"

    reading = EnergyReading.model_validate(good_reading_payload)

    assert reading.data_quality == "suspicious"
    assert reading.has_known_data_quality() is False


def test_unexpected_api_field_is_preserved(good_reading_payload: dict[str, Any]) -> None:
    good_reading_payload["pressure_hpa"] = 1013.2

    reading = EnergyReading.model_validate(good_reading_payload)

    assert reading.model_dump()["pressure_hpa"] == 1013.2


def test_reading_is_immutable(good_reading_payload: dict[str, Any]) -> None:
    reading = EnergyReading.model_validate(good_reading_payload)

    with pytest.raises(ValidationError):
        reading.consumption_kw = 0.0


@pytest.mark.parametrize("mandatory_field", ["timestamp", "site_id", "data_quality"])
def test_mandatory_field_cannot_be_missing(
    good_reading_payload: dict[str, Any],
    mandatory_field: str,
) -> None:
    del good_reading_payload[mandatory_field]

    with pytest.raises(ValidationError):
        EnergyReading.model_validate(good_reading_payload)


@pytest.mark.parametrize("mandatory_field", ["timestamp", "site_id", "data_quality"])
def test_mandatory_field_cannot_be_null(
    good_reading_payload: dict[str, Any],
    mandatory_field: str,
) -> None:
    good_reading_payload[mandatory_field] = None

    with pytest.raises(ValidationError):
        EnergyReading.model_validate(good_reading_payload)


def test_missing_null_reasons_defaults_to_empty_list(
    good_reading_payload: dict[str, Any],
) -> None:
    del good_reading_payload["null_reasons"]

    reading = EnergyReading.model_validate(good_reading_payload)

    assert reading.null_reasons == []


def test_timestamp_is_parsed_as_datetime(good_reading_payload: dict[str, Any]) -> None:
    reading = EnergyReading.model_validate(good_reading_payload)

    assert isinstance(reading.timestamp, datetime)
    assert reading.timestamp.microsecond == 123456
