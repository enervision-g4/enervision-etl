import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import (
    SCHEMA_VERSION,
    CollectionMode,
    EventType,
    MeasureImputedPayload,
    MeasureRawPayload,
    MessageEnvelope,
    SitePayload,
    envelope_for_imputed_reading,
    envelope_for_raw_reading,
    envelope_for_site,
)
from enervision_contracts.imputed_reading import ImputationMethod, ImputedReading
from enervision_contracts.site import Site

MEASURED_AT = datetime(2024, 6, 15, 14, 32, 0, 123456, tzinfo=UTC)


@pytest.fixture
def utc_reading(good_reading_payload: dict[str, Any]) -> EnergyReading:
    return EnergyReading.model_validate(good_reading_payload).model_copy(
        update={"timestamp": MEASURED_AT}
    )


@pytest.fixture
def utc_critical_reading(critical_reading_payload: dict[str, Any]) -> EnergyReading:
    return EnergyReading.model_validate(critical_reading_payload).model_copy(
        update={"timestamp": MEASURED_AT}
    )


def test_a_raw_envelope_carries_the_measurement(utc_reading: EnergyReading) -> None:
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    assert envelope.event_type == EventType.MEASURE_RAW
    assert envelope.collection_mode == CollectionMode.REALTIME
    assert envelope.payload.site_id == "SITE001"
    assert envelope.payload.consumption_kw == 87.34
    assert envelope.payload.data_quality == "good"


def test_every_envelope_declares_the_schema_version(utc_reading: EnergyReading) -> None:
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    assert envelope.schema_version == SCHEMA_VERSION


def test_the_partition_key_is_the_site_identifier(utc_reading: EnergyReading) -> None:
    # La cle Kafka determine la partition, donc l'ordre chronologique par site.
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    assert envelope.partition_key == "SITE001"


def test_nulls_survive_until_the_serialized_json(
    utc_critical_reading: EnergyReading,
) -> None:
    envelope = envelope_for_raw_reading(utc_critical_reading, CollectionMode.REALTIME)

    serialized = json.loads(envelope.model_dump_json())

    for field_name in (
        "consumption_kw",
        "consumption_kwh",
        "voltage_v",
        "current_a",
        "power_factor",
        "temperature_celsius",
        "humidity_percent",
    ):
        assert serialized["payload"][field_name] is None
    assert serialized["payload"]["null_reasons"] == ["network_loss"]
    assert serialized["payload"]["data_quality"] == "critical"


def test_the_payload_holds_only_the_columns_of_the_data_model(
    utc_reading: EnergyReading,
) -> None:
    # site_type est une denormalisation de l'API, l'information appartient a SITE.
    # load_percent se recalcule par jointure, MEASURE_RAW n'a pas cette colonne.
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    serialized_payload = json.loads(envelope.model_dump_json())["payload"]

    assert set(serialized_payload) == {
        "site_id",
        "timestamp",
        "consumption_kw",
        "consumption_kwh",
        "voltage_v",
        "current_a",
        "power_factor",
        "temperature_celsius",
        "humidity_percent",
        "null_reasons",
        "data_quality",
    }


def test_a_naive_timestamp_is_refused(utc_reading: EnergyReading) -> None:
    # Publier un horodatage sans fuseau laisserait le consumer deviner. La normalisation
    # vers UTC doit avoir eu lieu avant la publication.
    naive_reading = utc_reading.model_copy(
        update={"timestamp": MEASURED_AT.replace(tzinfo=None)}
    )

    with pytest.raises(ValidationError):
        envelope_for_raw_reading(naive_reading, CollectionMode.REALTIME)


def test_a_non_utc_timestamp_is_converted(utc_reading: EnergyReading) -> None:
    paris_reading = utc_reading.model_copy(
        update={"timestamp": MEASURED_AT.astimezone(timezone(timedelta(hours=2)))}
    )

    envelope = envelope_for_raw_reading(paris_reading, CollectionMode.REALTIME)

    assert envelope.payload.timestamp == MEASURED_AT
    assert envelope.payload.timestamp.utcoffset() == timedelta(0)


def test_the_serialized_timestamp_carries_an_explicit_offset(
    utc_reading: EnergyReading,
) -> None:
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    serialized_timestamp = json.loads(envelope.model_dump_json())["payload"]["timestamp"]

    assert serialized_timestamp.startswith("2024-06-15T14:32:00.123456")
    assert serialized_timestamp.endswith("Z") or serialized_timestamp.endswith("+00:00")


def test_an_imputed_envelope_declares_its_method() -> None:
    imputed = ImputedReading(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        consumption_kw=515.0,
        imputation_method=ImputationMethod.LINEAR_INTERPOLATION,
        imputed_fields=("consumption_kw",),
    )

    envelope = envelope_for_imputed_reading(imputed, CollectionMode.BATCH)

    assert envelope.event_type == EventType.MEASURE_IMPUTED
    assert envelope.payload.imputation_method == ImputationMethod.LINEAR_INTERPOLATION
    assert envelope.payload.consumption_kw == 515.0
    assert envelope.partition_key == "SITE002"


def test_the_imputed_payload_never_exposes_a_technical_identifier() -> None:
    # measure_raw_id est un UUID genere a l'insertion par le consumer. Le collecteur ne
    # peut pas le connaitre : la correlation passe par la cle metier (site_id, timestamp).
    imputed = ImputedReading(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        imputation_method=ImputationMethod.NONE,
    )

    serialized_payload = json.loads(
        envelope_for_imputed_reading(imputed, CollectionMode.BATCH).model_dump_json()
    )["payload"]

    assert "measure_raw_id" not in serialized_payload
    assert serialized_payload["site_id"] == "SITE002"
    assert "timestamp" in serialized_payload


def test_a_site_envelope_carries_the_registry(
    site_registry_payload: list[dict[str, Any]],
) -> None:
    site = Site.model_validate(site_registry_payload[1])

    envelope = envelope_for_site(site)

    assert envelope.event_type == EventType.SITE
    assert envelope.collection_mode is None
    assert envelope.payload.capacity_kw == 1000
    assert envelope.partition_key == "SITE002"


def test_an_envelope_survives_a_full_round_trip(utc_reading: EnergyReading) -> None:
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    restored = MessageEnvelope[MeasureRawPayload].model_validate_json(
        envelope.model_dump_json()
    )

    assert restored == envelope


def test_an_unknown_field_added_by_a_newer_producer_is_tolerated(
    utc_reading: EnergyReading,
) -> None:
    # Pendant un deploiement, un consumer peut lire des messages produits par une version
    # plus recente. Rejeter un champ inconnu couperait la chaine pour rien.
    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)
    message = json.loads(envelope.model_dump_json())
    message["payload"]["pressure_hpa"] = 1013.2
    message["emitted_by"] = "collector-2"

    restored = MessageEnvelope[MeasureRawPayload].model_validate(message)

    assert restored.payload.consumption_kw == 87.34


def test_the_production_instant_is_recorded_in_utc(utc_reading: EnergyReading) -> None:
    before = datetime.now(UTC)

    envelope = envelope_for_raw_reading(utc_reading, CollectionMode.REALTIME)

    assert before <= envelope.produced_at <= datetime.now(UTC)


def test_payload_models_are_immutable() -> None:
    payload = SitePayload(
        site_id="SITE001",
        site_type="office",
        site_name="Bureau",
        location="Paris, France",
        capacity_kw=200,
        status="active",
    )

    with pytest.raises(ValidationError):
        payload.capacity_kw = 999


def test_an_imputed_payload_keeps_an_irrecoverable_value_null() -> None:
    imputed = ImputedReading(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        imputation_method=ImputationMethod.NONE,
    )

    payload = MeasureImputedPayload.model_validate(
        envelope_for_imputed_reading(imputed, CollectionMode.BATCH).payload.model_dump()
    )

    assert payload.consumption_kw is None
    assert payload.imputation_method == ImputationMethod.NONE
