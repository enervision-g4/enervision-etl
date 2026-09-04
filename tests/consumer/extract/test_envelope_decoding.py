from datetime import UTC, datetime
from typing import Any

import pytest

from enervision_consumer.extract.envelope_decoding import (
    EnvelopeDecodingError,
    decode_envelope,
)
from enervision_contracts.envelope import (
    AlertPayload,
    CollectionMode,
    EventType,
    MeasureImputedPayload,
    MeasureRawPayload,
    MessageEnvelope,
    SitePayload,
)
from enervision_contracts.imputed_reading import ImputationMethod

MEASURED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)
RAW_TOPIC = "enervision.measure_raw"

RAW_ENVELOPE = MessageEnvelope[MeasureRawPayload]
IMPUTED_ENVELOPE = MessageEnvelope[MeasureImputedPayload]
SITE_ENVELOPE = MessageEnvelope[SitePayload]
ALERT_ENVELOPE = MessageEnvelope[AlertPayload]


def encoded(envelope: MessageEnvelope[Any]) -> bytes:
    return envelope.model_dump_json().encode("utf-8")


def raw_envelope() -> MessageEnvelope[MeasureRawPayload]:
    return MessageEnvelope[MeasureRawPayload](
        event_type=EventType.MEASURE_RAW,
        produced_at=MEASURED_AT,
        collection_mode=CollectionMode.REALTIME,
        payload=MeasureRawPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            consumption_kw=None,
            null_reasons=["network_loss"],
            data_quality="critical",
        ),
    )


def test_a_raw_measure_is_decoded_into_its_payload_type() -> None:
    decoded = decode_envelope(RAW_TOPIC, encoded(raw_envelope()), RAW_ENVELOPE)

    assert decoded.event_type == EventType.MEASURE_RAW
    assert decoded.payload.site_id == "SITE002"
    assert decoded.payload.timestamp == MEASURED_AT


def test_a_null_measurement_survives_the_decoding() -> None:
    # La regle directrice du projet vaut jusqu'au bout de la chaine : le consumer
    # relit un None, pas un zero.
    decoded = decode_envelope(RAW_TOPIC, encoded(raw_envelope()), RAW_ENVELOPE)

    assert decoded.payload.consumption_kw is None
    assert decoded.payload.null_reasons == ["network_loss"]
    assert decoded.payload.data_quality == "critical"


def test_every_published_payload_type_can_be_decoded() -> None:
    imputed = MessageEnvelope[MeasureImputedPayload](
        event_type=EventType.MEASURE_IMPUTED,
        produced_at=MEASURED_AT,
        collection_mode=CollectionMode.BATCH,
        payload=MeasureImputedPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            consumption_kw=542.1,
            imputation_method=ImputationMethod.FORWARD_FILL,
        ),
    )
    site = MessageEnvelope[SitePayload](
        event_type=EventType.SITE,
        produced_at=MEASURED_AT,
        payload=SitePayload(
            site_id="SITE002",
            site_type="factory",
            site_name="Usine Lyon",
            location="Lyon, France",
            capacity_kw=1000,
            status="active",
        ),
    )
    alert = MessageEnvelope[AlertPayload](
        event_type=EventType.ALERT,
        produced_at=MEASURED_AT,
        collection_mode=CollectionMode.REALTIME,
        payload=AlertPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            source_alert_id="ALR-SITE002-1718458320",
            severity="critical",
            type="outage",
            message="Risque de surcharge",
            value_kw=812.5,
            threshold_kw=720.0,
        ),
    )

    assert decode_envelope("t", encoded(imputed), IMPUTED_ENVELOPE).payload.consumption_kw
    assert decode_envelope("t", encoded(site), SITE_ENVELOPE).payload.capacity_kw == 1000
    assert (
        decode_envelope("t", encoded(alert), ALERT_ENVELOPE).payload.source_alert_id
        == "ALR-SITE002-1718458320"
    )


def test_an_unreadable_message_is_rejected_with_its_topic() -> None:
    with pytest.raises(EnvelopeDecodingError) as rejet:
        decode_envelope(RAW_TOPIC, b"{ceci n'est pas du json", RAW_ENVELOPE)

    assert RAW_TOPIC in str(rejet.value)


def test_a_payload_violating_the_contract_is_rejected() -> None:
    # site_id est la cle de partition et la cle etrangere : sans lui, le message est
    # ininsérable, mieux vaut le signaler que de deviner.
    ampute = b'{"schema_version":"1.0.0","event_type":"measure_raw",' \
             b'"produced_at":"2024-06-15T14:32:00Z","payload":{"data_quality":"good"}}'

    with pytest.raises(EnvelopeDecodingError):
        decode_envelope(RAW_TOPIC, ampute, RAW_ENVELOPE)


def test_a_message_without_body_is_rejected_rather_than_crashing() -> None:
    # Une suppression sur un topic compacte arrive sous forme de message sans corps.
    # Le collecteur n'en produit pas : en recevoir un est une anomalie a signaler.
    with pytest.raises(EnvelopeDecodingError):
        decode_envelope("enervision.site", None, SITE_ENVELOPE)


def test_a_field_added_by_a_newer_producer_is_tolerated() -> None:
    # Seules les evolutions additives sont sures, et le contrat les accepte : un
    # consumer plus ancien que son producteur ne doit pas s'arreter pour autant.
    enrichi = raw_envelope().model_dump()
    enrichi["payload"]["pressure_hpa"] = 1013.2
    enrichi["emitted_by"] = "collector-2"

    decoded = decode_envelope(
        RAW_TOPIC,
        MessageEnvelope[MeasureRawPayload].model_validate(enrichi).model_dump_json().encode(),
        RAW_ENVELOPE,
    )

    assert decoded.payload.site_id == "SITE002"
