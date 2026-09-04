from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from enervision_consumer.orchestration.persistence_consumer import PersistenceConsumer
from enervision_contracts.envelope import (
    CollectionMode,
    EventType,
    MeasureImputedPayload,
    MeasureRawPayload,
    MessageEnvelope,
    SitePayload,
)
from enervision_contracts.imputed_reading import ImputationMethod

from .conftest import FakeConsumerMessage

SITE_TOPIC = "enervision.site"
RAW_TOPIC = "enervision.measure_raw"
IMPUTED_TOPIC = "enervision.measure_imputed"
MEASURED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)
RAW_ID = UUID("2f1c8b3a-5d47-4e21-9a6f-0c3b7e8d1a52")


def site_message() -> FakeConsumerMessage:
    envelope = MessageEnvelope[SitePayload](
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
    return FakeConsumerMessage(SITE_TOPIC, envelope.model_dump_json().encode("utf-8"))


def raw_message() -> FakeConsumerMessage:
    envelope = MessageEnvelope[MeasureRawPayload](
        event_type=EventType.MEASURE_RAW,
        produced_at=MEASURED_AT,
        collection_mode=CollectionMode.REALTIME,
        payload=MeasureRawPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            consumption_kw=542.1,
            data_quality="good",
        ),
    )
    return FakeConsumerMessage(RAW_TOPIC, envelope.model_dump_json().encode("utf-8"))


def imputed_message() -> FakeConsumerMessage:
    envelope = MessageEnvelope[MeasureImputedPayload](
        event_type=EventType.MEASURE_IMPUTED,
        produced_at=MEASURED_AT,
        collection_mode=CollectionMode.REALTIME,
        payload=MeasureImputedPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            consumption_kw=542.1,
            imputation_method=ImputationMethod.FORWARD_FILL,
        ),
    )
    return FakeConsumerMessage(IMPUTED_TOPIC, envelope.model_dump_json().encode("utf-8"))


def build_consumer(kafka: Any, connection: Any) -> PersistenceConsumer:
    return PersistenceConsumer(
        consumer=kafka,
        connection=connection,
        site_topic=SITE_TOPIC,
        measure_raw_topic=RAW_TOPIC,
        measure_imputed_topic=IMPUTED_TOPIC,
    )


def test_the_consumer_subscribes_to_the_three_topics_it_persists(
    consumer: Any,
    connection: Any,
) -> None:
    kafka = consumer([site_message()])

    build_consumer(kafka, connection).run(max_messages=1)

    assert set(kafka.subscribed) == {SITE_TOPIC, RAW_TOPIC, IMPUTED_TOPIC}


def test_a_site_message_reaches_the_registry_table(consumer: Any, connection: Any) -> None:
    kafka = consumer([site_message()])

    report = build_consumer(kafka, connection).run(max_messages=1)

    assert "INSERT INTO site" in connection.opened_cursor.statements[0]
    assert connection.opened_cursor.parameters[0][0] == "SITE002"
    assert report.sites_written == 1


def test_a_raw_measure_is_written_then_acknowledged(consumer: Any, connection: Any) -> None:
    kafka = consumer([raw_message()])

    report = build_consumer(kafka, connection).run(max_messages=1)

    assert "INSERT INTO measure_raw" in connection.opened_cursor.statements[0]
    assert report.raw_measures_written == 1
    assert len(kafka.committed) == 1


def test_an_imputed_measure_is_linked_to_its_raw_row(
    consumer: Any,
    journalled_connection: Any,
) -> None:
    # La correlation passe par (site_id, timestamp) : le message n'apporte pas
    # l'identifiant technique, le consumer va le chercher.
    connection = journalled_connection([], (RAW_ID,))
    kafka = consumer([imputed_message()])

    report = build_consumer(kafka, connection).run(max_messages=1)

    assert "SELECT measure_raw_id" in connection.opened_cursor.statements[0]
    assert connection.opened_cursor.parameters[1][0] == RAW_ID
    assert report.imputed_measures_written == 1


def test_the_database_is_committed_before_the_offset(
    consumer: Any,
    journalled_connection: Any,
) -> None:
    # La regle qui protege de la perte silencieuse : acquitter l'offset avant
    # l'ecriture perdrait le message si le processus tombait entre les deux.
    journal: list[str] = []
    connection = journalled_connection(journal)
    kafka = consumer([raw_message()], journal)

    build_consumer(kafka, connection).run(max_messages=1)

    assert journal == ["base", "offset"]
