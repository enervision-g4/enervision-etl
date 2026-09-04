from datetime import UTC, datetime
from typing import Any

from psycopg.errors import ForeignKeyViolation

from enervision_consumer.orchestration.alerting_consumer import AlertingConsumer
from enervision_contracts.envelope import (
    AlertPayload,
    CollectionMode,
    EventType,
    MessageEnvelope,
    SitePayload,
)

from .conftest import FakeConsumerMessage
from .test_persistence_consumer import RecordingRegistryRefresh

SITE_TOPIC = "enervision.site"
ALERT_TOPIC = "enervision.alert"
MEASURE_TOPIC = "enervision.measure_raw"
RAISED_AT = datetime(2024, 6, 15, 14, 12, tzinfo=UTC)


def alert_message() -> FakeConsumerMessage:
    envelope = MessageEnvelope[AlertPayload](
        event_type=EventType.ALERT,
        produced_at=RAISED_AT,
        collection_mode=CollectionMode.REALTIME,
        payload=AlertPayload(
            site_id="SITE002",
            timestamp=RAISED_AT,
            source_alert_id="ALR-SITE002-1718458320",
            severity="critical",
            type="outage",
            message="Risque de surcharge",
            value_kw=812.5,
            threshold_kw=720.0,
        ),
    )
    return FakeConsumerMessage(ALERT_TOPIC, envelope.model_dump_json().encode("utf-8"))


def site_message() -> FakeConsumerMessage:
    envelope = MessageEnvelope[SitePayload](
        event_type=EventType.SITE,
        produced_at=RAISED_AT,
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


def build_consumer(kafka: Any, connection: Any, refresh: Any = None) -> AlertingConsumer:
    return AlertingConsumer(
        consumer=kafka,
        connection=connection,
        site_topic=SITE_TOPIC,
        alert_topic=ALERT_TOPIC,
        refresh_site_registry=refresh if refresh is not None else RecordingRegistryRefresh(),
    )


def test_the_alerting_consumer_ignores_the_measure_topics(
    consumer: Any,
    connection: Any,
) -> None:
    # Deux consumer groups distincts sur des topics distincts : l'alerting n'a aucune
    # raison de relire les mesures, que la persistance ecrit deja.
    kafka = consumer([alert_message()])

    build_consumer(kafka, connection).run(max_messages=1)

    assert set(kafka.subscribed) == {SITE_TOPIC, ALERT_TOPIC}
    assert MEASURE_TOPIC not in kafka.subscribed


def test_an_alert_is_written_then_acknowledged(consumer: Any, connection: Any) -> None:
    kafka = consumer([alert_message()])

    report = build_consumer(kafka, connection).run(max_messages=1)

    assert "INSERT INTO alert" in connection.opened_cursor.statements[0]
    assert connection.opened_cursor.parameters[0][0] == "ALR-SITE002-1718458320"
    assert report.alerts_written == 1
    assert len(kafka.committed) == 1


def test_the_registry_is_kept_up_to_date_by_this_consumer_too(
    consumer: Any,
    connection: Any,
) -> None:
    # alert.site_id est aussi une cle etrangere : ce consumer tient sa propre vue du
    # referentiel, sans dependre de l'avancement de celui de persistance.
    kafka = consumer([site_message()])

    report = build_consumer(kafka, connection).run(max_messages=1)

    assert "INSERT INTO site" in connection.opened_cursor.statements[0]
    assert report.sites_written == 1


def test_an_alert_whose_site_is_unknown_is_retried_after_a_refresh(
    consumer: Any,
    recovering_connection: Any,
) -> None:
    connection = recovering_connection(ForeignKeyViolation("site absent"), 1)
    kafka = consumer([alert_message()])
    refresh = RecordingRegistryRefresh()

    report = build_consumer(kafka, connection, refresh).run(max_messages=1)

    assert refresh.calls == 2
    assert report.alerts_written == 1
    assert len(kafka.committed) == 1


def test_the_database_is_committed_before_the_offset(
    consumer: Any,
    journalled_connection: Any,
) -> None:
    journal: list[str] = []
    connection = journalled_connection(journal)
    kafka = consumer([alert_message()], journal)

    build_consumer(kafka, connection, RecordingRegistryRefresh(journal)).run(max_messages=1)

    assert journal == ["referentiel", "base", "offset"]
