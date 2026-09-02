import io
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import (
    CollectionMode,
    envelope_for_raw_reading,
    envelope_for_site,
)
from enervision_contracts.site import Site
from enervision_etl.load.publisher import MessagePublisher
from enervision_etl.load.stdout_publisher import StdoutPublisher

MEASURE_TOPIC = "enervision.measure_raw"
SITE_TOPIC = "enervision.site"
MEASURED_AT = datetime(2024, 6, 15, 14, 32, 0, 123456, tzinfo=UTC)


@pytest.fixture
def captured_output() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def publisher(captured_output: io.StringIO) -> StdoutPublisher:
    return StdoutPublisher(stream=captured_output)


def build_envelope(payload: dict[str, Any], site_id: str = "SITE001"):
    reading = EnergyReading.model_validate(payload).model_copy(
        update={"timestamp": MEASURED_AT, "site_id": site_id}
    )
    return envelope_for_raw_reading(reading, CollectionMode.REALTIME)


def published_lines(captured_output: io.StringIO) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in captured_output.getvalue().splitlines() if line.strip()
    ]


def test_it_satisfies_the_publisher_protocol(publisher: StdoutPublisher) -> None:
    assert isinstance(publisher, MessagePublisher)


def test_a_message_produces_exactly_one_line(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert len(published_lines(captured_output)) == 1


def test_a_line_describes_the_topic_the_key_and_the_message(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    # La ligne reproduit ce que Kafka transporte, afin de pouvoir servir de jeu
    # d'essai au consumer sans qu'aucun broker ne soit necessaire.
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    published = published_lines(captured_output)[0]

    assert published["topic"] == MEASURE_TOPIC
    assert published["key"] == "SITE001"
    assert published["value"]["event_type"] == "measure_raw"
    assert published["value"]["payload"]["consumption_kw"] == 87.34


def test_the_key_always_comes_from_the_envelope(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, site_id="SITE007"))

    assert published_lines(captured_output)[0]["key"] == "SITE007"


def test_nulls_are_preserved_in_the_published_line(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    critical_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(critical_reading_payload))

    payload = published_lines(captured_output)[0]["value"]["payload"]

    assert payload["consumption_kw"] is None
    assert payload["null_reasons"] == ["network_loss"]


def test_messages_keep_their_publication_order(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    for site_id in ("SITE001", "SITE002", "SITE003"):
        publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, site_id))

    assert [line["key"] for line in published_lines(captured_output)] == [
        "SITE001",
        "SITE002",
        "SITE003",
    ]


def test_a_line_never_spans_several_physical_lines(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    # Un JSON indente casserait la relecture ligne par ligne par le consumer.
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert captured_output.getvalue().count("\n") == 1


def test_several_topics_share_the_same_stream(
    publisher: StdoutPublisher,
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
    site_registry_payload: list[dict[str, Any]],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))
    publisher.publish(SITE_TOPIC, envelope_for_site(Site.model_validate(site_registry_payload[0])))

    assert [line["topic"] for line in published_lines(captured_output)] == [
        MEASURE_TOPIC,
        SITE_TOPIC,
    ]


def test_flush_reports_no_pending_message(publisher: StdoutPublisher) -> None:
    # L'ecriture est synchrone : rien ne reste jamais en attente.
    assert publisher.flush() == 0


def test_published_messages_are_counted_per_topic(
    publisher: StdoutPublisher,
    good_reading_payload: dict[str, Any],
    site_registry_payload: list[dict[str, Any]],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE002"))
    publisher.publish(SITE_TOPIC, envelope_for_site(Site.model_validate(site_registry_payload[0])))

    assert publisher.published_counts == {MEASURE_TOPIC: 2, SITE_TOPIC: 1}


def test_it_can_be_used_as_a_context_manager(
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    with StdoutPublisher(stream=captured_output) as publisher:
        publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert len(published_lines(captured_output)) == 1


def test_closing_does_not_close_a_stream_it_does_not_own(
    captured_output: io.StringIO,
    good_reading_payload: dict[str, Any],
) -> None:
    # Fermer sys.stdout rendrait tout affichage ulterieur impossible.
    publisher = StdoutPublisher(stream=captured_output)
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    publisher.close()

    assert not captured_output.closed
