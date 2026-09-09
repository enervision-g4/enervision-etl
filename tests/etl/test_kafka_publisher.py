import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Optional

import pytest

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import CollectionMode, envelope_for_raw_reading
from enervision_etl.load.errors import MessagePublicationError
from enervision_etl.load.kafka_publisher import (
    KafkaPublisher,
    build_producer_configuration,
)
from enervision_etl.load.publisher import MessagePublisher

MEASURE_TOPIC = "enervision.measure_raw"
MEASURED_AT = datetime(2024, 6, 15, 14, 32, 0, 123456, tzinfo=UTC)


class DeliveryFailure:
    """Reproduit l'objet d'erreur passe au callback par confluent_kafka."""

    def __init__(self, reason: str) -> None:
        self._reason = reason

    def __str__(self) -> str:
        return self._reason


class FakeMessage:
    """Reproduit l'objet Message rendu par confluent_kafka au callback."""

    def __init__(self, topic: str, key: Optional[bytes], value: Optional[bytes]) -> None:
        self._topic = topic
        self._key = key
        self._value = value

    def topic(self) -> str:
        return self._topic

    def key(self) -> Optional[bytes]:
        return self._key

    def value(self) -> Optional[bytes]:
        return self._value


class FakeProducer:
    """Producer en memoire reproduisant le contrat de confluent_kafka.Producer.

    Reproduit notamment les deux pieges du client reel : les callbacks de livraison ne
    se declenchent qu'a l'appel de poll ou flush, et produce leve BufferError lorsque la
    file locale est pleine.
    """

    def __init__(
        self,
        queue_capacity: Optional[int] = None,
        delivery_failure: Optional[str] = None,
    ) -> None:
        self.delivered: list[FakeMessage] = []
        self.poll_calls = 0
        self.flush_calls = 0
        self._pending: list[tuple[FakeMessage, Optional[Callable[..., None]]]] = []
        self._queue_capacity = queue_capacity
        self._delivery_failure = delivery_failure

    def produce(
        self,
        topic: str,
        *,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        on_delivery: Optional[Callable[..., None]] = None,
    ) -> None:
        if self._queue_capacity is not None and len(self._pending) >= self._queue_capacity:
            raise BufferError("Local: Queue full")
        self._pending.append((FakeMessage(topic, key, value), on_delivery))

    def poll(self, timeout: float = 0) -> int:
        self.poll_calls += 1
        return self._drain()

    def flush(self, timeout: float = 0) -> int:
        self.flush_calls += 1
        self._drain()
        return len(self._pending)

    def __len__(self) -> int:
        return len(self._pending)

    def _drain(self) -> int:
        served = len(self._pending)
        for message, callback in self._pending:
            if callback is not None:
                failure = (
                    DeliveryFailure(self._delivery_failure) if self._delivery_failure else None
                )
                callback(failure, message)
            if self._delivery_failure is None:
                self.delivered.append(message)
        self._pending.clear()
        return served


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def publisher(fake_producer: FakeProducer) -> KafkaPublisher:
    return KafkaPublisher(producer=fake_producer)


def build_envelope(payload: dict[str, Any], site_id: str = "SITE001"):
    reading = EnergyReading.model_validate(payload).model_copy(
        update={"timestamp": MEASURED_AT, "site_id": site_id}
    )
    return envelope_for_raw_reading(reading, CollectionMode.REALTIME)


def test_it_satisfies_the_publisher_protocol(publisher: KafkaPublisher) -> None:
    assert isinstance(publisher, MessagePublisher)


def test_the_producer_is_configured_for_exactly_once_delivery() -> None:
    # Sans idempotence, un rejeu reseau duplique silencieusement les messages.
    configuration = build_producer_configuration("kafka:9092")

    assert configuration["bootstrap.servers"] == "kafka:9092"
    assert configuration["enable.idempotence"] is True
    assert configuration["acks"] == "all"


def test_a_message_reaches_the_producer(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))
    publisher.flush()

    assert len(fake_producer.delivered) == 1
    assert fake_producer.delivered[0].topic() == MEASURE_TOPIC


def test_the_key_is_the_site_identifier_encoded_in_utf8(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE007"))
    publisher.flush()

    assert fake_producer.delivered[0].key() == b"SITE007"


def test_the_value_is_the_serialized_envelope(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    critical_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(critical_reading_payload))
    publisher.flush()

    raw_value = fake_producer.delivered[0].value()
    assert raw_value is not None
    message = json.loads(raw_value.decode("utf-8"))

    assert message["schema_version"] == "1.0.0"
    assert message["payload"]["consumption_kw"] is None
    assert message["payload"]["null_reasons"] == ["network_loss"]


def test_publishing_serves_pending_delivery_callbacks(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    # Sans appel a poll, les callbacks ne se declenchent jamais et la file locale enfle.
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert fake_producer.poll_calls >= 1


def test_a_full_local_queue_is_drained_then_the_message_is_retried(
    good_reading_payload: dict[str, Any],
) -> None:
    producer_with_tiny_queue = FakeProducer(queue_capacity=1)
    publisher = KafkaPublisher(producer=producer_with_tiny_queue)

    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE001"))
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE002"))
    publisher.flush()

    assert [message.key() for message in producer_with_tiny_queue.delivered] == [
        b"SITE001",
        b"SITE002",
    ]


def test_a_message_that_cannot_be_queued_raises_rather_than_disappearing(
    good_reading_payload: dict[str, Any],
) -> None:
    class AlwaysFullProducer(FakeProducer):
        def produce(self, *args: Any, **kwargs: Any) -> None:
            raise BufferError("Local: Queue full")

    publisher = KafkaPublisher(producer=AlwaysFullProducer())

    with pytest.raises(MessagePublicationError):
        publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))


def test_delivery_failures_are_counted_instead_of_passing_unnoticed(
    good_reading_payload: dict[str, Any],
) -> None:
    failing_producer = FakeProducer(delivery_failure="broker unreachable")
    publisher = KafkaPublisher(producer=failing_producer)

    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))
    publisher.flush()

    assert publisher.delivery_failures == 1
    assert failing_producer.delivered == []


def test_successful_deliveries_are_counted_per_topic(
    publisher: KafkaPublisher,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE001"))
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload, "SITE002"))
    publisher.flush()

    assert publisher.delivered_counts == {MEASURE_TOPIC: 2}
    assert publisher.delivery_failures == 0


def test_flush_reports_the_messages_still_pending(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert publisher.flush() == 0
    assert fake_producer.flush_calls == 1


def test_closing_flushes_before_releasing(
    publisher: KafkaPublisher,
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    # Docker envoie SIGTERM puis tue le conteneur : sans ce vidage, les messages
    # encore en file sont perdus.
    publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    publisher.close()

    assert fake_producer.flush_calls >= 1
    assert len(fake_producer.delivered) == 1


def test_it_can_be_used_as_a_context_manager(
    fake_producer: FakeProducer,
    good_reading_payload: dict[str, Any],
) -> None:
    with KafkaPublisher(producer=fake_producer) as publisher:
        publisher.publish(MEASURE_TOPIC, build_envelope(good_reading_payload))

    assert len(fake_producer.delivered) == 1
