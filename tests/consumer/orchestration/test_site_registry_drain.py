from datetime import UTC, datetime
from typing import Any

from enervision_consumer.orchestration.site_registry_drain import SiteRegistryDrain
from enervision_contracts.envelope import EventType, MessageEnvelope, SitePayload

from .conftest import FakeConsumerMessage

SITE_TOPIC = "enervision.site"
PUBLISHED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)


def site_message(site_id: str) -> FakeConsumerMessage:
    envelope = MessageEnvelope[SitePayload](
        event_type=EventType.SITE,
        produced_at=PUBLISHED_AT,
        payload=SitePayload(
            site_id=site_id,
            site_type="factory",
            site_name=f"Site {site_id}",
            location="France",
            capacity_kw=1000,
            status="active",
        ),
    )
    return FakeConsumerMessage(SITE_TOPIC, envelope.model_dump_json().encode("utf-8"))


def build_drain(kafka: Any, connection: Any) -> SiteRegistryDrain:
    return SiteRegistryDrain(
        open_consumer=lambda: kafka,
        connection=connection,
        site_topic=SITE_TOPIC,
        poll_timeout_seconds=0.0,
        silent_polls_before_end=2,
    )


def test_every_published_site_is_applied(consumer: Any, connection: Any) -> None:
    kafka = consumer([site_message("SITE001"), site_message("SITE002")])

    applied = build_drain(kafka, connection)()

    assert applied == 2
    assert connection.opened_cursor.parameters[0][0] == "SITE001"
    assert connection.opened_cursor.parameters[1][0] == "SITE002"


def test_the_registry_is_committed_once_drained(consumer: Any, connection: Any) -> None:
    kafka = consumer([site_message("SITE001")])

    build_drain(kafka, connection)()

    assert connection.commits == 1


def test_the_drain_never_acquits_its_offsets(consumer: Any, connection: Any) -> None:
    # C'est ce qui lui fait relire le topic compacte depuis son debut a chaque appel,
    # et donc reconstruire l'etat courant du parc plutot qu'un delta.
    kafka = consumer([site_message("SITE001")])

    build_drain(kafka, connection)()

    assert kafka.committed == []


def test_the_dedicated_consumer_is_released(consumer: Any, connection: Any) -> None:
    kafka = consumer([site_message("SITE001")])

    build_drain(kafka, connection)()

    assert kafka.closed is True


def test_an_empty_registry_is_not_an_error(consumer: Any, connection: Any) -> None:
    kafka = consumer([])

    assert build_drain(kafka, connection)() == 0


def test_a_group_still_joining_does_not_end_the_drain(consumer: Any, connection: Any) -> None:
    # Constate sur un vrai broker : un groupe neuf ne recoit rien pendant qu'il rejoint
    # et se voit attribuer sa partition. Compter ce silence comme une fin de topic
    # faisait rendre un referentiel vide, alors qu'il y avait sept sites a lire.
    kafka = consumer(
        [site_message("SITE001"), site_message("SITE002")],
        None,
        4,
    )

    assert build_drain(kafka, connection)() == 2


def test_a_broker_error_event_does_not_end_the_drain(consumer: Any, connection: Any) -> None:
    # Meme piege que dans la boucle : l'evenement d'erreur ne porte pas de JSON, et le
    # drainage ne doit ni le decoder ni le prendre pour la fin du topic.
    erreur = FakeConsumerMessage(
        SITE_TOPIC,
        b"Subscribed topic not available: unknown topic or partition",
        error="UNKNOWN_TOPIC_OR_PART",
    )
    kafka = consumer([erreur, site_message("SITE001")])

    assert build_drain(kafka, connection)() == 1
