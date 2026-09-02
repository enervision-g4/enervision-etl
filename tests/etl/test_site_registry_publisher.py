import io
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from enervision_contracts.site import Site
from enervision_etl.load.site_registry_publisher import SiteRegistryPublisher
from enervision_etl.load.stdout_publisher import StdoutPublisher

SITE_TOPIC = "enervision.site"
REFRESH_INTERVAL_SECONDS = 3600.0
STARTED_AT = datetime(2026, 9, 2, 10, 0, 0, tzinfo=UTC)


class FrozenClock:
    """Horloge pilotee par le test, pour eprouver l'intervalle sans attendre."""

    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def __call__(self) -> datetime:
        return self.instant

    def advance(self, seconds: float) -> None:
        self.instant += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(STARTED_AT)


@pytest.fixture
def captured_output() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def registry_publisher(
    captured_output: io.StringIO,
    clock: FrozenClock,
) -> SiteRegistryPublisher:
    return SiteRegistryPublisher(
        publisher=StdoutPublisher(stream=captured_output),
        topic=SITE_TOPIC,
        refresh_interval_seconds=REFRESH_INTERVAL_SECONDS,
        clock=clock,
    )


@pytest.fixture
def registry(site_registry_payload: list[dict[str, Any]]) -> list[Site]:
    return [Site.model_validate(payload) for payload in site_registry_payload]


def published_keys(captured_output: io.StringIO) -> list[str]:
    return [
        json.loads(line)["key"]
        for line in captured_output.getvalue().splitlines()
        if line.strip()
    ]


def test_the_first_publication_sends_the_whole_registry(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    captured_output: io.StringIO,
) -> None:
    published = registry_publisher.publish_changes(registry)

    assert published == ["SITE001", "SITE002", "SITE003"]
    assert published_keys(captured_output) == ["SITE001", "SITE002", "SITE003"]


def test_an_unchanged_registry_sends_nothing(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    captured_output: io.StringIO,
) -> None:
    # En regime stable, le trafic doit tomber a zero : un referentiel bouge tous les ans.
    registry_publisher.publish_changes(registry)
    captured_output.truncate(0)
    captured_output.seek(0)

    published = registry_publisher.publish_changes(registry)

    assert published == []
    assert captured_output.getvalue() == ""


def test_only_the_modified_site_is_republished(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    captured_output: io.StringIO,
) -> None:
    registry_publisher.publish_changes(registry)
    captured_output.truncate(0)
    captured_output.seek(0)
    upgraded_registry = [
        site.model_copy(update={"capacity_kw": 1200.0}) if site.site_id == "SITE002" else site
        for site in registry
    ]

    published = registry_publisher.publish_changes(upgraded_registry)

    assert published == ["SITE002"]
    assert published_keys(captured_output) == ["SITE002"]


def test_a_status_change_is_detected(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
) -> None:
    # Un site mis hors service change de statut plutot que de disparaitre du referentiel.
    registry_publisher.publish_changes(registry)
    decommissioned = [
        site.model_copy(update={"status": "inactive"}) if site.site_id == "SITE003" else site
        for site in registry
    ]

    assert registry_publisher.publish_changes(decommissioned) == ["SITE003"]


def test_a_newly_exposed_site_is_published(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
) -> None:
    registry_publisher.publish_changes(registry)
    newcomer = registry[0].model_copy(update={"site_id": "SITE008", "site_name": "Entrepot"})

    assert registry_publisher.publish_changes([*registry, newcomer]) == ["SITE008"]


def test_a_site_that_disappears_is_forgotten_and_republished_if_it_returns(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
) -> None:
    registry_publisher.publish_changes(registry)
    registry_publisher.publish_changes(registry[:2])

    assert registry_publisher.publish_changes(registry) == ["SITE003"]


def test_an_empty_registry_publishes_nothing(
    registry_publisher: SiteRegistryPublisher,
    captured_output: io.StringIO,
) -> None:
    assert registry_publisher.publish_changes([]) == []
    assert captured_output.getvalue() == ""


def test_a_refresh_is_due_before_any_publication(
    registry_publisher: SiteRegistryPublisher,
) -> None:
    assert registry_publisher.is_refresh_due() is True


def test_no_refresh_is_due_right_after_a_publication(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
) -> None:
    registry_publisher.publish_changes(registry)

    assert registry_publisher.is_refresh_due() is False


def test_a_refresh_becomes_due_once_the_interval_has_elapsed(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    clock: FrozenClock,
) -> None:
    registry_publisher.publish_changes(registry)

    clock.advance(REFRESH_INTERVAL_SECONDS - 1)
    assert registry_publisher.is_refresh_due() is False

    clock.advance(2)
    assert registry_publisher.is_refresh_due() is True


def test_an_unchanged_registry_still_postpones_the_next_refresh(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    clock: FrozenClock,
) -> None:
    # Sans cela, un referentiel stable serait reinterroge a chaque cycle du collecteur.
    registry_publisher.publish_changes(registry)
    clock.advance(REFRESH_INTERVAL_SECONDS + 1)
    registry_publisher.publish_changes(registry)

    assert registry_publisher.is_refresh_due() is False


def test_the_configured_topic_is_used(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    captured_output: io.StringIO,
) -> None:
    registry_publisher.publish_changes(registry)

    topics = {
        json.loads(line)["topic"]
        for line in captured_output.getvalue().splitlines()
        if line.strip()
    }
    assert topics == {SITE_TOPIC}


def test_the_published_message_carries_the_site_characteristics(
    registry_publisher: SiteRegistryPublisher,
    registry: list[Site],
    captured_output: io.StringIO,
) -> None:
    registry_publisher.publish_changes(registry)

    first_message = json.loads(captured_output.getvalue().splitlines()[0])

    assert first_message["value"]["event_type"] == "site"
    assert first_message["value"]["payload"]["capacity_kw"] == 200.0
    assert first_message["value"]["collection_mode"] is None


@pytest.mark.parametrize("invalid_interval", [0, -1])
def test_a_non_positive_refresh_interval_is_rejected(
    captured_output: io.StringIO,
    invalid_interval: float,
) -> None:
    with pytest.raises(ValueError):
        SiteRegistryPublisher(
            publisher=StdoutPublisher(stream=captured_output),
            topic=SITE_TOPIC,
            refresh_interval_seconds=invalid_interval,
        )
