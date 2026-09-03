from datetime import UTC, datetime, timedelta
from typing import Any, Optional

import pytest

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import MessageEnvelope
from enervision_contracts.site import Site
from enervision_etl.extract.errors import MockApiUnavailableError, SiteNotFoundError
from enervision_etl.orchestration.realtime_collector import RealtimeCollector

MEASURE_TOPIC = "enervision.measure_raw"
IMPUTED_TOPIC = "enervision.measure_imputed"
SITE_TOPIC = "enervision.site"


class RecordingPublisher:
    """Destination en memoire, pour observer ce que le collecteur publie."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, MessageEnvelope[Any]]] = []
        self.flush_calls = 0
        self.closed = False

    def publish(self, topic: str, envelope: MessageEnvelope[Any]) -> None:
        self.messages.append((topic, envelope))

    def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_calls += 1
        return 0

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "RecordingPublisher":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def topics(self) -> list[str]:
        return [topic for topic, _ in self.messages]

    def payloads_on(self, topic: str) -> list[Any]:
        return [envelope.payload for published, envelope in self.messages if published == topic]


class ScriptedApiClient:
    """Client d'API pilote par le test, y compris dans ses pannes."""

    def __init__(self, registry: list[Site], readings_by_site: dict[str, list[Any]]) -> None:
        self._registry = registry
        self._readings_by_site = {site_id: list(v) for site_id, v in readings_by_site.items()}
        self.registry_calls = 0
        self.current_calls: list[str] = []

    def fetch_site_registry(self) -> list[Site]:
        self.registry_calls += 1
        return self._registry

    def fetch_current_reading(self, site_id: str) -> EnergyReading:
        self.current_calls.append(site_id)
        scripted = self._readings_by_site[site_id]
        if not scripted:
            raise MockApiUnavailableError(f"/api/v1/sites/{site_id}/current")
        outcome = scripted.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def build_reading(
    site_id: str,
    minute: int,
    consumption_kw: Optional[float],
    quality: str = "good",
) -> EnergyReading:
    return EnergyReading(
        timestamp=datetime(2026, 9, 2, 10, minute, tzinfo=UTC),
        site_id=site_id,
        site_type="factory",
        consumption_kw=consumption_kw,
        consumption_kwh=consumption_kw,
        voltage_v=400.0 if consumption_kw is not None else None,
        current_a=800.0,
        power_factor=0.92,
        temperature_celsius=18.0,
        humidity_percent=60.0,
        null_reasons=[] if consumption_kw is not None else ["consumption_sensor_failure"],
        data_quality=quality if consumption_kw is not None else "degraded",
    )


@pytest.fixture
def registry(site_registry_payload: list[dict[str, Any]]) -> list[Site]:
    return [Site.model_validate(payload) for payload in site_registry_payload[:2]]


@pytest.fixture
def publisher() -> RecordingPublisher:
    return RecordingPublisher()


def build_collector(
    api_client: ScriptedApiClient,
    publisher: RecordingPublisher,
    **overrides: Any,
) -> RealtimeCollector:
    parameters: dict[str, Any] = {
        "api_client": api_client,
        "publisher": publisher,
        "site_topic": SITE_TOPIC,
        "measure_raw_topic": MEASURE_TOPIC,
        "measure_imputed_topic": IMPUTED_TOPIC,
        "source_timezone": "UTC",
        "max_gap_measures": 3,
        "site_refresh_interval_seconds": 3600.0,
    }
    parameters.update(overrides)
    return RealtimeCollector(**parameters)


def test_the_first_cycle_publishes_the_site_registry(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher).run_cycle()

    assert publisher.topics().count(SITE_TOPIC) == 2


def test_each_configured_site_is_polled_once_per_cycle(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher).run_cycle()

    assert api_client.current_calls == ["SITE001", "SITE002"]


def test_every_measurement_is_published_raw(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher).run_cycle()

    raw_payloads = publisher.payloads_on(MEASURE_TOPIC)
    assert [payload.site_id for payload in raw_payloads] == ["SITE001", "SITE002"]
    assert [payload.consumption_kw for payload in raw_payloads] == [100.0, 500.0]


def test_a_measurement_with_nulls_is_published_untouched(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, None)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher).run_cycle()

    degraded = publisher.payloads_on(MEASURE_TOPIC)[0]
    assert degraded.consumption_kw is None
    assert degraded.null_reasons == ["consumption_sensor_failure"]


def test_a_site_failure_does_not_interrupt_the_others(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [SiteNotFoundError("SITE001")],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    report = build_collector(api_client, publisher).run_cycle()

    assert report.failed_sites == ["SITE001"]
    assert [payload.site_id for payload in publisher.payloads_on(MEASURE_TOPIC)] == ["SITE002"]


def test_the_cycle_reports_the_quality_distribution(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, None)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    report = build_collector(api_client, publisher).run_cycle()

    assert report.readings_by_quality == {"degraded": 1, "good": 1}
    assert report.null_reasons_counts == {"consumption_sensor_failure": 1}


def test_a_gap_is_filled_from_the_previous_cycles(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # En temps reel la mesure suivante est inconnue : seule la recopie est possible,
    # a partir des mesures gardees en memoire par le collecteur.
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", 0, 100.0), build_reading("SITE001", 1, None)],
            "SITE002": [build_reading("SITE002", 0, 500.0), build_reading("SITE002", 1, 510.0)],
        },
    )
    collector = build_collector(api_client, publisher)

    collector.run_cycle()
    collector.run_cycle()

    imputed = [p for p in publisher.payloads_on(IMPUTED_TOPIC) if p.site_id == "SITE001"]
    assert imputed[-1].consumption_kw == 100.0
    assert imputed[-1].imputation_method == "forward_fill"


def test_an_unfillable_gap_stays_null_in_the_imputed_stream(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, None)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher).run_cycle()

    imputed = [p for p in publisher.payloads_on(IMPUTED_TOPIC) if p.site_id == "SITE001"]
    assert imputed[0].consumption_kw is None
    assert imputed[0].imputation_method == "none"


def test_the_registry_is_not_refetched_before_its_interval(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", m, 100.0) for m in range(3)],
            "SITE002": [build_reading("SITE002", m, 500.0) for m in range(3)],
        },
    )
    collector = build_collector(api_client, publisher)

    for _ in range(3):
        collector.run_cycle()

    assert api_client.registry_calls == 1


def test_an_unchanged_registry_is_published_only_once(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", m, 100.0) for m in range(3)],
            "SITE002": [build_reading("SITE002", m, 500.0) for m in range(3)],
        },
    )
    collector = build_collector(api_client, publisher, site_refresh_interval_seconds=0.001)

    for _ in range(3):
        collector.run_cycle()

    assert publisher.topics().count(SITE_TOPIC) == 2


def test_the_configured_restriction_limits_the_polled_sites(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(registry, {"SITE002": [build_reading("SITE002", 0, 500.0)]})

    build_collector(api_client, publisher, configured_sites=["SITE002"]).run_cycle()

    assert api_client.current_calls == ["SITE002"]


def test_running_several_cycles_stops_at_the_requested_count(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", m, 100.0) for m in range(2)],
            "SITE002": [build_reading("SITE002", m, 500.0) for m in range(2)],
        },
    )
    collector = build_collector(api_client, publisher)

    reports = collector.run(max_cycles=2, interval_seconds=0.001)

    assert len(reports) == 2


def test_the_cycle_duration_is_measured(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    report = build_collector(api_client, publisher).run_cycle()

    assert report.duration_seconds >= 0.0


def test_timestamps_are_normalized_to_utc(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    naive_reading = build_reading("SITE001", 0, 100.0).model_copy(
        update={"timestamp": datetime(2026, 9, 2, 12, 0)}
    )
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [naive_reading], "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )

    build_collector(api_client, publisher, source_timezone="Europe/Paris").run_cycle()

    published = publisher.payloads_on(MEASURE_TOPIC)[0]
    assert published.timestamp == datetime(2026, 9, 2, 10, 0, tzinfo=UTC)
    assert published.timestamp.utcoffset() == timedelta(0)


def test_the_loop_stops_when_a_shutdown_is_requested(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", m, 100.0) for m in range(5)],
            "SITE002": [build_reading("SITE002", m, 500.0) for m in range(5)],
        },
    )
    collector = build_collector(api_client, publisher)
    cycles_before_stop = 2
    executed = 0

    def should_stop() -> bool:
        nonlocal executed
        executed += 1
        return executed > cycles_before_stop

    reports = collector.run(interval_seconds=0.001, should_stop=should_stop)

    assert len(reports) == cycles_before_stop


def test_a_started_cycle_always_completes_before_stopping(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # Interrompre au milieu publierait une photo partielle du parc.
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )
    collector = build_collector(api_client, publisher)
    already_asked = False

    def should_stop() -> bool:
        nonlocal already_asked
        if not already_asked:
            already_asked = True
            return False
        return True

    collector.run(interval_seconds=0.001, should_stop=should_stop)

    assert len(publisher.payloads_on(MEASURE_TOPIC)) == 2
