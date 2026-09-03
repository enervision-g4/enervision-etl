from datetime import UTC, datetime, timedelta
from typing import Any, Optional

import pytest

from enervision_contracts.alert import Alert
from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import MessageEnvelope
from enervision_contracts.site import Site
from enervision_etl.extract.errors import MockApiUnavailableError, SiteNotFoundError
from enervision_etl.orchestration.realtime_collector import RealtimeCollector

MEASURE_TOPIC = "enervision.measure_raw"
IMPUTED_TOPIC = "enervision.measure_imputed"
SITE_TOPIC = "enervision.site"
ALERT_TOPIC = "enervision.alert"


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

    def __init__(
        self,
        registry: list[Site],
        readings_by_site: dict[str, list[Any]],
        alerts: Optional[list[Alert]] = None,
    ) -> None:
        self._registry = registry
        self._readings_by_site = {site_id: list(v) for site_id, v in readings_by_site.items()}
        self._alerts = list(alerts) if alerts is not None else []
        self.registry_calls = 0
        self.current_calls: list[str] = []
        self.alert_calls = 0

    def fetch_active_alerts(self) -> list[Alert]:
        self.alert_calls += 1
        return list(self._alerts)

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
        "alert_topic": ALERT_TOPIC,
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
    sites_per_cycle = 2
    wanted_cycles = 2

    reports = collector.run(
        interval_seconds=0.001,
        should_stop=lambda: len(api_client.current_calls) >= sites_per_cycle * wanted_cycles,
    )

    assert len(reports) == wanted_cycles


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

    collector.run(
        interval_seconds=0.001,
        should_stop=lambda: len(api_client.current_calls) >= 1,
    )

    assert len(publisher.payloads_on(MEASURE_TOPIC)) == 2


def test_a_shutdown_during_the_wait_prevents_one_more_cycle(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # Un signal recu pendant l'attente doit sortir sans lancer de cycle supplementaire,
    # sinon docker envoie SIGKILL avant que le processus n'ait reagi.
    api_client = ScriptedApiClient(
        registry,
        {
            "SITE001": [build_reading("SITE001", m, 100.0) for m in range(5)],
            "SITE002": [build_reading("SITE002", m, 500.0) for m in range(5)],
        },
    )
    collector = build_collector(api_client, publisher)
    shutdown_after_first_cycle = False

    def should_stop() -> bool:
        return shutdown_after_first_cycle or len(api_client.current_calls) >= 2

    reports = collector.run(interval_seconds=5.0, should_stop=should_stop)

    assert len(reports) == 1


def test_an_unreachable_cadence_is_reported_at_startup(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # Avec 2 sites espaces de 40 s, un cycle dure 80 s : il ne peut pas tenir dans 60 s.
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )
    collector = build_collector(api_client, publisher)
    collector.run_cycle()

    infeasible = collector.cadence_shortfall_seconds(
        poll_interval_seconds=60.0,
        minimum_request_interval_seconds=40.0,
    )

    assert infeasible == pytest.approx(20.0)


def test_a_reachable_cadence_reports_no_shortfall(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        {"SITE001": [build_reading("SITE001", 0, 100.0)],
         "SITE002": [build_reading("SITE002", 0, 500.0)]},
    )
    collector = build_collector(api_client, publisher)
    collector.run_cycle()

    assert collector.cadence_shortfall_seconds(60.0, 2.0) == 0.0


def test_the_cadence_cannot_be_judged_before_the_park_is_known(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(registry, {})
    collector = build_collector(api_client, publisher)

    assert collector.cadence_shortfall_seconds(60.0, 40.0) == 0.0


def build_alert(site_id: str, minute: int, severity: str = "critical") -> Alert:
    return Alert(
        alert_id=f"ALR-{site_id}-{minute}",
        timestamp=datetime(2026, 9, 2, 10, minute),
        site_id=site_id,
        severity=severity,
        type="outage",
        message="Risque de surcharge",
        value=812.5,
        threshold=720.0,
    )


def nominal_readings() -> dict[str, list[Any]]:
    return {
        "SITE001": [build_reading("SITE001", 0, 100.0)],
        "SITE002": [build_reading("SITE002", 0, 500.0)],
    }


def test_a_cycle_publishes_the_active_alerts(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, nominal_readings(), alerts=[build_alert("SITE002", 12)]
    )

    build_collector(api_client, publisher).run_cycle()

    published_alerts = publisher.payloads_on(ALERT_TOPIC)
    assert len(published_alerts) == 1
    assert published_alerts[0].site_id == "SITE002"
    assert published_alerts[0].source_alert_id == "ALR-SITE002-12"
    assert published_alerts[0].value_kw == 812.5
    # L'API date ses alertes sans fuseau : le collecteur les situe avant publication.
    assert published_alerts[0].timestamp.tzinfo == UTC


def test_alerts_are_fetched_once_per_cycle_not_once_per_site(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, nominal_readings(), alerts=[build_alert("SITE001", 5)]
    )

    build_collector(api_client, publisher).run_cycle()

    assert api_client.alert_calls == 1
    assert len(api_client.current_calls) == 2


def test_a_park_without_alert_publishes_nothing_on_the_alert_topic(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(registry, nominal_readings(), alerts=[])

    build_collector(api_client, publisher).run_cycle()

    assert publisher.payloads_on(ALERT_TOPIC) == []


def test_the_cycle_report_counts_the_published_alerts(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry,
        nominal_readings(),
        alerts=[build_alert("SITE001", 5), build_alert("SITE002", 12)],
    )

    report = build_collector(api_client, publisher).run_cycle()

    assert report.published_alert_count == 2
