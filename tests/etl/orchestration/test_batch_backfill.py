from datetime import UTC, datetime, timedelta
from typing import Any, Optional

import pytest

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import MessageEnvelope
from enervision_contracts.site import Site
from enervision_etl.extract.errors import SiteNotFoundError
from enervision_etl.orchestration.batch_backfill import BatchBackfill

MEASURE_TOPIC = "enervision.measure_raw"
IMPUTED_TOPIC = "enervision.measure_imputed"
WINDOW_START = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 9, 2, 9, 0, tzinfo=UTC)


class RecordingPublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, MessageEnvelope[Any]]] = []

    def publish(self, topic: str, envelope: MessageEnvelope[Any]) -> None:
        self.messages.append((topic, envelope))

    def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0

    def close(self) -> None:
        return None

    def __enter__(self) -> "RecordingPublisher":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def payloads_on(self, topic: str) -> list[Any]:
        return [envelope.payload for published, envelope in self.messages if published == topic]


class ScriptedApiClient:
    def __init__(self, registry: list[Site], windows: dict[str, Any]) -> None:
        self._registry = registry
        self._windows = windows
        self.window_calls: list[tuple[str, float]] = []

    def fetch_site_registry(self) -> list[Site]:
        return self._registry

    def fetch_readings_window(
        self,
        site_id: Optional[str],
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: float = 60.0,
    ) -> list[EnergyReading]:
        self.window_calls.append((site_id or "", resolution_seconds))
        outcome = self._windows[site_id or ""]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def build_series(site_id: str, consumptions: list[Optional[float]]) -> list[EnergyReading]:
    return [
        EnergyReading(
            timestamp=WINDOW_START + timedelta(minutes=index),
            site_id=site_id,
            site_type="factory",
            consumption_kw=value,
            consumption_kwh=value,
            voltage_v=400.0 if value is not None else None,
            current_a=800.0 if value is not None else None,
            power_factor=0.92 if value is not None else None,
            temperature_celsius=18.0,
            humidity_percent=60.0,
            null_reasons=[] if value is not None else ["network_loss"],
            data_quality="good" if value is not None else "critical",
        )
        for index, value in enumerate(consumptions)
    ]


@pytest.fixture
def registry(site_registry_payload: list[dict[str, Any]]) -> list[Site]:
    return [Site.model_validate(payload) for payload in site_registry_payload[:2]]


@pytest.fixture
def publisher() -> RecordingPublisher:
    return RecordingPublisher()


def build_backfill(
    api_client: ScriptedApiClient,
    publisher: RecordingPublisher,
    **overrides: Any,
) -> BatchBackfill:
    parameters: dict[str, Any] = {
        "api_client": api_client,
        "publisher": publisher,
        "measure_raw_topic": MEASURE_TOPIC,
        "measure_imputed_topic": IMPUTED_TOPIC,
        "source_timezone": "UTC",
        "max_gap_measures": 3,
    }
    parameters.update(overrides)
    return BatchBackfill(**parameters)


def test_a_healthy_window_is_published_raw_and_imputed(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, 110.0, 120.0])}
    )

    report = build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert len(publisher.payloads_on(MEASURE_TOPIC)) == 3
    assert len(publisher.payloads_on(IMPUTED_TOPIC)) == 3
    assert report.published_measures == 3


def test_an_inner_gap_is_interpolated_thanks_to_the_known_future(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # En batch la serie entiere est connue : l'interpolation devient possible,
    # contrairement au temps reel.
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, None, 300.0])}
    )

    build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    imputed = publisher.payloads_on(IMPUTED_TOPIC)
    assert imputed[1].consumption_kw == pytest.approx(200.0)
    assert imputed[1].imputation_method == "linear_interpolation"


def test_the_raw_stream_keeps_the_gap_untouched(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, None, 300.0])}
    )

    build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert publisher.payloads_on(MEASURE_TOPIC)[1].consumption_kw is None


def test_a_degenerate_window_is_refused(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # Une serie integralement nulle n'est pas un historique : c'est l'etat d'une panne
    # au moment de l'appel, projete par le simulateur sur toute la periode demandee.
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [None] * 10)}
    )

    report = build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert report.refused_as_degenerate is True
    assert report.published_measures == 0
    assert publisher.messages == []


def test_a_degenerate_window_can_be_published_on_purpose(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [None] * 10)}
    )

    report = build_backfill(api_client, publisher, publish_degenerate_windows=True).run(
        "SITE002", WINDOW_START, WINDOW_END
    )

    assert report.refused_as_degenerate is True
    assert report.published_measures == 10


def test_a_window_below_the_degeneracy_threshold_is_accepted(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0] * 6 + [None] * 4)}
    )

    report = build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert report.refused_as_degenerate is False
    assert report.published_measures == 10


def test_the_null_ratio_is_reported(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, None, 300.0, None])}
    )

    report = build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert report.null_ratio == pytest.approx(0.5)


def test_an_empty_window_is_reported_without_failing(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(registry, {"SITE002": []})

    report = build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert report.collected_measures == 0
    assert report.published_measures == 0


def test_an_unknown_site_propagates_the_error(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    # Contrairement au temps reel, un backfill vise un site precis : echouer est correct.
    api_client = ScriptedApiClient(registry, {"SITE999": SiteNotFoundError("SITE999")})

    with pytest.raises(SiteNotFoundError):
        build_backfill(api_client, publisher).run("SITE999", WINDOW_START, WINDOW_END)


def test_the_requested_resolution_is_forwarded(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, 110.0])}
    )

    build_backfill(api_client, publisher).run(
        "SITE002", WINDOW_START, WINDOW_END, resolution_seconds=300.0
    )

    assert api_client.window_calls == [("SITE002", 300.0)]


def test_the_batch_collection_mode_is_declared(
    registry: list[Site],
    publisher: RecordingPublisher,
) -> None:
    api_client = ScriptedApiClient(
        registry, {"SITE002": build_series("SITE002", [100.0, 110.0])}
    )

    build_backfill(api_client, publisher).run("SITE002", WINDOW_START, WINDOW_END)

    assert all(envelope.collection_mode == "batch" for _, envelope in publisher.messages)
