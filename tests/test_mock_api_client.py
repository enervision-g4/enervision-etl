from datetime import datetime, timedelta
from typing import Any

import pytest
import responses

from enervision_etl.extract.errors import SiteNotFoundError
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import (
    MAX_READINGS_PER_REQUEST,
    MockApiClient,
)

BASE_URL = "http://192.0.2.10:8000"
WINDOW_START = datetime(2024, 6, 14, 0, 0, 0)
WINDOW_END = datetime(2024, 6, 15, 0, 0, 0)


@pytest.fixture
def api_client() -> MockApiClient:
    return MockApiClient(ResilientHttpClient(base_url=BASE_URL, timeout_seconds=5.0))


def build_reading_page(site_id: str, first_timestamp: datetime, page_length: int) -> list[dict]:
    return [
        {
            "timestamp": (first_timestamp + timedelta(minutes=index)).isoformat(),
            "site_id": site_id,
            "site_type": "factory",
            "consumption_kw": 500.0 + index,
            "consumption_kwh": 500.0 + index,
            "voltage_v": 398.5,
            "current_a": 826.4,
            "power_factor": 0.921,
            "temperature_celsius": 18.3,
            "humidity_percent": 62.1,
            "null_reasons": [],
            "data_quality": "good",
        }
        for index in range(page_length)
    ]


@responses.activate
def test_fetch_site_registry_returns_typed_sites(
    api_client: MockApiClient,
    site_registry_payload: list[dict[str, Any]],
) -> None:
    responses.get(f"{BASE_URL}/api/v1/sites", json=site_registry_payload)

    sites = api_client.fetch_site_registry()

    assert [site.site_id for site in sites] == ["SITE001", "SITE002", "SITE003"]
    assert sites[1].capacity_kw == 1000


@responses.activate
def test_fetch_current_reading_preserves_a_partial_measurement(
    api_client: MockApiClient,
    partial_reading_payload: dict[str, Any],
) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/sites/SITE001/current",
        json=partial_reading_payload,
    )

    reading = api_client.fetch_current_reading("SITE001")

    assert reading.temperature_celsius is None
    assert reading.consumption_kw == 91.20
    assert reading.data_quality == "partial"


@responses.activate
def test_fetch_current_reading_preserves_a_total_network_loss(
    api_client: MockApiClient,
    critical_reading_payload: dict[str, Any],
) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/sites/SITE001/current",
        json=critical_reading_payload,
    )

    reading = api_client.fetch_current_reading("SITE001")

    assert reading.missing_measurement_fields() == (
        "consumption_kw",
        "consumption_kwh",
        "voltage_v",
        "current_a",
        "power_factor",
        "temperature_celsius",
        "humidity_percent",
    )


@responses.activate
def test_fetch_current_reading_on_unknown_site_raises(api_client: MockApiClient) -> None:
    responses.get(f"{BASE_URL}/api/v1/sites/SITE999/current", status=404)

    with pytest.raises(SiteNotFoundError) as raised:
        api_client.fetch_current_reading("SITE999")

    assert raised.value.site_id == "SITE999"


@responses.activate
def test_fetch_readings_window_returns_a_single_short_page(
    api_client: MockApiClient,
    degraded_series_payload: list[dict[str, Any]],
) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=degraded_series_payload)

    readings = api_client.fetch_readings_window("SITE002", WINDOW_START, WINDOW_END)

    assert len(readings) == 2
    assert readings[1].consumption_kw is None
    assert len(responses.calls) == 1


@responses.activate
def test_fetch_readings_window_paginates_until_a_short_page(
    api_client: MockApiClient,
) -> None:
    page_size = 3
    responses.get(
        f"{BASE_URL}/api/v1/readings",
        json=build_reading_page("SITE002", WINDOW_START, page_size),
    )
    responses.get(
        f"{BASE_URL}/api/v1/readings",
        json=build_reading_page("SITE002", WINDOW_START + timedelta(minutes=3), 1),
    )

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_END,
        page_size=page_size,
    )

    assert len(readings) == 4
    assert len(responses.calls) == 2
    assert readings[0].timestamp < readings[-1].timestamp


@responses.activate
def test_fetch_readings_window_stops_when_the_api_repeats_a_page(
    api_client: MockApiClient,
) -> None:
    stuck_page = build_reading_page("SITE002", WINDOW_START, 2)
    for _ in range(5):
        responses.get(f"{BASE_URL}/api/v1/readings", json=stuck_page)

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_END,
        page_size=2,
    )

    assert len(readings) == 2
    assert len(responses.calls) <= 2


@responses.activate
def test_fetch_readings_window_handles_an_empty_period(api_client: MockApiClient) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=[])

    readings = api_client.fetch_readings_window("SITE002", WINDOW_START, WINDOW_END)

    assert readings == []


@responses.activate
def test_oversized_page_is_rejected_before_reaching_the_api(
    api_client: MockApiClient,
) -> None:
    with pytest.raises(ValueError):
        api_client.fetch_readings_window(
            "SITE002",
            WINDOW_START,
            WINDOW_END,
            page_size=MAX_READINGS_PER_REQUEST + 1,
        )

    assert len(responses.calls) == 0


@responses.activate
def test_inverted_period_is_rejected_before_reaching_the_api(
    api_client: MockApiClient,
) -> None:
    with pytest.raises(ValueError):
        api_client.fetch_readings_window("SITE002", WINDOW_END, WINDOW_START)

    assert len(responses.calls) == 0


@responses.activate
def test_period_bounds_are_sent_as_iso_timestamps(
    api_client: MockApiClient,
    degraded_series_payload: list[dict[str, Any]],
) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=degraded_series_payload)

    api_client.fetch_readings_window("SITE002", WINDOW_START, WINDOW_END, page_size=100)

    sent_parameters = responses.calls[0].request.params
    assert sent_parameters["site_id"] == "SITE002"
    assert sent_parameters["start_time"] == WINDOW_START.isoformat()
    assert sent_parameters["end_time"] == WINDOW_END.isoformat()
    assert sent_parameters["limit"] == "100"


@responses.activate
def test_health_check_reports_a_healthy_service(api_client: MockApiClient) -> None:
    responses.get(
        f"{BASE_URL}/health",
        json={"status": "healthy", "timestamp": "2024-06-15T14:32:00.123456"},
    )

    assert api_client.is_healthy() is True
