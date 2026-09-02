import json
from datetime import datetime, timedelta
from itertools import pairwise
from typing import Any
from urllib.parse import parse_qsl, urlparse

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


def simulate_readings_endpoint(request: Any) -> tuple[int, dict, str]:
    """Reproduit le comportement mesure sur l'instance reelle de l'API mock.

    L'endpoint renvoie limit points repartis uniformement entre start_time et end_time,
    l'intervalle valant (end_time - start_time) / limit.
    """
    parameters = dict(parse_qsl(urlparse(request.url).query))
    limit = int(parameters["limit"])
    window_start = datetime.fromisoformat(parameters["start_time"])
    window_end = datetime.fromisoformat(parameters["end_time"])
    interval = (window_end - window_start) / limit

    payload = [
        {
            "timestamp": (window_start + interval * index).isoformat(),
            "site_id": parameters.get("site_id", "SITE002"),
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
        for index in range(limit)
    ]
    return 200, {}, json.dumps(payload)


def register_simulated_readings_endpoint() -> None:
    responses.add_callback(
        responses.GET,
        f"{BASE_URL}/api/v1/readings",
        callback=simulate_readings_endpoint,
        content_type="application/json",
    )


@responses.activate
def test_limit_is_the_number_of_points_not_a_page_size(api_client: MockApiClient) -> None:
    register_simulated_readings_endpoint()

    api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(hours=1),
        resolution_seconds=60.0,
    )

    sent_parameters = responses.calls[0].request.params
    assert sent_parameters["limit"] == "60"


@responses.activate
def test_a_short_window_is_fetched_in_a_single_call(api_client: MockApiClient) -> None:
    register_simulated_readings_endpoint()

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(hours=1),
        resolution_seconds=60.0,
    )

    assert len(responses.calls) == 1
    assert len(readings) == 60


@responses.activate
def test_the_requested_resolution_is_honoured(api_client: MockApiClient) -> None:
    register_simulated_readings_endpoint()

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(hours=2),
        resolution_seconds=300.0,
    )

    observed_intervals = {
        (later.timestamp - earlier.timestamp).total_seconds()
        for earlier, later in pairwise(readings)
    }
    assert observed_intervals == {300.0}


@responses.activate
def test_a_long_window_is_split_into_contiguous_chunks(api_client: MockApiClient) -> None:
    register_simulated_readings_endpoint()
    resolution_seconds = 60.0
    window_end = WINDOW_START + timedelta(seconds=resolution_seconds * 2500)

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        window_end,
        resolution_seconds=resolution_seconds,
    )

    assert len(responses.calls) == 3
    requested_windows = [
        (call.request.params["start_time"], call.request.params["end_time"])
        for call in responses.calls
    ]
    for earlier, later in pairwise(requested_windows):
        assert earlier[1] == later[0], "tranches jointives : ni trou ni recouvrement"
    assert len(readings) == 2500


@responses.activate
def test_chunks_never_request_more_than_the_documented_maximum(
    api_client: MockApiClient,
) -> None:
    register_simulated_readings_endpoint()

    api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(days=3),
        resolution_seconds=60.0,
    )

    for call in responses.calls:
        assert int(call.request.params["limit"]) <= MAX_READINGS_PER_REQUEST


@responses.activate
def test_duplicated_timestamps_across_chunks_are_collapsed(
    api_client: MockApiClient,
) -> None:
    register_simulated_readings_endpoint()

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(seconds=60.0 * 1500),
        resolution_seconds=60.0,
    )

    timestamps = [reading.timestamp for reading in readings]
    assert len(set(timestamps)) == len(timestamps)
    assert timestamps == sorted(timestamps)


@responses.activate
def test_nulls_returned_by_the_batch_endpoint_are_preserved(
    api_client: MockApiClient,
    degraded_series_payload: list[dict[str, Any]],
) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=degraded_series_payload)

    readings = api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(minutes=2),
        resolution_seconds=60.0,
    )

    assert readings[1].consumption_kw is None
    assert readings[1].null_reasons == [
        "consumption_sensor_failure",
        "electrical_sensor_failure",
    ]


@responses.activate
def test_fetch_readings_window_handles_an_empty_period(api_client: MockApiClient) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=[])

    readings = api_client.fetch_readings_window("SITE002", WINDOW_START, WINDOW_END)

    assert readings == []


@responses.activate
def test_non_positive_resolution_is_rejected_before_reaching_the_api(
    api_client: MockApiClient,
) -> None:
    with pytest.raises(ValueError):
        api_client.fetch_readings_window(
            "SITE002",
            WINDOW_START,
            WINDOW_END,
            resolution_seconds=0,
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

    api_client.fetch_readings_window(
        "SITE002",
        WINDOW_START,
        WINDOW_START + timedelta(hours=1),
        resolution_seconds=60.0,
    )

    sent_parameters = responses.calls[0].request.params
    assert sent_parameters["site_id"] == "SITE002"
    assert sent_parameters["start_time"] == WINDOW_START.isoformat()
    assert sent_parameters["end_time"] == (WINDOW_START + timedelta(hours=1)).isoformat()


@responses.activate
def test_health_check_reports_a_healthy_service(api_client: MockApiClient) -> None:
    responses.get(
        f"{BASE_URL}/health",
        json={"status": "healthy", "timestamp": "2024-06-15T14:32:00.123456"},
    )

    assert api_client.is_healthy() is True
