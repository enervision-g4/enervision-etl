from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
import responses

from enervision_etl.extract.errors import (
    InvalidRequestParameterError,
    MockApiUnavailableError,
    SiteNotFoundError,
)
from enervision_etl.extract.http_client import (
    RETRYABLE_STATUS_CODES,
    ResilientHttpClient,
    build_http_session,
)

BASE_URL = "http://192.0.2.10:8000"


@pytest.fixture
def http_client() -> ResilientHttpClient:
    return ResilientHttpClient(base_url=BASE_URL, timeout_seconds=5.0)


def test_retry_policy_targets_only_transient_failures() -> None:
    session = build_http_session()
    retry_policy = session.get_adapter(BASE_URL).max_retries

    assert set(retry_policy.status_forcelist) == set(RETRYABLE_STATUS_CODES)
    assert 404 not in retry_policy.status_forcelist
    assert 422 not in retry_policy.status_forcelist


def test_retry_policy_uses_exponential_backoff() -> None:
    session = build_http_session(total_retries=3, backoff_factor=0.5)
    retry_policy = session.get_adapter(BASE_URL).max_retries

    assert retry_policy.total == 3
    assert retry_policy.backoff_factor == 0.5


def test_every_request_carries_an_explicit_timeout() -> None:
    instrumented_session = MagicMock(spec=requests.Session)
    instrumented_session.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"status": "healthy"}),
    )
    client = ResilientHttpClient(
        base_url=BASE_URL,
        timeout_seconds=2.5,
        session=instrumented_session,
    )

    client.get_json("/health")

    assert instrumented_session.get.call_args.kwargs["timeout"] == 2.5


@responses.activate
def test_successful_json_response_is_returned(
    http_client: ResilientHttpClient,
    good_reading_payload: dict[str, Any],
) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/sites/SITE001/current",
        json=good_reading_payload,
        status=200,
    )

    received_payload = http_client.get_json("/api/v1/sites/SITE001/current")

    assert received_payload["site_id"] == "SITE001"


@responses.activate
def test_response_containing_nulls_is_never_rejected(
    http_client: ResilientHttpClient,
    critical_reading_payload: dict[str, Any],
) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/sites/SITE001/current",
        json=critical_reading_payload,
        status=200,
    )

    received_payload = http_client.get_json("/api/v1/sites/SITE001/current")

    assert received_payload["consumption_kw"] is None
    assert received_payload["null_reasons"] == ["network_loss"]


@responses.activate
def test_not_found_raises_site_not_found(http_client: ResilientHttpClient) -> None:
    responses.get(f"{BASE_URL}/api/v1/sites/SITE999/current", status=404)

    with pytest.raises(SiteNotFoundError) as raised:
        http_client.get_json("/api/v1/sites/SITE999/current", site_id="SITE999")

    assert raised.value.site_id == "SITE999"


@responses.activate
def test_unprocessable_entity_raises_invalid_parameter(
    http_client: ResilientHttpClient,
) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/readings",
        status=422,
        json={"detail": "limit must be between 1 and 1000"},
    )

    with pytest.raises(InvalidRequestParameterError) as raised:
        http_client.get_json("/api/v1/readings", query_parameters={"limit": 5000})

    assert raised.value.endpoint == "/api/v1/readings"


@responses.activate
def test_server_error_raises_unavailable(http_client: ResilientHttpClient) -> None:
    responses.get(f"{BASE_URL}/api/v1/sites", status=503)

    with pytest.raises(MockApiUnavailableError):
        http_client.get_json("/api/v1/sites")


@responses.activate
def test_connection_failure_raises_unavailable(http_client: ResilientHttpClient) -> None:
    responses.get(
        f"{BASE_URL}/api/v1/sites",
        body=requests.ConnectionError("connection refused"),
    )

    with pytest.raises(MockApiUnavailableError):
        http_client.get_json("/api/v1/sites")


@responses.activate
def test_query_parameters_are_forwarded(
    http_client: ResilientHttpClient,
    degraded_series_payload: list[dict[str, Any]],
) -> None:
    responses.get(f"{BASE_URL}/api/v1/readings", json=degraded_series_payload, status=200)

    http_client.get_json(
        "/api/v1/readings",
        query_parameters={"site_id": "SITE002", "limit": 48},
    )

    assert responses.calls[0].request.params == {"site_id": "SITE002", "limit": "48"}


@responses.activate
def test_trailing_slash_in_base_url_does_not_produce_a_double_slash(
    good_reading_payload: dict[str, Any],
) -> None:
    client = ResilientHttpClient(base_url=f"{BASE_URL}/", timeout_seconds=5.0)
    responses.get(f"{BASE_URL}/api/v1/sites/SITE001/current", json=good_reading_payload)

    client.get_json("/api/v1/sites/SITE001/current")

    assert responses.calls[0].request.url.startswith(f"{BASE_URL}/api/v1/")


def test_client_can_be_used_as_a_context_manager() -> None:
    instrumented_session = MagicMock(spec=requests.Session)

    with ResilientHttpClient(BASE_URL, 5.0, session=instrumented_session):
        pass

    instrumented_session.close.assert_called_once()
