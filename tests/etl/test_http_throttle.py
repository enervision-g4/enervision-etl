from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from enervision_etl.extract.http_client import ResilientHttpClient

BASE_URL = "http://192.0.2.10:8000"


class ControlledClock:
    """Horloge et sommeil pilotes par le test."""

    def __init__(self) -> None:
        self.elapsed = 0.0
        self.sleep_durations: list[float] = []

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, duration: float) -> None:
        self.sleep_durations.append(duration)
        self.elapsed += duration

    def spend(self, duration: float) -> None:
        self.elapsed += duration


@pytest.fixture
def clock() -> ControlledClock:
    return ControlledClock()


@pytest.fixture
def answering_session() -> Any:
    session = MagicMock(spec=requests.Session)
    session.get.return_value = MagicMock(status_code=200, json=MagicMock(return_value={}))
    return session


def build_client(
    answering_session: Any,
    clock: ControlledClock,
    minimum_interval_seconds: float,
) -> ResilientHttpClient:
    return ResilientHttpClient(
        base_url=BASE_URL,
        timeout_seconds=5.0,
        session=answering_session,
        minimum_interval_seconds=minimum_interval_seconds,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


def test_without_throttling_requests_follow_each_other_immediately(
    answering_session: Any,
    clock: ControlledClock,
) -> None:
    client = build_client(answering_session, clock, minimum_interval_seconds=0.0)

    for _ in range(5):
        client.get_json("/health")

    assert clock.sleep_durations == []


def test_the_first_request_is_never_delayed(
    answering_session: Any,
    clock: ControlledClock,
) -> None:
    client = build_client(answering_session, clock, minimum_interval_seconds=0.2)

    client.get_json("/health")

    assert clock.sleep_durations == []


def test_consecutive_requests_are_spaced_by_the_configured_interval(
    answering_session: Any,
    clock: ControlledClock,
) -> None:
    # Une rafale de requetes met l'API mock en defaut : elle renvoie alors des series
    # entierement nulles, qu'on prendrait a tort pour des pannes de capteurs.
    client = build_client(answering_session, clock, minimum_interval_seconds=0.2)

    client.get_json("/health")
    client.get_json("/health")
    client.get_json("/health")

    assert clock.sleep_durations == [0.2, 0.2]


def test_a_slow_request_consumes_the_interval(
    answering_session: Any,
    clock: ControlledClock,
) -> None:
    # Si la requete precedente a deja pris plus que l'intervalle, aucune attente n'est
    # necessaire : le but est d'espacer, pas de ralentir systematiquement.
    client = build_client(answering_session, clock, minimum_interval_seconds=0.2)

    client.get_json("/health")
    clock.spend(0.5)
    client.get_json("/health")

    assert clock.sleep_durations == []


def test_a_partially_elapsed_interval_is_completed(
    answering_session: Any,
    clock: ControlledClock,
) -> None:
    client = build_client(answering_session, clock, minimum_interval_seconds=0.2)

    client.get_json("/health")
    clock.spend(0.05)
    client.get_json("/health")

    assert clock.sleep_durations == [pytest.approx(0.15)]


@pytest.mark.parametrize("invalid_interval", [-1.0, -0.1])
def test_a_negative_interval_is_rejected(invalid_interval: float) -> None:
    with pytest.raises(ValueError):
        ResilientHttpClient(
            base_url=BASE_URL,
            timeout_seconds=5.0,
            minimum_interval_seconds=invalid_interval,
        )
