from typing import Any, Optional

import pytest
from typer.testing import CliRunner

from enervision_consumer import cli
from enervision_consumer.cli import application

runner = CliRunner()

DATABASE_URL = "postgres://g4_app:secret@g4_db:5432/g4_db"
BROKER = "g4_kafka:9092"


class StubConsumer:
    """Consumer vide : de quoi observer le cablage sans bus reel."""

    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def poll(self, timeout: float = 0) -> Optional[Any]:
        return None

    def commit(self, message: Optional[Any] = None, asynchronous: bool = True) -> None:
        return None

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def wired_services(monkeypatch: pytest.MonkeyPatch, connection: Any) -> dict[str, Any]:
    """Remplace les acces reseau par des doubles et enregistre les appels."""
    opened_groups: list[str] = []
    opened_consumers: list[StubConsumer] = []
    opened_databases: list[str] = []

    def create_consumer(bootstrap_servers: str, group_id: str) -> StubConsumer:
        opened_groups.append(group_id)
        consumer = StubConsumer()
        opened_consumers.append(consumer)
        return consumer

    def create_connection(database_url: str) -> Any:
        opened_databases.append(database_url)
        return connection

    monkeypatch.setattr(cli, "create_consumer", create_consumer)
    monkeypatch.setattr(cli, "create_connection", create_connection)
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", BROKER)
    monkeypatch.delenv("KAFKA_CONSUMER_GROUP", raising=False)

    return {
        "groups": opened_groups,
        "consumers": opened_consumers,
        "databases": opened_databases,
        "connection": connection,
    }


def test_the_help_lists_both_services() -> None:
    result = runner.invoke(application, ["--help"])

    assert result.exit_code == 0
    assert "consume-persistence" in result.stdout
    assert "consume-alerting" in result.stdout


def test_persistence_opens_its_own_group_and_one_for_the_registry(
    wired_services: dict[str, Any],
) -> None:
    # Le drainage du referentiel relit le topic compacte depuis son debut : il lui faut
    # un groupe distinct, sinon il ferait avancer les offsets du service.
    result = runner.invoke(application, ["consume-persistence", "--max-messages", "0"])

    assert result.exit_code == 0
    assert wired_services["groups"] == [
        "enervision-consumer-persistence",
        "enervision-consumer-persistence-registry",
    ]


def test_alerting_opens_its_own_group(wired_services: dict[str, Any]) -> None:
    result = runner.invoke(application, ["consume-alerting", "--max-messages", "0"])

    assert result.exit_code == 0
    assert wired_services["groups"][0] == "enervision-consumer-alerting"


def test_the_configured_database_is_the_one_opened(wired_services: dict[str, Any]) -> None:
    runner.invoke(application, ["consume-persistence", "--max-messages", "0"])

    assert wired_services["databases"] == [DATABASE_URL]


def test_the_registry_is_drained_before_any_message_is_read(
    wired_services: dict[str, Any],
) -> None:
    runner.invoke(application, ["consume-persistence", "--max-messages", "0"])

    drain_consumer = wired_services["consumers"][1]
    assert drain_consumer.subscribed == ["enervision.site"]
    assert drain_consumer.closed is True


def test_the_service_consumer_is_released_on_exit(wired_services: dict[str, Any]) -> None:
    runner.invoke(application, ["consume-persistence", "--max-messages", "0"])

    assert wired_services["consumers"][0].closed is True


def test_an_incomplete_configuration_stops_the_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Echouer au demarrage, pas apres plusieurs minutes de consommation.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", BROKER)

    result = runner.invoke(application, ["consume-persistence", "--max-messages", "0"])

    assert result.exit_code != 0
    assert "database_url" in str(result.exception).lower()
