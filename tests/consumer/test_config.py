from typing import Any

import pytest
from pydantic import ValidationError

from enervision_consumer.config import (
    AlertingConsumerSettings,
    PersistenceConsumerSettings,
)

CONFIGURABLE_VARIABLES = (
    "DATABASE_URL",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_SITE",
    "KAFKA_TOPIC_MEASURE_RAW",
    "KAFKA_TOPIC_MEASURE_IMPUTED",
    "KAFKA_TOPIC_ALERT",
    "KAFKA_CONSUMER_GROUP",
    "LOG_LEVEL",
    "LOG_AS_JSON",
)

VALID_DATABASE_URL = "postgres://g4_app:secret@g4_db:5432/g4_db"
VALID_BROKER = "g4_kafka:9092"


@pytest.fixture
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for variable_name in CONFIGURABLE_VARIABLES:
        monkeypatch.delenv(variable_name, raising=False)
    return monkeypatch


def build_persistence(**environment: Any) -> PersistenceConsumerSettings:
    return PersistenceConsumerSettings(_env_file=None, **environment)


def build_alerting(**environment: Any) -> AlertingConsumerSettings:
    return AlertingConsumerSettings(_env_file=None, **environment)


def test_loads_required_settings_from_environment(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    settings = build_persistence(
        database_url=VALID_DATABASE_URL, kafka_bootstrap_servers=VALID_BROKER
    )

    assert settings.database_url == VALID_DATABASE_URL
    assert settings.kafka_bootstrap_servers == VALID_BROKER


def test_a_missing_database_url_stops_the_startup(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    # Mieux vaut un echec immediat et lisible qu'une premiere ecriture qui echoue
    # apres plusieurs minutes de consommation.
    with pytest.raises(ValidationError):
        build_persistence(kafka_bootstrap_servers=VALID_BROKER)


def test_a_missing_broker_stops_the_startup(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValidationError):
        build_persistence(database_url=VALID_DATABASE_URL)


def test_an_empty_broker_is_refused(isolated_environment: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError):
        build_persistence(database_url=VALID_DATABASE_URL, kafka_bootstrap_servers="")


def test_both_postgres_url_schemes_are_accepted(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    # enervision-devops publie ses URL en postgres://, psycopg accepte les deux.
    for url in ("postgres://u:p@h:5432/d", "postgresql://u:p@h:5432/d"):
        assert build_persistence(
            database_url=url, kafka_bootstrap_servers=VALID_BROKER
        ).database_url == url


def test_an_unknown_url_scheme_is_refused(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValidationError):
        build_persistence(
            database_url="mysql://u:p@h:3306/d", kafka_bootstrap_servers=VALID_BROKER
        )


def test_windows_line_endings_do_not_corrupt_values(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    settings = build_persistence(
        database_url=f"{VALID_DATABASE_URL}\r\n", kafka_bootstrap_servers=f"{VALID_BROKER}\r"
    )

    assert settings.database_url == VALID_DATABASE_URL
    assert settings.kafka_bootstrap_servers == VALID_BROKER


def test_the_two_services_default_to_distinct_consumer_groups(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    # Deux groupes distincts : chacun recoit l'integralite des messages du topic des
    # sites, la ou un groupe commun se les repartirait.
    persistance = build_persistence(
        database_url=VALID_DATABASE_URL, kafka_bootstrap_servers=VALID_BROKER
    )
    alerting = build_alerting(
        database_url=VALID_DATABASE_URL, kafka_bootstrap_servers=VALID_BROKER
    )

    assert persistance.kafka_consumer_group == "enervision-consumer-persistence"
    assert alerting.kafka_consumer_group == "enervision-consumer-alerting"


def test_the_registry_drain_uses_its_own_group(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    # Le redrainage relit le topic compacte depuis son debut : partager le groupe
    # principal ferait sauter des messages de mesures au passage.
    settings = build_persistence(
        database_url=VALID_DATABASE_URL, kafka_bootstrap_servers=VALID_BROKER
    )

    assert settings.registry_consumer_group == "enervision-consumer-persistence-registry"


def test_each_service_declares_only_the_topics_it_consumes(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    alerting = build_alerting(
        database_url=VALID_DATABASE_URL, kafka_bootstrap_servers=VALID_BROKER
    )

    assert alerting.kafka_topic_alert == "enervision.alert"
    assert not hasattr(alerting, "kafka_topic_measure_raw")
