import pytest
from pydantic import ValidationError

from enervision_etl.config import EtlSettings

CONFIGURABLE_VARIABLES = (
    "API_MOCK_BASE_URL",
    "API_MOCK_TIMEOUT_SECONDS",
    "API_MOCK_SOURCE_TIMEZONE",
    "POLL_INTERVAL_SECONDS",
    "SITES",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_READINGS",
    "KAFKA_TOPIC_READINGS_IMPUTED",
    "KAFKA_TOPIC_ALERTS",
    "METRICS_PORT",
    "IMPUTATION_MAX_GAP_MEASURES",
)

MINIMAL_ENVIRONMENT = {
    "API_MOCK_BASE_URL": "http://192.0.2.10:8000",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "SITES": "SITE001,SITE002",
}


@pytest.fixture
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for variable_name in CONFIGURABLE_VARIABLES:
        monkeypatch.delenv(variable_name, raising=False)
    return monkeypatch


def build_settings(**environment: str) -> EtlSettings:
    return EtlSettings(_env_file=None, **environment)


def test_loads_required_settings_from_environment(isolated_environment: pytest.MonkeyPatch) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    settings = build_settings()

    assert settings.api_mock_base_url == "http://192.0.2.10:8000"
    assert settings.kafka_bootstrap_servers == "kafka:9092"


def test_defaults_match_the_specification(isolated_environment: pytest.MonkeyPatch) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    settings = build_settings()

    assert settings.api_mock_timeout_seconds == 5.0
    assert settings.poll_interval_seconds == 60
    assert settings.api_mock_source_timezone == "UTC"
    assert settings.kafka_topic_readings == "enervision.readings.raw"
    assert settings.kafka_topic_readings_imputed == "enervision.readings.imputed"
    assert settings.kafka_topic_alerts == "enervision.alerts"
    assert settings.metrics_port == 8001
    assert settings.imputation_max_gap_measures == 3


def test_site_identifiers_are_split_on_comma(isolated_environment: pytest.MonkeyPatch) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv(
        "SITES",
        "SITE001,SITE002,SITE003,SITE004,SITE005,SITE006,SITE007",
    )

    settings = build_settings()

    assert settings.sites == [
        "SITE001",
        "SITE002",
        "SITE003",
        "SITE004",
        "SITE005",
        "SITE006",
        "SITE007",
    ]


def test_surrounding_whitespace_in_site_list_is_tolerated(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("SITES", " SITE001 , SITE002 ,SITE003 ")

    settings = build_settings()

    assert settings.sites == ["SITE001", "SITE002", "SITE003"]


def test_trailing_slash_is_stripped_from_base_url(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("API_MOCK_BASE_URL", "http://192.0.2.10:8000/")

    settings = build_settings()

    assert settings.api_mock_base_url == "http://192.0.2.10:8000"


@pytest.mark.parametrize("missing_variable", ["API_MOCK_BASE_URL", "KAFKA_BOOTSTRAP_SERVERS"])
def test_missing_mandatory_variable_is_rejected(
    isolated_environment: pytest.MonkeyPatch,
    missing_variable: str,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        if variable_name != missing_variable:
            isolated_environment.setenv(variable_name, value)

    with pytest.raises(ValidationError):
        build_settings()


def test_empty_site_list_is_rejected(isolated_environment: pytest.MonkeyPatch) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("SITES", " , ")

    with pytest.raises(ValidationError):
        build_settings()


def test_base_url_without_http_scheme_is_rejected(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("API_MOCK_BASE_URL", "192.0.2.10:8000")

    with pytest.raises(ValidationError):
        build_settings()


def test_non_positive_poll_interval_is_rejected(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("POLL_INTERVAL_SECONDS", "0")

    with pytest.raises(ValidationError):
        build_settings()
