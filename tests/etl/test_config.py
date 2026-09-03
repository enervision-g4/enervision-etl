import pytest
from pydantic import ValidationError

from enervision_etl.config import EtlSettings

CONFIGURABLE_VARIABLES = (
    "API_MOCK_BASE_URL",
    "API_MOCK_TIMEOUT_SECONDS",
    "API_MOCK_SOURCE_TIMEZONE",
    "API_MOCK_MIN_REQUEST_INTERVAL_SECONDS",
    "POLL_INTERVAL_SECONDS",
    "SITE_REFRESH_INTERVAL_SECONDS",
    "SITES",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_SITE",
    "KAFKA_TOPIC_MEASURE_RAW",
    "KAFKA_TOPIC_MEASURE_IMPUTED",
    "KAFKA_TOPIC_ALERT",
    "PUBLISHER_TARGET",
    "LOG_LEVEL",
    "LOG_AS_JSON",
    "METRICS_PORT",
    "IMPUTATION_MAX_GAP_MEASURES",
)

MINIMAL_ENVIRONMENT = {
    "API_MOCK_BASE_URL": "http://192.0.2.10:8000",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
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


def test_site_list_is_not_mandatory(isolated_environment: pytest.MonkeyPatch) -> None:
    # Enumerer les sites dupliquerait une information que l'API expose deja.
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    build_settings()


def test_defaults_match_the_project_conventions(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    settings = build_settings()

    assert settings.api_mock_timeout_seconds == 5.0
    assert settings.poll_interval_seconds == 60
    assert settings.site_refresh_interval_seconds == 3600.0
    assert settings.api_mock_source_timezone == "UTC"
    assert settings.api_mock_min_request_interval_seconds == 0.2
    assert settings.kafka_topic_site == "enervision.site"
    assert settings.kafka_topic_measure_raw == "enervision.measure_raw"
    assert settings.kafka_topic_measure_imputed == "enervision.measure_imputed"
    assert settings.kafka_topic_alert == "enervision.alert"
    assert settings.publisher_target == "stdout"
    assert settings.log_level == "INFO"
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


def test_an_absent_site_list_means_the_whole_park(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    settings = build_settings()

    assert settings.sites == []
    assert settings.collects_every_site is True


@pytest.mark.parametrize("wildcard", ["ALL", "all", " All "])
def test_the_wildcard_means_the_whole_park(
    isolated_environment: pytest.MonkeyPatch,
    wildcard: str,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("SITES", wildcard)

    settings = build_settings()

    assert settings.sites == []
    assert settings.collects_every_site is True


def test_a_blank_site_list_means_the_whole_park(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("SITES", " , ")

    settings = build_settings()

    assert settings.collects_every_site is True


def test_an_explicit_list_restricts_the_collection(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("SITES", "SITE002,SITE005")

    settings = build_settings()

    assert settings.sites == ["SITE002", "SITE005"]
    assert settings.collects_every_site is False


def test_windows_line_endings_do_not_corrupt_values(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    # docker --env-file transmet le retour chariot d'un fichier enregistre sous Windows.
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value + "\r")
    isolated_environment.setenv("PUBLISHER_TARGET", "stdout\r")
    isolated_environment.setenv("SITES", "SITE001,SITE002\r")

    settings = build_settings()

    assert settings.api_mock_base_url == "http://192.0.2.10:8000"
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.publisher_target == "stdout"
    assert settings.sites == ["SITE001", "SITE002"]


def test_base_url_without_http_scheme_is_rejected(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("API_MOCK_BASE_URL", "192.0.2.10:8000")

    with pytest.raises(ValidationError):
        build_settings()


def test_every_topic_is_named_after_a_table_of_the_data_model(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)

    settings = build_settings()

    for topic in (
        settings.kafka_topic_site,
        settings.kafka_topic_measure_raw,
        settings.kafka_topic_measure_imputed,
        settings.kafka_topic_alert,
    ):
        prefixe, _, table = topic.partition(".")
        assert prefixe == "enervision"
        assert table in {"measure_raw", "measure_imputed", "alert", "site"}


def test_a_topic_can_be_overridden_from_the_environment(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("KAFKA_TOPIC_MEASURE_RAW", "enervision.mesures")

    settings = build_settings()

    assert settings.kafka_topic_measure_raw == "enervision.mesures"


def test_non_positive_poll_interval_is_rejected(
    isolated_environment: pytest.MonkeyPatch,
) -> None:
    for variable_name, value in MINIMAL_ENVIRONMENT.items():
        isolated_environment.setenv(variable_name, value)
    isolated_environment.setenv("POLL_INTERVAL_SECONDS", "0")

    with pytest.raises(ValidationError):
        build_settings()
