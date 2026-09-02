from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ACCEPTED_URL_SCHEMES = ("http://", "https://")


class EtlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    api_mock_base_url: str
    api_mock_timeout_seconds: float = Field(default=5.0, gt=0)
    api_mock_source_timezone: str = "UTC"

    poll_interval_seconds: int = Field(default=60, gt=0)
    sites: Annotated[list[str], NoDecode]

    kafka_bootstrap_servers: str = Field(min_length=1)
    kafka_topic_readings: str = "enervision.readings.raw"
    kafka_topic_readings_imputed: str = "enervision.readings.imputed"
    kafka_topic_alerts: str = "enervision.alerts"

    metrics_port: int = Field(default=8001, gt=0, le=65535)
    imputation_max_gap_measures: int = Field(default=3, gt=0)

    @field_validator("api_mock_base_url")
    @classmethod
    def normalize_base_url(cls, configured_url: str) -> str:
        stripped_url = configured_url.strip().rstrip("/")
        if not stripped_url.startswith(ACCEPTED_URL_SCHEMES):
            raise ValueError(
                f"API_MOCK_BASE_URL must start with one of {ACCEPTED_URL_SCHEMES}, "
                f"received {configured_url!r}"
            )
        return stripped_url

    @field_validator("sites", mode="before")
    @classmethod
    def split_site_identifiers(cls, configured_sites: object) -> list[str]:
        if isinstance(configured_sites, list):
            return [str(identifier).strip() for identifier in configured_sites]
        if not isinstance(configured_sites, str):
            raise TypeError(
                f"SITES must be a comma separated string, received {type(configured_sites)}"
            )
        return [
            identifier.strip()
            for identifier in configured_sites.split(",")
            if identifier.strip()
        ]

    @field_validator("sites")
    @classmethod
    def reject_empty_site_list(cls, site_identifiers: list[str]) -> list[str]:
        # Un parc vide demarrerait sans jamais interroger l'API, panne silencieuse a eviter.
        if not site_identifiers:
            raise ValueError("SITES must contain at least one site identifier")
        return site_identifiers


def load_settings() -> EtlSettings:
    return EtlSettings()  # type: ignore[call-arg]
