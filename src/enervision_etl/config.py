"""Configuration du pipeline, lue dans l'environnement et validee au demarrage.

Aucune adresse n'est codee en dur. Une configuration incomplete fait echouer le
demarrage immediatement, avec un message explicite, plutot qu'au bout de
plusieurs minutes de fonctionnement sur une valeur absente.
"""

from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ACCEPTED_URL_SCHEMES = ("http://", "https://")
"""Schemas acceptes pour l'URL de l'API mock."""


class EtlSettings(BaseSettings):
    """Parametres du pipeline ETL.

    Chaque champ correspond a une variable d'environnement de meme nom en
    majuscules. Les champs sans valeur par defaut sont obligatoires.

    Attributes:
        api_mock_base_url: Racine de l'API mock, schema http ou https obligatoire.
        api_mock_timeout_seconds: Delai d'attente applique a chaque requete.
        api_mock_source_timezone: Fuseau suppose des horodatages naifs de l'API.
        poll_interval_seconds: Periode du collecteur temps reel.
        sites: Identifiants des sites a collecter.
        kafka_bootstrap_servers: Broker Kafka du conteneur messager-consumer.
        kafka_topic_readings: Topic des mesures brutes.
        kafka_topic_readings_imputed: Topic des mesures imputees.
        kafka_topic_alerts: Topic des alertes.
        metrics_port: Port d'exposition des metriques Prometheus.
        imputation_max_gap_measures: Longueur maximale d'un trou encore imputable.
    """

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
        """Retire la barre oblique finale et verifie la presence d'un schema.

        Args:
            configured_url: Valeur brute lue dans l'environnement.

        Returns:
            L'URL sans barre oblique finale, evitant les doubles barres a la concatenation.

        Raises:
            ValueError: Si l'URL ne commence pas par http:// ou https://.
        """
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
        """Decoupe la liste des sites fournie sous forme de chaine separee par des virgules.

        Args:
            configured_sites: Valeur brute, chaine separee par des virgules ou
                liste deja construite.

        Returns:
            Les identifiants de site, debarrasses des espaces et des entrees vides.

        Raises:
            TypeError: Si la valeur n'est ni une chaine ni une liste.
        """
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
        """Refuse une liste de sites vide.

        Args:
            site_identifiers: Identifiants deja decoupes.

        Returns:
            La liste inchangee si elle contient au moins un site.

        Raises:
            ValueError: Si la liste est vide.
        """
        # Un parc vide demarrerait sans jamais interroger l'API, panne silencieuse a eviter.
        if not site_identifiers:
            raise ValueError("SITES must contain at least one site identifier")
        return site_identifiers


def load_settings() -> EtlSettings:
    """Charge la configuration depuis l'environnement et le fichier .env.

    Returns:
        Les parametres valides du pipeline.

    Raises:
        ValidationError: Si une variable obligatoire manque ou est invalide.
    """
    return EtlSettings()  # type: ignore[call-arg]
