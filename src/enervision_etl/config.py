"""Configuration lue dans l'environnement et validee au demarrage.

Aucune adresse en dur, et une configuration incomplete fait echouer le demarrage
immediatement plutot qu'apres plusieurs minutes de fonctionnement.
"""

from enum import StrEnum
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ACCEPTED_URL_SCHEMES = ("http://", "https://")
"""Schemas acceptes pour l'URL de l'API mock."""

class PublisherTarget(StrEnum):
    """Destination des messages produits par le collecteur."""

    STDOUT = "stdout"
    KAFKA = "kafka"


EVERY_SITE_WILDCARD = "ALL"
"""Valeur de SITES demandant explicitement la collecte de tout le parc."""


class EtlSettings(BaseSettings):
    """Parametres du pipeline ETL.

    Chaque champ correspond a une variable d'environnement de meme nom en
    majuscules. Les champs sans valeur par defaut sont obligatoires.

    Attributes:
        api_mock_base_url: Racine de l'API mock, schema http ou https obligatoire.
        api_mock_timeout_seconds: Delai d'attente applique a chaque requete.
        api_mock_source_timezone: Fuseau suppose des horodatages naifs de l'API.
        poll_interval_seconds: Periode du collecteur temps reel.
        site_refresh_interval_seconds: Delai entre deux verifications de la liste
            des sites. Une republication n'a lieu que si un site a change.
        sites: Identifiants des sites a collecter. Une liste vide, absente ou
            reduite au mot ALL demande la collecte de tout le parc expose par l'API.
        kafka_bootstrap_servers: Broker Kafka du conteneur messager-consumer.
        kafka_topic_site: Topic de la liste des sites, alimentant la table SITE.
            A creer avec une politique de compaction.
        kafka_topic_measure_raw: Topic alimentant la table MEASURE_RAW.
        kafka_topic_measure_imputed: Topic alimentant la table MEASURE_IMPUTED.
        kafka_topic_alert: Topic alimentant la table ALERT.
        publisher_target: Destination des messages, stdout ou kafka.
        log_level: Seuil de journalisation.
        log_as_json: Vrai pour des logs JSON, faux pour un rendu console.
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
    site_refresh_interval_seconds: float = Field(default=3600.0, gt=0)
    sites: Annotated[list[str], NoDecode] = Field(default_factory=list)

    kafka_bootstrap_servers: str = Field(min_length=1)
    # Un topic par table du MCD, ce qui rend la destination de chaque message lisible
    # sans documentation et aligne les deux depots sur un vocabulaire unique.
    kafka_topic_site: str = "enervision.site"
    kafka_topic_measure_raw: str = "enervision.measure_raw"
    kafka_topic_measure_imputed: str = "enervision.measure_imputed"
    kafka_topic_alert: str = "enervision.alert"

    # stdout par defaut : rien ne doit tenter d'atteindre un broker par accident.
    publisher_target: PublisherTarget = PublisherTarget.STDOUT
    log_level: str = "INFO"
    log_as_json: bool = True

    metrics_port: int = Field(default=8001, gt=0, le=65535)
    imputation_max_gap_measures: int = Field(default=3, gt=0)

    @field_validator("api_mock_base_url")
    @classmethod
    def normalize_base_url(cls, configured_url: str) -> str:
        """Retire la barre oblique finale et verifie la presence d'un schema.

        Args:
            configured_url: Valeur brute lue dans l'environnement.

        Returns:
            L'URL sans barre oblique finale.

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
        """Decoupe la liste des sites separee par des virgules.

        Args:
            configured_sites: Chaine separee par des virgules, ou liste construite.

        Returns:
            Les identifiants, sans espaces ni entrees vides.

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
    def normalize_wildcard(cls, site_identifiers: list[str]) -> list[str]:
        """Ramene la demande de collecte totale a une liste vide.

        Enumerer les sites dupliquerait ce que l'API expose deja. Une liste vide
        signifie tout le parc, le filtrage explicite restant possible.

        Args:
            site_identifiers: Identifiants deja decoupes.

        Returns:
            La liste demandee, ou une liste vide pour signifier tout le parc.
        """
        if len(site_identifiers) == 1 and site_identifiers[0].upper() == EVERY_SITE_WILDCARD:
            return []
        return site_identifiers

    @property
    def collects_every_site(self) -> bool:
        """Indique si la configuration demande la collecte de tout le parc expose."""
        return not self.sites


def load_settings() -> EtlSettings:
    """Charge la configuration depuis l'environnement et le fichier .env.

    Returns:
        Les parametres valides du pipeline.

    Raises:
        ValidationError: Si une variable obligatoire manque ou est invalide.
    """
    return EtlSettings()  # type: ignore[call-arg]
