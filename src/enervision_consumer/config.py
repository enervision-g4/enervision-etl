"""Configuration des consumers, lue dans l'environnement et validee au demarrage.

Aucune adresse en dur, et une configuration incomplete fait echouer le demarrage
immediatement plutot qu'apres plusieurs minutes de fonctionnement. Chaque service ne
declare que les topics dont il a la charge : un conteneur d'alerting n'a pas a connaitre
les topics de mesures.
"""

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ACCEPTED_DATABASE_SCHEMES = ("postgres://", "postgresql://")
"""Schemas acceptes pour l'URL de la base. libpq reconnait les deux."""

REGISTRY_GROUP_SUFFIX = "-registry"
"""Suffixe du groupe dedie au redrainage du referentiel."""


class ConsumerSettings(BaseSettings):
    """Parametres communs aux deux consumers.

    Attributes:
        database_url: URL de connexion a PostgreSQL.
        kafka_bootstrap_servers: Adresse du broker.
        kafka_topic_site: Topic du referentiel, consomme par les deux services.
        kafka_consumer_group: Groupe du service, distinct pour chacun.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str
    kafka_bootstrap_servers: str
    kafka_topic_site: str = "enervision.site"
    kafka_consumer_group: str

    log_level: str = "INFO"
    log_as_json: bool = True

    @model_validator(mode="before")
    @classmethod
    def strip_surrounding_whitespace(
        cls,
        submitted_values: dict[str, object],
    ) -> dict[str, object]:
        """Retire les espaces et retours chariot autour de chaque valeur.

        Un fichier .env enregistre sous Windows termine ses lignes par un retour
        chariot, que docker transmet tel quel dans l'environnement du conteneur.

        Args:
            submitted_values: Valeurs brutes issues de l'environnement.

        Returns:
            Les memes valeurs, chaines nettoyees.
        """
        if not isinstance(submitted_values, dict):
            return submitted_values
        return {
            name: value.strip() if isinstance(value, str) else value
            for name, value in submitted_values.items()
        }

    @field_validator("database_url")
    @classmethod
    def require_a_known_database_scheme(cls, configured_url: str) -> str:
        """Verifie que l'URL de la base porte un schema reconnu.

        Args:
            configured_url: Valeur brute lue dans l'environnement.

        Returns:
            L'URL inchangee.

        Raises:
            ValueError: Si l'URL ne commence par aucun schema accepte.
        """
        if not configured_url.startswith(ACCEPTED_DATABASE_SCHEMES):
            raise ValueError(
                f"DATABASE_URL must start with one of {ACCEPTED_DATABASE_SCHEMES}, "
                f"received {configured_url!r}"
            )
        return configured_url

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def require_a_broker_address(cls, configured_brokers: str) -> str:
        """Refuse une adresse de broker vide.

        Args:
            configured_brokers: Valeur brute lue dans l'environnement.

        Returns:
            L'adresse inchangee.

        Raises:
            ValueError: Si aucune adresse n'est renseignee.
        """
        if not configured_brokers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required to reach the message bus")
        return configured_brokers

    @property
    def registry_consumer_group(self) -> str:
        """Groupe dedie au redrainage du referentiel.

        Distinct du groupe principal : ce drainage relit le topic compacte depuis son
        debut a chaque fois, et n'acquitte jamais ses offsets.
        """
        return f"{self.kafka_consumer_group}{REGISTRY_GROUP_SUFFIX}"


class PersistenceConsumerSettings(ConsumerSettings):
    """Parametres du consumer de persistance des sites et des mesures."""

    kafka_topic_measure_raw: str = "enervision.measure_raw"
    kafka_topic_measure_imputed: str = "enervision.measure_imputed"
    kafka_consumer_group: str = "enervision-consumer-persistence"


class AlertingConsumerSettings(ConsumerSettings):
    """Parametres du consumer d'alerting."""

    kafka_topic_alert: str = "enervision.alert"
    kafka_consumer_group: str = "enervision-consumer-alerting"


def load_persistence_settings() -> PersistenceConsumerSettings:
    """Charge la configuration du consumer de persistance.

    Returns:
        Les parametres valides du service.

    Raises:
        ValidationError: Si une variable obligatoire manque ou est invalide.
    """
    return PersistenceConsumerSettings()  # type: ignore[call-arg]


def load_alerting_settings() -> AlertingConsumerSettings:
    """Charge la configuration du consumer d'alerting.

    Returns:
        Les parametres valides du service.

    Raises:
        ValidationError: Si une variable obligatoire manque ou est invalide.
    """
    return AlertingConsumerSettings()  # type: ignore[call-arg]
