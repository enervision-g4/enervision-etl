"""Contrat des mesures energetiques, renvoyees par /current et /readings.

Ce module porte la regle directrice du projet : une valeur nulle n'est jamais
detruite. Tous les champs capteur sont donc optionnels, aucune coercition vers
zero n'est appliquee, et les metadonnees de qualite accompagnent la mesure.
"""

from datetime import datetime
from typing import Final, Optional

from pydantic import BaseModel, ConfigDict, Field

MEASUREMENT_FIELD_NAMES: Final[tuple[str, ...]] = (
    "consumption_kw",
    "consumption_kwh",
    "voltage_v",
    "current_a",
    "power_factor",
    "temperature_celsius",
    "humidity_percent",
)
"""Champs porteurs d'une mesure physique, seuls candidats a l'imputation."""

KNOWN_DATA_QUALITY_LEVELS: Final[frozenset[str]] = frozenset(
    {"good", "partial", "degraded", "critical"}
)
"""Niveaux de qualite documentes. La liste n'est pas fermee cote validation."""


class EnergyReading(BaseModel):
    """Releve energetique d'un site a un instant donne.

    Image fidele de la reponse de l'API mock, y compris ses valeurs nulles. Aucun
    nettoyage n'est applique a ce stade : cet objet alimente MEASURE_RAW, qui est
    un journal immuable servant a l'audit de fiabilite des capteurs.

    Attributes:
        timestamp: Horodatage de la mesure, naif tel que renvoye par l'API.
        site_id: Identifiant metier du site mesure.
        site_type: Type de site, denormalisation de l'API non republiee vers Kafka.
        consumption_kw: Puissance instantanee, ou None si le compteur est muet.
        consumption_kwh: Energie sur la periode, ou None.
        voltage_v: Tension triphasee, ou None.
        current_a: Intensite, ou None.
        power_factor: Facteur de puissance, ou None.
        temperature_celsius: Temperature exterieure, ou None.
        humidity_percent: Humidite relative, ou None.
        null_reasons: Causes des valeurs manquantes, liste non fermee.
        data_quality: Niveau de qualite declare par l'API.
    """

    # extra="allow" : un champ inconnu ajoute par une future version de l'API est conserve
    # plutot que rejete ou silencieusement perdu, conformement a la regle de non destruction.
    # frozen=True : une mesure brute est un fait historique, aucun code aval ne doit la reecrire.
    model_config = ConfigDict(extra="allow", frozen=True)

    timestamp: datetime
    site_id: str
    site_type: Optional[str] = None

    consumption_kw: Optional[float] = None
    consumption_kwh: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    power_factor: Optional[float] = None
    temperature_celsius: Optional[float] = None
    humidity_percent: Optional[float] = None

    null_reasons: list[str] = Field(default_factory=list)
    data_quality: str

    def missing_measurement_fields(self) -> tuple[str, ...]:
        """Liste les champs de mesure absents de ce releve.

        Returns:
            Les noms des champs valant None, dans l'ordre de MEASUREMENT_FIELD_NAMES.
            Un tuple vide signifie que tous les capteurs ont repondu.
        """
        return tuple(
            field_name
            for field_name in MEASUREMENT_FIELD_NAMES
            if getattr(self, field_name) is None
        )

    def has_known_data_quality(self) -> bool:
        """Indique si le niveau de qualite fait partie des valeurs documentees.

        La documentation ne garantit pas l'exhaustivite de cette liste. Cette methode
        permet de signaler une valeur inedite en supervision sans jamais rejeter la
        mesure, sous peine de perdre de la donnee.

        Returns:
            True si data_quality appartient a KNOWN_DATA_QUALITY_LEVELS.
        """
        return self.data_quality in KNOWN_DATA_QUALITY_LEVELS
