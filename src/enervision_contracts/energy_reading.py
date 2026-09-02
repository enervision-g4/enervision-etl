"""Mesures energetiques renvoyees par /current et /readings.

Une valeur nulle n'est jamais detruite : tous les champs capteur sont optionnels et
accompagnes de leurs metadonnees de qualite.
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
    """Releve brut d'un site a un instant donne, valeurs nulles comprises.

    Alimente MEASURE_RAW, journal immuable servant a l'audit des capteurs.

    Attributes:
        timestamp: Horodatage naif, tel que renvoye par l'API.
        site_type: Denormalisation de l'API, non republiee vers Kafka.
        null_reasons: Causes des valeurs manquantes, liste non fermee.
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
            Les noms des champs valant None. Un tuple vide signifie qu'aucun
            capteur n'est muet.
        """
        return tuple(
            field_name
            for field_name in MEASUREMENT_FIELD_NAMES
            if getattr(self, field_name) is None
        )

    def has_known_data_quality(self) -> bool:
        """Indique si le niveau de qualite fait partie des valeurs connues.

        Permet de signaler une valeur inedite en supervision sans rejeter la mesure.

        Returns:
            True si data_quality appartient a KNOWN_DATA_QUALITY_LEVELS.
        """
        return self.data_quality in KNOWN_DATA_QUALITY_LEVELS
