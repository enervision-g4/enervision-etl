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

KNOWN_DATA_QUALITY_LEVELS: Final[frozenset[str]] = frozenset(
    {"good", "partial", "degraded", "critical"}
)


class EnergyReading(BaseModel):
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
        return tuple(
            field_name
            for field_name in MEASUREMENT_FIELD_NAMES
            if getattr(self, field_name) is None
        )

    def has_known_data_quality(self) -> bool:
        # La documentation ne garantit pas l'exhaustivite de cette liste : on signale une
        # valeur inconnue sans jamais rejeter la mesure, sous peine de perdre de la donnee.
        return self.data_quality in KNOWN_DATA_QUALITY_LEVELS
