"""Contrat des mesures reconstruites, destinees a la table MEASURE_IMPUTED."""

from datetime import datetime
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ImputationMethod(StrEnum):
    """Strategies de reconstruction, nommees selon les termes usuels du domaine.

    NONE signale une mesure laissee telle quelle : complete, ou bien irrecuperable.
    """

    LINEAR_INTERPOLATION = "linear_interpolation"
    FORWARD_FILL = "forward_fill"
    MOVING_AVERAGE = "moving_average"
    EXCLUDED = "excluded"
    NONE = "none"


class ImputedReading(BaseModel):
    """Mesure reconstruite, publiee dans un flux distinct de la mesure brute.

    Ne porte aucun identifiant technique : measure_raw_id est un UUID genere a
    l'insertion par le consumer. La correlation passe par (site_id, timestamp).

    Attributes:
        imputation_method: Strategie appliquee a cette ligne.
        imputed_fields: Champs effectivement reconstruits, pour la supervision.
    """

    model_config = ConfigDict(frozen=True)

    site_id: str
    timestamp: datetime

    consumption_kw: Optional[float] = None
    consumption_kwh: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    power_factor: Optional[float] = None
    temperature_celsius: Optional[float] = None
    humidity_percent: Optional[float] = None

    imputation_method: ImputationMethod = ImputationMethod.NONE
    imputed_fields: tuple[str, ...] = Field(default_factory=tuple)
