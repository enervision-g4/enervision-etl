"""Contrat des mesures reconstruites, destinees a la table MEASURE_IMPUTED."""

from datetime import datetime
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ImputationMethod(StrEnum):
    """Strategies de reconstruction, nommees comme dans la documentation du projet.

    La valeur portee par une ligne decrit ce qui lui a reellement ete applique.
    NONE signale une mesure laissee telle quelle, soit parce qu'elle etait complete,
    soit parce que le trou etait trop long ou depourvu de point d'ancrage.
    """

    LINEAR_INTERPOLATION = "linear_interpolation"
    FORWARD_FILL = "forward_fill"
    MOVING_AVERAGE = "moving_average"
    EXCLUDED = "excluded"
    NONE = "none"


class ImputedReading(BaseModel):
    """Mesure reconstruite correspondant a un releve brut.

    Cet objet ne remplace jamais le releve brut : il vit dans un flux distinct et
    declare la methode qui lui a ete appliquee, afin que toute valeur synthetique
    reste identifiable comme telle.

    La correlation avec MEASURE_RAW se fait sur la cle metier (site_id, timestamp).
    L'identifiant technique measure_raw_id est un UUID genere a l'insertion par le
    consumer de persistance, que ce service ne peut pas connaitre.

    Attributes:
        site_id: Identifiant metier du site mesure.
        timestamp: Horodatage du releve brut d'origine, inchange.
        consumption_kw: Puissance, mesuree ou reconstruite, ou None si irrecuperable.
        consumption_kwh: Energie, mesuree ou reconstruite, ou None.
        voltage_v: Tension, mesuree ou reconstruite, ou None.
        current_a: Intensite, mesuree ou reconstruite, ou None.
        power_factor: Facteur de puissance, mesure ou reconstruit, ou None.
        temperature_celsius: Temperature, mesuree ou reconstruite, ou None.
        humidity_percent: Humidite, mesuree ou reconstruite, ou None.
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
