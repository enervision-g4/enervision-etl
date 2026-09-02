"""Normalisation des mesures avant publication.

Regroupe les conversions qui rendent une mesure comparable et exploitable en aval :
mise a l'heure UTC et calcul du taux de charge rapporte a la capacite du site.
Toutes les fonctions sont pures et sans effet de bord.
"""

from datetime import UTC, datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from enervision_contracts.energy_reading import EnergyReading

LOAD_PERCENT_DECIMALS = 2
"""Nombre de decimales conservees pour le taux de charge."""


def to_utc(timestamp: datetime, source_timezone: str) -> datetime:
    """Ramene un horodatage a UTC en explicitant son fuseau d'origine.

    Un horodatage deja situe est simplement converti, son decalage faisant foi. Un
    horodatage naif est d'abord rattache au fuseau declare en configuration : cette
    convention est externe a la donnee et ne doit jamais etre devinee.

    Args:
        timestamp: Horodatage a convertir, naif ou situe.
        source_timezone: Nom IANA du fuseau suppose des horodatages naifs,
            par exemple UTC ou Europe/Paris.

    Returns:
        Le meme instant, exprime en UTC et porteur de son fuseau.

    Raises:
        ValueError: Si source_timezone n'est pas un fuseau IANA connu.
    """
    if timestamp.tzinfo is not None:
        return timestamp.astimezone(UTC)

    try:
        declared_timezone = ZoneInfo(source_timezone)
    except (ZoneInfoNotFoundError, ValueError) as unknown_timezone:
        raise ValueError(
            f"Unknown source timezone {source_timezone!r}"
        ) from unknown_timezone

    return timestamp.replace(tzinfo=declared_timezone).astimezone(UTC)


def compute_load_percent(
    consumption_kw: Optional[float],
    capacity_kw: float,
) -> Optional[float]:
    """Calcule le taux de charge d'un site en pourcentage de sa capacite installee.

    Une valeur superieure a cent n'est pas plafonnee : la surcharge est un evenement
    metier reel, c'est meme celui qui declenche les alertes de type outage.

    Args:
        consumption_kw: Puissance instantanee mesuree, ou None si le compteur est muet.
        capacity_kw: Puissance maximale installee du site, strictement positive.

    Returns:
        Le taux de charge arrondi, ou None si la mesure est absente. Une mesure
        absente ne vaut pas zero pour cent : elle reste absente.

    Raises:
        ValueError: Si capacity_kw est nul ou negatif.
    """
    if consumption_kw is None:
        return None
    if capacity_kw <= 0:
        raise ValueError(f"capacity_kw must be strictly positive, received {capacity_kw}")

    return round(100 * consumption_kw / capacity_kw, LOAD_PERCENT_DECIMALS)


def normalize_reading(reading: EnergyReading, source_timezone: str) -> EnergyReading:
    """Produit une copie du releve dont l'horodatage est exprime en UTC.

    Les valeurs de mesure, les causes de nullite et le niveau de qualite sont
    reportes a l'identique. Le releve d'origine n'est pas modifie, le contrat
    EnergyReading etant immuable.

    Args:
        reading: Releve issu de l'API mock.
        source_timezone: Nom IANA du fuseau suppose des horodatages naifs.

    Returns:
        Un nouveau releve, identique au premier hormis son horodatage en UTC.

    Raises:
        ValueError: Si source_timezone n'est pas un fuseau IANA connu.
    """
    return reading.model_copy(
        update={"timestamp": to_utc(reading.timestamp, source_timezone)}
    )
