"""Normalisation avant publication : mise a l'heure UTC et taux de charge.

Fonctions pures, sans effet de bord.
"""

from datetime import UTC, datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from enervision_contracts.alert import Alert
from enervision_contracts.energy_reading import EnergyReading

LOAD_PERCENT_DECIMALS = 2
"""Nombre de decimales conservees pour le taux de charge."""


def to_utc(timestamp: datetime, source_timezone: str) -> datetime:
    """Ramene un horodatage a UTC en explicitant son fuseau d'origine.

    Un horodatage situe est converti, son decalage faisant foi. Un horodatage naif est
    rattache au fuseau declare en configuration, convention externe a la donnee.

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
    """Calcule le taux de charge en pourcentage de la capacite installee.

    Une valeur superieure a cent n'est pas plafonnee : la surcharge est un evenement
    metier reel.

    Args:
        consumption_kw: Puissance mesuree, ou None si le compteur est muet.
        capacity_kw: Puissance maximale installee, strictement positive.

    Returns:
        Le taux arrondi, ou None si la mesure est absente. Une mesure absente ne vaut
        pas zero pour cent.

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

    Mesures, causes de nullite et niveau de qualite sont reportes a l'identique.

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


def normalize_alert(alert: Alert, source_timezone: str) -> Alert:
    """Produit une copie de l'alerte dont l'horodatage est exprime en UTC.

    Severite, type, message et valeurs mesurees sont reportes a l'identique.

    Args:
        alert: Alerte issue de l'API mock.
        source_timezone: Nom IANA du fuseau suppose des horodatages naifs.

    Returns:
        Une nouvelle alerte, identique a la premiere hormis son horodatage en UTC.

    Raises:
        ValueError: Si source_timezone n'est pas un fuseau IANA connu.
    """
    return alert.model_copy(
        update={"timestamp": to_utc(alert.timestamp, source_timezone)}
    )
