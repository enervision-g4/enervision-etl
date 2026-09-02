from datetime import UTC, datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..contracts.energy_reading import EnergyReading

LOAD_PERCENT_DECIMALS = 2


def to_utc(timestamp: datetime, source_timezone: str) -> datetime:
    # L'API mock renvoie des horodatages naifs. Le fuseau qu'ils sous entendent est une
    # convention externe, declaree en configuration, jamais devinee depuis la donnee.
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
    # Une mesure absente ne vaut pas zero pour cent de charge : elle reste absente.
    if consumption_kw is None:
        return None
    if capacity_kw <= 0:
        raise ValueError(f"capacity_kw must be strictly positive, received {capacity_kw}")

    return round(100 * consumption_kw / capacity_kw, LOAD_PERCENT_DECIMALS)


def normalize_reading(reading: EnergyReading, source_timezone: str) -> EnergyReading:
    return reading.model_copy(
        update={"timestamp": to_utc(reading.timestamp, source_timezone)}
    )
