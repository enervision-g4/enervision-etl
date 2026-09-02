"""Contrat d'interface : enveloppe des messages entre le collecteur et les consumers.

Trois regles. Le payload epouse strictement les colonnes du modele de donnees. Un champ
inconnu est tolere a la lecture, donc seules les evolutions additives sont sures et
schema_version doit accompagner toute rupture. Les horodatages sont situes, en UTC.
"""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .energy_reading import EnergyReading
from .imputed_reading import ImputationMethod, ImputedReading
from .site import Site

SCHEMA_VERSION: Final[str] = "1.0.0"
"""Version du contrat. A incrementer des qu'une evolution n'est pas additive."""


class EventType(StrEnum):
    """Nature du message, alignee sur la table de destination."""

    MEASURE_RAW = "measure_raw"
    MEASURE_IMPUTED = "measure_imputed"
    SITE = "site"
    ALERT = "alert"


class CollectionMode(StrEnum):
    """Mode de collecte, qui distingue le flux temps reel du rattrapage historique."""

    REALTIME = "realtime"
    BATCH = "batch"


class SiteScopedPayload(BaseModel):
    """Base des payloads rattaches a un site.

    site_id sert de cle de partition, donc d'assurance d'ordre chronologique par site.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    site_id: str


class TimestampedPayload(SiteScopedPayload):
    """Base des payloads portant un horodatage de mesure, situe et exprime en UTC."""

    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def require_utc(cls, measured_at: datetime) -> datetime:
        """Refuse un horodatage naif et ramene les autres a UTC.

        Args:
            measured_at: Horodatage a controler.

        Returns:
            Le meme instant exprime en UTC.

        Raises:
            ValueError: Si l'horodatage ne porte aucun fuseau.
        """
        if measured_at.tzinfo is None:
            raise ValueError(
                "a published timestamp must carry a timezone, normalize it to UTC first"
            )
        return measured_at.astimezone(UTC)


class MeasureRawPayload(TimestampedPayload):
    """Mesure brute, image fidele de la reponse de l'API. Alimente MEASURE_RAW."""

    consumption_kw: Optional[float] = None
    consumption_kwh: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    power_factor: Optional[float] = None
    temperature_celsius: Optional[float] = None
    humidity_percent: Optional[float] = None
    null_reasons: list[str] = Field(default_factory=list)
    data_quality: str


class MeasureImputedPayload(TimestampedPayload):
    """Mesure reconstruite. Alimente MEASURE_IMPUTED.

    Correlation avec la mesure brute par (site_id, timestamp), measure_raw_id etant
    genere a l'insertion par le consumer.
    """

    consumption_kw: Optional[float] = None
    consumption_kwh: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    power_factor: Optional[float] = None
    temperature_celsius: Optional[float] = None
    humidity_percent: Optional[float] = None
    imputation_method: ImputationMethod


class SitePayload(SiteScopedPayload):
    """Caracteristiques fixes d'un site. Alimente SITE, sur un topic compacte."""

    site_type: str
    site_name: str
    location: str
    capacity_kw: float
    status: str


class MessageEnvelope[PayloadT: SiteScopedPayload](BaseModel):
    """Message publie sur le bus, quelle que soit sa nature.

    Attributes:
        produced_at: Instant de production, distinct de l'instant de mesure.
        collection_mode: Absent pour les fiches de site.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    schema_version: str = SCHEMA_VERSION
    event_type: EventType
    produced_at: datetime
    collection_mode: Optional[CollectionMode] = None
    payload: PayloadT

    @property
    def partition_key(self) -> str:
        """Cle de partition : l'identifiant du site, qui garantit l'ordre par site.

        Returns:
            L'identifiant du site porte par le payload.
        """
        return self.payload.site_id


def _now() -> datetime:
    return datetime.now(UTC)


def envelope_for_raw_reading(
    reading: EnergyReading,
    collection_mode: CollectionMode,
) -> MessageEnvelope[MeasureRawPayload]:
    """Emballe une mesure brute pour publication.

    Args:
        reading: Releve normalise, dont l'horodatage porte deja son fuseau.
        collection_mode: Mode de collecte ayant produit ce releve.

    Returns:
        Le message pret a serialiser.

    Raises:
        ValidationError: Si l'horodatage du releve est naif.
    """
    return MessageEnvelope[MeasureRawPayload](
        event_type=EventType.MEASURE_RAW,
        produced_at=_now(),
        collection_mode=collection_mode,
        payload=MeasureRawPayload(
            site_id=reading.site_id,
            timestamp=reading.timestamp,
            consumption_kw=reading.consumption_kw,
            consumption_kwh=reading.consumption_kwh,
            voltage_v=reading.voltage_v,
            current_a=reading.current_a,
            power_factor=reading.power_factor,
            temperature_celsius=reading.temperature_celsius,
            humidity_percent=reading.humidity_percent,
            null_reasons=list(reading.null_reasons),
            data_quality=reading.data_quality,
        ),
    )


def envelope_for_imputed_reading(
    reading: ImputedReading,
    collection_mode: CollectionMode,
) -> MessageEnvelope[MeasureImputedPayload]:
    """Emballe une mesure reconstruite pour publication.

    Args:
        reading: Mesure imputee, dont l'horodatage porte deja son fuseau.
        collection_mode: Mode de collecte ayant produit la mesure d'origine.

    Returns:
        Le message pret a serialiser.

    Raises:
        ValidationError: Si l'horodatage de la mesure est naif.
    """
    return MessageEnvelope[MeasureImputedPayload](
        event_type=EventType.MEASURE_IMPUTED,
        produced_at=_now(),
        collection_mode=collection_mode,
        payload=MeasureImputedPayload(
            site_id=reading.site_id,
            timestamp=reading.timestamp,
            consumption_kw=reading.consumption_kw,
            consumption_kwh=reading.consumption_kwh,
            voltage_v=reading.voltage_v,
            current_a=reading.current_a,
            power_factor=reading.power_factor,
            temperature_celsius=reading.temperature_celsius,
            humidity_percent=reading.humidity_percent,
            imputation_method=reading.imputation_method,
        ),
    )


def envelope_for_site(site: Site) -> MessageEnvelope[SitePayload]:
    """Emballe les caracteristiques d'un site pour publication.

    Args:
        site: Site issu de la liste renvoyee par l'API.

    Returns:
        Le message pret a serialiser, sans mode de collecte.
    """
    return MessageEnvelope[SitePayload](
        event_type=EventType.SITE,
        produced_at=_now(),
        payload=SitePayload(
            site_id=site.site_id,
            site_type=site.site_type,
            site_name=site.site_name,
            location=site.location,
            capacity_kw=site.capacity_kw,
            status=site.status,
        ),
    )
