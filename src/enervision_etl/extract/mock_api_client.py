from datetime import datetime, timedelta
from math import ceil
from typing import Any, Final, Optional

from ..contracts.energy_reading import EnergyReading
from ..contracts.site import Site
from .errors import MockApiError
from .http_client import ResilientHttpClient

# Plafond impose par la documentation de l'endpoint /api/v1/readings.
MAX_READINGS_PER_REQUEST: Final[int] = 1000

# Resolution par defaut, alignee sur la periode de polling du collecteur temps reel.
DEFAULT_RESOLUTION_SECONDS: Final[float] = 60.0

# Garde fou : borne le nombre de tranches pour une periode demesuree.
MAX_CHUNKS_PER_WINDOW: Final[int] = 500


class MockApiClient:
    def __init__(self, http_client: ResilientHttpClient) -> None:
        self._http_client = http_client

    def is_healthy(self) -> bool:
        try:
            health_report = self._http_client.get_json("/health")
        except MockApiError:
            return False
        return bool(health_report.get("status") == "healthy")

    def fetch_site_registry(self) -> list[Site]:
        site_payloads = self._http_client.get_json("/api/v1/sites")
        return [Site.model_validate(payload) for payload in site_payloads]

    def fetch_site(self, site_id: str) -> Site:
        site_payload = self._http_client.get_json(
            f"/api/v1/sites/{site_id}",
            site_id=site_id,
        )
        return Site.model_validate(site_payload)

    def fetch_current_reading(self, site_id: str) -> EnergyReading:
        reading_payload = self._http_client.get_json(
            f"/api/v1/sites/{site_id}/current",
            site_id=site_id,
        )
        return EnergyReading.model_validate(reading_payload)

    def fetch_readings_window(
        self,
        site_id: Optional[str],
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: float = DEFAULT_RESOLUTION_SECONDS,
    ) -> list[EnergyReading]:
        # Le parametre limit de /api/v1/readings n'est pas une taille de page : il fixe le
        # nombre de points repartis uniformement dans la fenetre demandee, l'intervalle
        # valant (end_time - start_time) / limit. Une pagination par curseur est donc
        # impossible, d'autant que l'API regenere la serie a chaque appel. On decoupe donc
        # la periode en tranches de duree fixe, chacune echantillonnee a la resolution voulue.
        if resolution_seconds <= 0:
            raise ValueError(
                f"resolution_seconds must be strictly positive, received {resolution_seconds}"
            )
        if start_time > end_time:
            raise ValueError(
                f"start_time {start_time.isoformat()} is after end_time {end_time.isoformat()}"
            )

        chunk_duration = timedelta(seconds=resolution_seconds * MAX_READINGS_PER_REQUEST)
        collected_readings: list[EnergyReading] = []
        already_collected_timestamps: set[datetime] = set()
        chunk_start_time = start_time

        for _ in range(MAX_CHUNKS_PER_WINDOW):
            if chunk_start_time >= end_time:
                break
            chunk_end_time = min(chunk_start_time + chunk_duration, end_time)
            requested_points = self._points_for_chunk(
                chunk_start_time, chunk_end_time, resolution_seconds
            )

            chunk = self._fetch_readings_chunk(
                site_id, chunk_start_time, chunk_end_time, requested_points
            )
            for reading in chunk:
                if reading.timestamp in already_collected_timestamps:
                    continue
                already_collected_timestamps.add(reading.timestamp)
                collected_readings.append(reading)

            chunk_start_time = chunk_end_time

        collected_readings.sort(key=lambda reading: reading.timestamp)
        return collected_readings

    @staticmethod
    def _points_for_chunk(
        chunk_start_time: datetime,
        chunk_end_time: datetime,
        resolution_seconds: float,
    ) -> int:
        chunk_seconds = (chunk_end_time - chunk_start_time).total_seconds()
        requested_points = ceil(chunk_seconds / resolution_seconds)
        return max(1, min(requested_points, MAX_READINGS_PER_REQUEST))

    def _fetch_readings_chunk(
        self,
        site_id: Optional[str],
        chunk_start_time: datetime,
        chunk_end_time: datetime,
        requested_points: int,
    ) -> list[EnergyReading]:
        query_parameters: dict[str, Any] = {
            "start_time": chunk_start_time.isoformat(),
            "end_time": chunk_end_time.isoformat(),
            "limit": requested_points,
        }
        if site_id is not None:
            query_parameters["site_id"] = site_id

        reading_payloads = self._http_client.get_json(
            "/api/v1/readings",
            query_parameters=query_parameters,
            site_id=site_id,
        )
        return [EnergyReading.model_validate(payload) for payload in reading_payloads]
