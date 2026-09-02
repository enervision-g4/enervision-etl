from datetime import datetime, timedelta
from typing import Any, Final, Optional

from ..contracts.energy_reading import EnergyReading
from ..contracts.site import Site
from .errors import MockApiError
from .http_client import ResilientHttpClient

# Plafond impose par la documentation de l'endpoint /api/v1/readings.
MAX_READINGS_PER_REQUEST: Final[int] = 1000

# Garde fou contre une API qui renverrait indefiniment la meme fenetre.
MAX_PAGES_PER_WINDOW: Final[int] = 500


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
        page_size: int = MAX_READINGS_PER_REQUEST,
    ) -> list[EnergyReading]:
        # Ces deux controles evitent d'emettre une requete dont on sait deja qu'elle
        # provoquerait un 422. Une erreur de programmation doit echouer sans charger l'API.
        if not 1 <= page_size <= MAX_READINGS_PER_REQUEST:
            raise ValueError(
                f"page_size must be between 1 and {MAX_READINGS_PER_REQUEST}, received {page_size}"
            )
        if start_time > end_time:
            raise ValueError(
                f"start_time {start_time.isoformat()} is after end_time {end_time.isoformat()}"
            )

        collected_readings: list[EnergyReading] = []
        already_collected_timestamps: set[datetime] = set()
        page_start_time = start_time

        for _ in range(MAX_PAGES_PER_WINDOW):
            page = self._fetch_readings_page(site_id, page_start_time, end_time, page_size)
            if not page:
                break

            unseen_readings = [
                reading
                for reading in page
                if reading.timestamp not in already_collected_timestamps
            ]
            if not unseen_readings:
                break

            collected_readings.extend(unseen_readings)
            already_collected_timestamps.update(reading.timestamp for reading in unseen_readings)

            if len(page) < page_size:
                break

            page_start_time = max(reading.timestamp for reading in page) + timedelta(
                microseconds=1
            )
            if page_start_time > end_time:
                break

        collected_readings.sort(key=lambda reading: reading.timestamp)
        return collected_readings

    def _fetch_readings_page(
        self,
        site_id: Optional[str],
        page_start_time: datetime,
        end_time: datetime,
        page_size: int,
    ) -> list[EnergyReading]:
        query_parameters: dict[str, Any] = {
            "start_time": page_start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "limit": page_size,
        }
        if site_id is not None:
            query_parameters["site_id"] = site_id

        reading_payloads = self._http_client.get_json(
            "/api/v1/readings",
            query_parameters=query_parameters,
            site_id=site_id,
        )
        return [EnergyReading.model_validate(payload) for payload in reading_payloads]
