"""Client type de l'API mock : chaque endpoint rend des objets valides, pas du JSON.

Le comportement de /api/v1/readings encode ici a ete mesure sur une instance reelle,
voir fetch_readings_window.
"""

from datetime import datetime, timedelta
from math import ceil
from typing import Any, Final, Optional

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.site import Site

from .errors import MockApiError, WindowTooLargeError
from .http_client import ResilientHttpClient

# Plafond de /api/v1/readings, verifie sur l'instance : au dela, l'API repond 422.
MAX_READINGS_PER_REQUEST: Final[int] = 1000

# Resolution par defaut, alignee sur la periode de polling du collecteur temps reel.
DEFAULT_RESOLUTION_SECONDS: Final[float] = 60.0

# Borne le nombre de tranches. Une periode plus longue est refusee, jamais tronquee.
MAX_CHUNKS_PER_WINDOW: Final[int] = 500


class MockApiClient:
    """Acces type aux endpoints de l'API mock."""

    def __init__(self, http_client: ResilientHttpClient) -> None:
        """Associe le client a un transport HTTP deja configure.

        Args:
            http_client: Transport portant l'URL de base et la politique de rejeu.
        """
        self._http_client = http_client

    def is_healthy(self) -> bool:
        """Indique si l'API mock se declare operationnelle.

        Returns:
            True si /health repond healthy. Toute erreur vaut indisponibilite.
        """
        try:
            health_report = self._http_client.get_json("/health")
        except MockApiError:
            return False
        return bool(health_report.get("status") == "healthy")

    def fetch_site_registry(self) -> list[Site]:
        """Recupere la liste complete du parc.

        Returns:
            Tous les sites exposes, avec type, puissance installee et statut. Alimente
            SITE, le taux de charge et le choix de la strategie d'imputation.

        Raises:
            MockApiUnavailableError: Si l'API reste injoignable.
        """
        site_payloads = self._http_client.get_json("/api/v1/sites")
        return [Site.model_validate(payload) for payload in site_payloads]

    def fetch_site(self, site_id: str) -> Site:
        """Recupere les caracteristiques d'un site donne.

        Args:
            site_id: Identifiant metier du site.

        Returns:
            Le site correspondant.

        Raises:
            SiteNotFoundError: Si le site est inconnu de l'API.
        """
        site_payload = self._http_client.get_json(
            f"/api/v1/sites/{site_id}",
            site_id=site_id,
        )
        return Site.model_validate(site_payload)

    def fetch_current_reading(self, site_id: str) -> EnergyReading:
        """Recupere la mesure instantanee d'un site, endpoint du collecteur temps reel.

        Une mesure partielle ou integralement nulle est un resultat valide.

        Args:
            site_id: Identifiant metier du site.

        Returns:
            Le releve courant, valeurs nulles comprises.

        Raises:
            SiteNotFoundError: Si le site est inconnu de l'API.
        """
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
        """Recupere l'historique simule d'un site, a la resolution demandee.

        Le parametre limit n'est pas une taille de page mais un nombre de points
        repartis dans la fenetre, et l'API regenere la serie a chaque appel : la
        pagination par curseur est donc impossible. La periode est decoupee en tranches
        jointives, chacune echantillonnee a la resolution voulue.

        Args:
            site_id: Identifiant du site, ou None pour interroger tout le parc.
            start_time: Debut de la periode, inclus.
            end_time: Fin de la periode.
            resolution_seconds: Ecart souhaite entre deux mesures consecutives.

        Returns:
            Les mesures de la periode, dedoublonnees par horodatage et triees par
            ordre chronologique croissant.

        Raises:
            ValueError: Si resolution_seconds n'est pas strictement positif, ou si
                start_time est posterieur a end_time.
            WindowTooLargeError: Si la periode depasse ce que le decoupage couvre.
            SiteNotFoundError: Si le site est inconnu de l'API.
        """
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

        fetched_chunks = 0
        while chunk_start_time < end_time:
            if fetched_chunks >= MAX_CHUNKS_PER_WINDOW:
                # Rendre un historique tronque serait pire qu'un echec : l'aval le
                # prendrait pour complet et les mesures manquantes passeraient inapercues.
                raise WindowTooLargeError(
                    requested_hours=(end_time - start_time).total_seconds() / 3600,
                    coverable_hours=(
                        chunk_duration.total_seconds() * MAX_CHUNKS_PER_WINDOW / 3600
                    ),
                )

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
            fetched_chunks += 1

        collected_readings.sort(key=lambda reading: reading.timestamp)
        return collected_readings

    @staticmethod
    def _points_for_chunk(
        chunk_start_time: datetime,
        chunk_end_time: datetime,
        resolution_seconds: float,
    ) -> int:
        """Determine le nombre de points a demander pour une tranche.

        Args:
            chunk_start_time: Debut de la tranche.
            chunk_end_time: Fin de la tranche.
            resolution_seconds: Ecart souhaite entre deux mesures.

        Returns:
            Le nombre de points, borne entre un et le plafond de l'API.
        """
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
        """Interroge /api/v1/readings pour une tranche unique.

        Args:
            site_id: Identifiant du site, ou None pour tout le parc.
            chunk_start_time: Debut de la tranche.
            chunk_end_time: Fin de la tranche.
            requested_points: Nombre de mesures demandees dans cette tranche.

        Returns:
            Les mesures renvoyees par l'API pour cette tranche.
        """
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
