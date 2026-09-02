"""Publication de la liste des sites, qui alimente la table SITE.

Un etat courant, pas un flux : quelques sites qui changent au mieux une fois par an.
Deux mecanismes evitent de saturer le topic. Cote broker, la compaction ne conserve que
le dernier message par cle, propriete a appliquer a la creation du topic. Cote
collecteur, ce module ne publie que ce qui a change.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Optional

from enervision_contracts.envelope import SitePayload, envelope_for_site
from enervision_contracts.site import Site

from .publisher import MessagePublisher

DEFAULT_REFRESH_INTERVAL_SECONDS = 3600.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


class SiteRegistryPublisher:
    """Diffuse la liste des sites en n'emettant que ce qui a change.

    Conserve en memoire la derniere version publiee de chaque site.
    """

    def __init__(
        self,
        publisher: MessagePublisher,
        topic: str,
        refresh_interval_seconds: float = DEFAULT_REFRESH_INTERVAL_SECONDS,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """Prepare la diffusion.

        Args:
            publisher: Destination des messages.
            topic: Topic de la table SITE, a politique de compaction.
            refresh_interval_seconds: Delai entre deux verifications de la liste.
            clock: Source de temps, injectee par les tests.

        Raises:
            ValueError: Si l'intervalle n'est pas strictement positif.
        """
        if refresh_interval_seconds <= 0:
            raise ValueError(
                "refresh_interval_seconds must be strictly positive, "
                f"received {refresh_interval_seconds}"
            )

        self._publisher = publisher
        self._topic = topic
        self._refresh_interval_seconds = refresh_interval_seconds
        self._clock = clock if clock is not None else _utc_now
        self._last_published_payloads: dict[str, SitePayload] = {}
        self._last_refreshed_at: Optional[datetime] = None

    def is_refresh_due(self) -> bool:
        """Indique s'il est temps de redemander la liste des sites a l'API.

        Returns:
            True avant toute diffusion, puis a chaque intervalle ecoule.
        """
        if self._last_refreshed_at is None:
            return True

        elapsed_seconds = (self._clock() - self._last_refreshed_at).total_seconds()
        return elapsed_seconds >= self._refresh_interval_seconds

    def publish_changes(self, site_registry: list[Site]) -> list[str]:
        """Diffuse les sites nouveaux ou modifies, et eux seuls.

        Un site disparu n'est pas annule, faute de message a valeur nulle dans le
        contrat. Le cas reel, une mise hors service, se traduit par un changement de
        statut, bien detecte.

        Args:
            site_registry: Liste complete telle que renvoyee par l'API.

        Returns:
            Les identifiants diffuses, dans l'ordre de la liste.
        """
        current_payloads = {
            site.site_id: envelope_for_site(site).payload for site in site_registry
        }

        published_site_ids: list[str] = []
        for site in site_registry:
            payload = current_payloads[site.site_id]
            if self._last_published_payloads.get(site.site_id) == payload:
                continue
            self._publisher.publish(self._topic, envelope_for_site(site))
            published_site_ids.append(site.site_id)

        self._last_published_payloads = current_payloads
        self._last_refreshed_at = self._clock()
        return published_site_ids
