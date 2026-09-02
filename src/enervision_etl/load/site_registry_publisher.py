"""Publication du referentiel des sites, c'est a dire de la liste du parc.

Le referentiel rassemble les caracteristiques fixes de chaque site, telles que renvoyees
par GET /api/v1/sites : type, nom, localisation, puissance installee, statut. Il alimente
la table SITE, vers laquelle pointe la cle etrangere de chaque mesure.

Ce n'est pas un flux d'evenements mais un etat courant : une poignee de sites qui
changent au mieux une fois par an. Le republier a chaque cycle du collecteur saturerait
le topic pour rien.

Deux mecanismes complementaires evitent ce gaspillage. Cote broker, le topic est cree
avec une politique de compaction : seul le dernier message par cle est conserve, donc
le journal reste de la taille du parc quel que soit le nombre de republications. Cette
propriete appartient au topic, pas au producteur ; elle doit etre appliquee a sa
creation. Cote collecteur, ce module ne publie que ce qui a reellement change, ce qui
ramene le trafic a zero en regime stable.

La compaction apporte un benefice supplementaire : un consumer qui demarre a vide peut
relire le topic depuis le debut et reconstituer l'etat complet du parc, sans dependre
du moment ou le collecteur republiera.
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
    """Diffuse la liste des sites en n'emettant que ce qui a change depuis la veille.

    Conserve en memoire la derniere version publiee de chaque site, et compare le
    referentiel entrant a cette photo pour n'emettre que les differences.
    """

    def __init__(
        self,
        publisher: MessagePublisher,
        topic: str,
        refresh_interval_seconds: float = DEFAULT_REFRESH_INTERVAL_SECONDS,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """Prepare la diffusion du referentiel.

        Args:
            publisher: Destination des messages.
            topic: Topic de la table SITE, a politique de compaction.
            refresh_interval_seconds: Delai entre deux verifications de la liste des sites.
            clock: Source de temps, injectee par les tests.

        Raises:
            ValueError: Si l'intervalle de rafraichissement n'est pas strictement positif.
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
            True tant qu'aucune diffusion n'a eu lieu, puis a chaque fois que
            l'intervalle configure s'est ecoule depuis la derniere.
        """
        if self._last_refreshed_at is None:
            return True

        elapsed_seconds = (self._clock() - self._last_refreshed_at).total_seconds()
        return elapsed_seconds >= self._refresh_interval_seconds

    def publish_changes(self, site_registry: list[Site]) -> list[str]:
        """Diffuse les sites nouveaux ou modifies, et eux seuls.

        Un site absent du referentiel n'est pas annule : le supprimer demanderait
        d'emettre un message a valeur nulle, que le contrat de publication ne prevoit
        pas. Un site mis hors service change de statut plutot que de disparaitre, cas
        qui est bien detecte. Un site disparu puis reapparu est simplement rediffuse.

        Args:
            site_registry: Referentiel complet tel que renvoye par l'API.

        Returns:
            Les identifiants des sites effectivement diffuses, dans l'ordre du
            referentiel.
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
