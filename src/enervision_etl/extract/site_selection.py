"""Determination des sites reellement collectes.

La liste des sites vit dans l'API, pas dans la configuration. Enumerer chaque site
dans une variable d'environnement dupliquerait cette information et deviendrait
ingerable au dela de quelques dizaines de sites. La configuration ne sert donc qu'a
restreindre la collecte, jamais a la definir.
"""

from enervision_contracts.site import Site

from .errors import MockApiError


class UnknownConfiguredSiteError(MockApiError):
    """La configuration demande des sites que l'API n'expose pas.

    Attributes:
        unknown_site_ids: Identifiants configures mais absents du referentiel.
    """

    def __init__(self, unknown_site_ids: list[str]) -> None:
        """Construit l'erreur pour les identifiants introuvables.

        Args:
            unknown_site_ids: Identifiants configures mais absents du referentiel.
        """
        super().__init__(
            "SITES refers to identifiers the mock API does not expose: "
            + ", ".join(unknown_site_ids)
        )
        self.unknown_site_ids = unknown_site_ids


def resolve_site_identifiers(
    configured_sites: list[str],
    site_registry: list[Site],
) -> list[str]:
    """Croise la configuration et le referentiel pour obtenir les sites a collecter.

    Args:
        configured_sites: Restriction demandee en configuration. Une liste vide
            demande la collecte de tout le parc.
        site_registry: Referentiel renvoye par l'API mock.

    Returns:
        Les identifiants a collecter, dans l'ordre du referentiel afin que la
        collecte ne depende pas de l'ordre de saisie en configuration.

    Raises:
        ValueError: Si le referentiel est vide, ce qui ferait tourner le service
            sans jamais rien collecter.
        UnknownConfiguredSiteError: Si la configuration reference un site absent
            du referentiel.
    """
    exposed_site_ids = [site.site_id for site in site_registry]
    if not exposed_site_ids:
        raise ValueError("the mock API exposes no site, there is nothing to collect")

    if not configured_sites:
        return exposed_site_ids

    exposed_by_normalized_id = {site_id.upper(): site_id for site_id in exposed_site_ids}
    requested_normalized_ids = {site_id.upper() for site_id in configured_sites}

    unknown_site_ids = sorted(requested_normalized_ids - exposed_by_normalized_id.keys())
    if unknown_site_ids:
        raise UnknownConfiguredSiteError(unknown_site_ids)

    return [
        site_id for site_id in exposed_site_ids if site_id.upper() in requested_normalized_ids
    ]
