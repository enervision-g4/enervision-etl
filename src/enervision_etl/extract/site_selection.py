"""Determination des sites collectes, en croisant la liste du parc et la configuration.

La liste vit dans l'API, pas dans l'environnement : la configuration ne sert qu'a
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
    """Croise la configuration et la liste du parc pour obtenir les sites a collecter.

    Args:
        configured_sites: Restriction demandee, vide pour tout le parc.
        site_registry: Liste des sites renvoyee par l'API mock.

    Returns:
        Les identifiants a collecter, dans l'ordre du parc et non de la configuration.

    Raises:
        ValueError: Si le parc est vide, le service tournerait sans rien collecter.
        UnknownConfiguredSiteError: Si un site configure est absent du parc.
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
