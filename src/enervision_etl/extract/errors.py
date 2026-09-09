"""Exceptions de la couche d'extraction.

Chaque code HTTP designe une action corrective differente : les distinguer evite de
traiter un defaut de programmation comme un incident d'exploitation.
"""

from typing import Optional


class MockApiError(Exception):
    """Classe de base de toutes les erreurs remontees par le client de l'API mock."""


class SiteNotFoundError(MockApiError):
    """Site inconnu de l'API, reponse HTTP 404 : la configuration SITES est perimee.

    Attributes:
        site_id: Identifiant du site absent.
    """

    def __init__(self, site_id: str) -> None:
        """Construit l'erreur pour un identifiant de site donne.

        Args:
            site_id: Identifiant du site que l'API ne connait pas.
        """
        super().__init__(f"Site {site_id!r} is unknown to the mock API")
        self.site_id = site_id


class InvalidRequestParameterError(MockApiError):
    """Requete invalide, reponse HTTP 422 : defaut de code, inutile de la rejouer.

    Attributes:
        endpoint: Chemin de l'endpoint appele.
        detail: Corps de la reponse d'erreur renvoyee par l'API.
    """

    def __init__(self, endpoint: str, detail: str) -> None:
        """Construit l'erreur pour un endpoint et un motif de rejet donnes.

        Args:
            endpoint: Chemin de l'endpoint appele.
            detail: Explication du rejet fournie par l'API.
        """
        super().__init__(f"Mock API rejected the parameters sent to {endpoint}: {detail}")
        self.endpoint = endpoint
        self.detail = detail


class MockApiUnavailableError(MockApiError):
    """API injoignable apres epuisement du rejeu : 5xx, delai depasse ou connexion.

    Attributes:
        endpoint: Chemin de l'endpoint appele.
        cause: Description technique de la panne, si elle est connue.
    """

    def __init__(self, endpoint: str, cause: Optional[str] = None) -> None:
        """Construit l'erreur pour un endpoint injoignable.

        Args:
            endpoint: Chemin de l'endpoint appele.
            cause: Description technique de la panne, facultative.
        """
        super().__init__(
            f"Mock API did not answer for {endpoint} after exhausting the retry budget"
            + (f": {cause}" if cause else "")
        )
        self.endpoint = endpoint
        self.cause = cause


class WindowTooLargeError(MockApiError):
    """La periode demandee depasse ce que le decoupage peut couvrir.

    Renvoyer une serie tronquee serait pire qu'un echec : l'aval la prendrait pour
    l'historique complet et les mesures manquantes passeraient inapercues.

    Attributes:
        requested_hours: Duree demandee, en heures.
        coverable_hours: Duree couvrable a la resolution demandee, en heures.
    """

    def __init__(self, requested_hours: float, coverable_hours: float) -> None:
        """Construit l'erreur pour une periode trop longue.

        Args:
            requested_hours: Duree demandee, en heures.
            coverable_hours: Duree couvrable a la resolution demandee, en heures.
        """
        super().__init__(
            f"requested window of {requested_hours:.1f} h exceeds the {coverable_hours:.1f} h "
            "coverable at this resolution: narrow the window or coarsen the resolution"
        )
        self.requested_hours = requested_hours
        self.coverable_hours = coverable_hours
