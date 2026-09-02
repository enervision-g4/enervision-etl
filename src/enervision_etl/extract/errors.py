"""Exceptions de la couche d'extraction.

Chaque code d'erreur HTTP renvoye par l'API mock designe un probleme de nature
differente, donc une action corrective differente. Les distinguer par des types
d'exception distincts evite de traiter un defaut de programmation comme un
incident d'exploitation.
"""

from typing import Optional


class MockApiError(Exception):
    """Classe de base de toutes les erreurs remontees par le client de l'API mock."""


class SiteNotFoundError(MockApiError):
    """Le site demande est inconnu de l'API mock, reponse HTTP 404.

    Signale que la variable de configuration SITES ne correspond plus au parc
    reellement expose par l'API.

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
    """La requete emise est invalide, reponse HTTP 422.

    Signale un defaut de construction de la requete cote collecteur, et non un
    incident d'exploitation. La rejouer serait donc inutile.

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
    """L'API mock n'a pas repondu apres epuisement du budget de rejeu.

    Couvre les erreurs serveur 5xx, les delais d'attente depasses et les echecs
    de connexion.

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
