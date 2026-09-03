"""Client HTTP resilient pour l'API mock.

Session reutilisee, delai d'attente explicite sur chaque requete, rejeu limite aux
pannes transitoires, et traduction des codes HTTP en exceptions metier distinctes.

Un espacement minimal entre requetes peut etre impose : l'instance mock se degrade
lorsqu'on l'interroge en rafale, et renvoie alors des series entierement nulles qu'on
prendrait a tort pour des pannes de capteurs.
"""

import time
from collections.abc import Callable
from types import TracebackType
from typing import Any, Final, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .errors import (
    InvalidRequestParameterError,
    MockApiUnavailableError,
    SiteNotFoundError,
)

# Seules les pannes transitoires sont rejouees. Un 404 ou un 422 sont deterministes :
# les rejouer ne ferait que multiplier la charge sans jamais changer la reponse.
RETRYABLE_STATUS_CODES: Final[tuple[int, ...]] = (500, 502, 503, 504)

DEFAULT_TOTAL_RETRIES: Final[int] = 3
DEFAULT_BACKOFF_FACTOR: Final[float] = 0.5


def build_http_session(
    total_retries: int = DEFAULT_TOTAL_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
) -> requests.Session:
    """Construit une session HTTP dotee d'une politique de rejeu.

    La session est reutilisee, ce qui evite une poignee de main TCP par mesure.

    Args:
        total_retries: Nombre maximal de tentatives supplementaires par requete.
        backoff_factor: Facteur de la temporisation exponentielle.

    Returns:
        Une session montee sur les schemas http et https.
    """
    retry_policy = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    session = requests.Session()
    retrying_adapter = HTTPAdapter(max_retries=retry_policy)
    session.mount("http://", retrying_adapter)
    session.mount("https://", retrying_adapter)
    return session


class ResilientHttpClient:
    """Effectue des appels GET sur l'API mock et traduit ses codes d'erreur."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        session: Optional[requests.Session] = None,
        total_retries: int = DEFAULT_TOTAL_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        minimum_interval_seconds: float = 0.0,
        monotonic: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        """Prepare le client pour une instance donnee de l'API mock.

        Args:
            base_url: Racine de l'API.
            timeout_seconds: Delai d'attente applique a chaque requete.
            session: Session a reutiliser, creee par defaut si omise.
            total_retries: Nombre maximal de tentatives supplementaires.
            backoff_factor: Facteur de la temporisation exponentielle.
            minimum_interval_seconds: Espacement minimal entre deux requetes. Zero
                pour n'imposer aucun rythme.
            monotonic: Source de temps monotone, injectee par les tests.
            sleep: Fonction d'attente, injectee par les tests.

        Raises:
            ValueError: Si minimum_interval_seconds est negatif.
        """
        if minimum_interval_seconds < 0:
            raise ValueError(
                "minimum_interval_seconds must not be negative, "
                f"received {minimum_interval_seconds}"
            )

        self._minimum_interval_seconds = minimum_interval_seconds
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep
        self._last_request_at: Optional[float] = None
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._session = session if session is not None else build_http_session(
            total_retries=total_retries,
            backoff_factor=backoff_factor,
        )

    def get_json(
        self,
        endpoint: str,
        query_parameters: Optional[dict[str, Any]] = None,
        site_id: Optional[str] = None,
    ) -> Any:  # noqa: ANN401
        """Appelle un endpoint en GET et renvoie son corps JSON deserialise.

        Une reponse 200 contenant des valeurs nulles est valide et n'est jamais filtree.

        Args:
            endpoint: Chemin de l'endpoint, commencant par une barre oblique.
            query_parameters: Parametres de requete, facultatifs.
            site_id: Site concerne, pour qualifier une erreur 404.

        Returns:
            Le corps de la reponse. Le typage fort est applique ensuite par contracts.

        Raises:
            SiteNotFoundError: Si l'API repond 404.
            InvalidRequestParameterError: Si l'API repond 422.
            MockApiUnavailableError: Si l'API repond 5xx ou reste injoignable.
        """
        self._wait_for_the_minimum_interval()
        try:
            response = self._session.get(
                f"{self._base_url}{endpoint}",
                params=query_parameters,
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as transport_failure:
            raise MockApiUnavailableError(endpoint, str(transport_failure)) from transport_failure

        if response.status_code == 404:
            raise SiteNotFoundError(site_id if site_id is not None else endpoint)
        if response.status_code == 422:
            raise InvalidRequestParameterError(endpoint, response.text)
        if response.status_code >= 500:
            raise MockApiUnavailableError(endpoint, f"HTTP {response.status_code}")

        response.raise_for_status()
        return response.json()

    def _wait_for_the_minimum_interval(self) -> None:
        """Espace la requete a venir de la precedente, si un rythme est impose."""
        if self._minimum_interval_seconds <= 0:
            return

        now = self._monotonic()
        if self._last_request_at is not None:
            remaining = self._minimum_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                self._sleep(remaining)
                now = self._monotonic()
        self._last_request_at = now

    def close(self) -> None:
        """Ferme la session HTTP et libere les connexions maintenues ouvertes."""
        self._session.close()

    def __enter__(self) -> "ResilientHttpClient":
        """Entre dans le bloc de contexte et renvoie le client lui meme."""
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Ferme la session a la sortie du bloc de contexte.

        Args:
            exception_type: Type de l'exception ayant interrompu le bloc.
            exception_value: Instance de cette exception.
            exception_traceback: Pile d'appels associee.
        """
        self.close()
