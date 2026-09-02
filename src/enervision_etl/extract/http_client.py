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
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        session: Optional[requests.Session] = None,
        total_retries: int = DEFAULT_TOTAL_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    ) -> None:
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
        # Le corps JSON de l'API mock est structurellement dynamique : le typage fort est
        # applique juste apres, par les modeles Pydantic du module contracts.
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

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "ResilientHttpClient":
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        self.close()
