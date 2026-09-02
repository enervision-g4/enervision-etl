from typing import Optional


class MockApiError(Exception):
    pass


class SiteNotFoundError(MockApiError):
    # HTTP 404 : la liste SITES de la configuration ne correspond plus au parc expose par l'API.
    def __init__(self, site_id: str) -> None:
        super().__init__(f"Site {site_id!r} is unknown to the mock API")
        self.site_id = site_id


class InvalidRequestParameterError(MockApiError):
    # HTTP 422 : la requete construite par le collecteur est invalide, c'est un defaut de code
    # et non un incident d'exploitation. La rejouer serait inutile.
    def __init__(self, endpoint: str, detail: str) -> None:
        super().__init__(f"Mock API rejected the parameters sent to {endpoint}: {detail}")
        self.endpoint = endpoint
        self.detail = detail


class MockApiUnavailableError(MockApiError):
    def __init__(self, endpoint: str, cause: Optional[str] = None) -> None:
        super().__init__(
            f"Mock API did not answer for {endpoint} after exhausting the retry budget"
            + (f": {cause}" if cause else "")
        )
        self.endpoint = endpoint
        self.cause = cause
