from collections.abc import Sequence
from types import TracebackType
from typing import Any, Optional

import pytest


class FakeCursor:
    """Curseur en memoire qui enregistre ce que le depot envoie a la base.

    Il ne simule pas Postgres : les semantiques de ON CONFLICT et des cles etrangeres
    sont verifiees contre une vraie base, pas ici. Il sert a observer la requete emise
    et a scenariser une erreur du pilote.
    """

    def __init__(
        self,
        failure: Optional[Exception] = None,
        fetched_row: Optional[tuple[Any, ...]] = None,
    ) -> None:
        self.statements: list[str] = []
        self.parameters: list[Sequence[Any]] = []
        self._failure = failure
        self.fetched_row = fetched_row

    def execute(self, statement: str, parameters: Optional[Sequence[Any]] = None) -> None:
        self.statements.append(statement)
        self.parameters.append(parameters if parameters is not None else ())
        if self._failure is not None:
            raise self._failure

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        return self.fetched_row

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        return None


class FakeConnection:
    """Connexion en memoire, qui rend toujours le meme curseur observable."""

    def __init__(
        self,
        failure: Optional[Exception] = None,
        fetched_row: Optional[tuple[Any, ...]] = None,
    ) -> None:
        self.opened_cursor = FakeCursor(failure, fetched_row)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.opened_cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def connection() -> FakeConnection:
    return FakeConnection()


@pytest.fixture
def failing_connection() -> Any:
    def build(failure: Exception) -> FakeConnection:
        return FakeConnection(failure)

    return build


@pytest.fixture
def connection_returning() -> Any:
    def build(fetched_row: Optional[tuple[Any, ...]]) -> FakeConnection:
        return FakeConnection(fetched_row=fetched_row)

    return build
