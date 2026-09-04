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
        failing_attempts: Optional[int] = None,
    ) -> None:
        self.statements: list[str] = []
        self.parameters: list[Sequence[Any]] = []
        self._failure = failure
        self._remaining_failures = failing_attempts
        self.fetched_row = fetched_row

    def execute(self, statement: str, parameters: Optional[Sequence[Any]] = None) -> None:
        self.statements.append(statement)
        self.parameters.append(parameters if parameters is not None else ())
        if self._failure is None:
            return
        # failing_attempts a None fait echouer indefiniment, un entier fait echouer ce
        # nombre de fois puis laisse passer : de quoi rejouer un message apres remede.
        if self._remaining_failures is None:
            raise self._failure
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
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
        journal: Optional[list[str]] = None,
        failing_attempts: Optional[int] = None,
    ) -> None:
        self.opened_cursor = FakeCursor(failure, fetched_row, failing_attempts)
        self.journal = journal if journal is not None else []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.opened_cursor

    def commit(self) -> None:
        self.commits += 1
        self.journal.append("base")

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


@pytest.fixture
def journalled_connection() -> Any:
    def build(journal: list[str], fetched_row: Optional[tuple[Any, ...]] = None) -> FakeConnection:
        return FakeConnection(fetched_row=fetched_row, journal=journal)

    return build


@pytest.fixture
def recovering_connection() -> Any:
    def build(
        failure: Exception,
        failing_attempts: int,
        journal: Optional[list[str]] = None,
    ) -> FakeConnection:
        return FakeConnection(
            failure=failure, journal=journal, failing_attempts=failing_attempts
        )

    return build
