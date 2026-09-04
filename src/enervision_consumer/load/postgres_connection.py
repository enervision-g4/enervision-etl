"""Acces a la base, reduit au sous ensemble du pilote reellement utilise.

La connexion n'est jamais en validation automatique : l'offset Kafka ne s'acquitte
qu'apres un commit reussi, donc le moment du commit appartient a l'orchestration et a
elle seule.
"""

from collections.abc import Sequence
from types import TracebackType
from typing import Optional, Protocol, cast


class CursorLike(Protocol):
    """Sous ensemble du curseur psycopg reellement utilise ici."""

    def execute(self, statement: str, parameters: Optional[Sequence[object]] = None) -> None:
        """Envoie une requete parametree a la base."""
        ...

    def __enter__(self) -> "CursorLike":
        """Ouvre le bloc de contexte du curseur."""
        ...

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Referme le curseur a la sortie du bloc."""
        ...


class ConnectionLike(Protocol):
    """Sous ensemble de la connexion psycopg reellement utilisee ici."""

    def cursor(self) -> CursorLike:
        """Ouvre un curseur sur cette connexion."""
        ...

    def commit(self) -> None:
        """Valide la transaction en cours."""
        ...

    def rollback(self) -> None:
        """Annule la transaction en cours."""
        ...

    def close(self) -> None:
        """Ferme la connexion."""
        ...


def create_connection(database_url: str) -> ConnectionLike:
    """Ouvre une connexion a la base de destination.

    Args:
        database_url: URL de connexion, schema postgres:// ou postgresql://.

    Returns:
        La connexion, en validation manuelle.

    Raises:
        ValueError: Si aucune URL n'est fournie.
    """
    if not database_url:
        raise ValueError("database_url is required to reach the database")

    import psycopg

    # Explicite bien que ce soit le defaut du pilote : l'ordre commit puis acquittement
    # de l'offset est une regle du projet, pas un detail de configuration.
    return cast(ConnectionLike, psycopg.connect(database_url, autocommit=False))
