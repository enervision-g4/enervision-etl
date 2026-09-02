"""Interface de publication des messages.

Le collecteur depend de ce protocole, jamais d'un client de messagerie concret. Trois
consequences.

La chaine complete se demontre et se teste sans broker, en branchant une implementation
qui ecrit sur un flux texte. Le developpement n'est donc pas suspendu a la mise a
disposition de l'infrastructure.

Les tests d'orchestration n'ont besoin d'aucun service externe, ce qui les garde rapides
et deterministes.

Et le jour ou le transport change, seule l'implementation bouge.
"""

from types import TracebackType
from typing import Any, Optional, Protocol, runtime_checkable

from enervision_contracts.envelope import MessageEnvelope

DEFAULT_FLUSH_TIMEOUT_SECONDS = 10.0


@runtime_checkable
class MessagePublisher(Protocol):
    """Contrat commun a toutes les destinations de publication."""

    def publish(self, topic: str, envelope: MessageEnvelope[Any]) -> None:
        """Met un message en route vers un topic.

        La cle de partition n'est pas un parametre : elle est portee par l'enveloppe,
        ce qui interdit qu'un appelant publie une mesure sous une cle etrangere a son
        site et brise l'ordre chronologique de la partition.

        Args:
            topic: Nom du topic de destination.
            envelope: Message a publier.
        """
        ...

    def flush(self, timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS) -> int:
        """Attend la remise effective des messages en attente.

        Args:
            timeout_seconds: Delai maximal d'attente.

        Returns:
            Le nombre de messages encore en attente a l'expiration du delai. Zero
            signifie que tout a ete remis.
        """
        ...

    def close(self) -> None:
        """Libere les ressources, apres avoir vide les messages en attente."""
        ...

    def __enter__(self) -> "MessagePublisher":
        """Entre dans le bloc de contexte."""
        ...

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Ferme la destination a la sortie du bloc de contexte."""
        ...
