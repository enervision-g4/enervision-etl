"""Interface de publication des messages.

Le collecteur depend de ce protocole, jamais d'un client de messagerie concret : la
chaine se teste et se demontre sans broker, en branchant une destination sur flux texte.
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

        La cle de partition est portee par l'enveloppe, jamais passee en parametre :
        publier sous une cle etrangere au site briserait l'ordre de la partition.

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
            Le nombre de messages encore en attente, zero si tout a ete remis.
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
