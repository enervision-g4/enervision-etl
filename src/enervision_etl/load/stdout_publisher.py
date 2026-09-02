"""Publication sur un flux texte, une ligne JSON par message.

Destination de developpement et de demonstration. Elle reproduit fidelement ce que
Kafka transporte, a savoir un topic, une cle de partition et une valeur, ce qui permet
deux choses : faire tourner la chaine complete sans broker, et rediriger la sortie dans
un fichier qui servira de jeu d'essai realiste aux consumers.
"""

import json
import sys
from collections import Counter
from types import TracebackType
from typing import Any, Optional, TextIO

from enervision_contracts.envelope import MessageEnvelope

from .publisher import DEFAULT_FLUSH_TIMEOUT_SECONDS


class StdoutPublisher:
    """Ecrit chaque message sous forme de ligne JSON autonome."""

    def __init__(self, stream: Optional[TextIO] = None) -> None:
        """Prepare la destination.

        Args:
            stream: Flux d'ecriture. La sortie standard par defaut. Le flux n'est
                jamais ferme par cette classe, qui ne le possede pas.
        """
        self._stream = stream if stream is not None else sys.stdout
        self._published_counts: Counter[str] = Counter()

    @property
    def published_counts(self) -> dict[str, int]:
        """Nombre de messages ecrits, par topic."""
        return dict(self._published_counts)

    def publish(self, topic: str, envelope: MessageEnvelope[Any]) -> None:
        """Ecrit un message sur le flux.

        Args:
            topic: Nom du topic de destination.
            envelope: Message a publier.
        """
        record = {
            "topic": topic,
            "key": envelope.partition_key,
            "value": json.loads(envelope.model_dump_json()),
        }
        self._stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._published_counts[topic] += 1

    def flush(self, timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS) -> int:
        """Vide le tampon du flux.

        Args:
            timeout_seconds: Ignore, l'ecriture etant synchrone.

        Returns:
            Toujours zero : aucun message ne peut rester en attente.
        """
        self._stream.flush()
        return 0

    def close(self) -> None:
        """Vide le tampon sans fermer le flux sous-jacent."""
        self.flush()

    def __enter__(self) -> "StdoutPublisher":
        """Entre dans le bloc de contexte et renvoie la destination."""
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Vide le tampon a la sortie du bloc de contexte.

        Args:
            exception_type: Type de l'exception ayant interrompu le bloc, si elle existe.
            exception_value: Instance de cette exception.
            exception_traceback: Pile d'appels associee.
        """
        self.close()
