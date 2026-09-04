"""Boucle de consommation commune aux deux consumers.

Elle porte les regles qui ne doivent exister qu'a un seul endroit : le referentiel
draine avant les faits qui le referencent, la transaction validee avant l'acquittement
de l'offset, et la reprise sur un site encore inconnu. Les dupliquer par consumer
reviendrait a les laisser diverger.

Le client Kafka rend aussi ses evenements d'erreur par le meme poll que les messages,
un topic pas encore connu du broker par exemple. Seul error() les distingue, et leur
value() porte le texte de l'erreur : les traiter comme des donnees ferait echouer le
decodage sur ce qui n'est qu'un avertissement transitoire.
"""

from collections.abc import Callable, Mapping
from typing import Optional

from ..extract.kafka_consumer import ConsumedMessage, ConsumerLike
from ..load.errors import UnknownSiteReferenceError
from ..load.postgres_connection import ConnectionLike
from ..logging_setup import get_logger

logger = get_logger("consumption_loop")

DEFAULT_POLL_TIMEOUT_SECONDS = 1.0
"""Attente maximale d'un message avant de rendre la main a la boucle."""

MessageHandler = Callable[[ConsumedMessage], None]
"""Ecriture d'un message dans sa table, propre a chaque topic."""


class ConsumptionLoop:
    """Lecture du bus, ecriture en base et acquittement, dans cet ordre."""

    def __init__(
        self,
        consumer: ConsumerLike,
        connection: ConnectionLike,
        refresh_site_registry: Callable[[], int],
        handlers: Mapping[str, MessageHandler],
    ) -> None:
        """Prepare la boucle.

        Args:
            consumer: Acces au bus de messages.
            connection: Connexion vers la base, en validation manuelle.
            refresh_site_registry: Relit le referentiel et l'applique en base, en
                rendant le nombre de sites appliques.
            handlers: Ecriture a appliquer, par topic consomme.
        """
        self._consumer = consumer
        self._connection = connection
        self._refresh_site_registry = refresh_site_registry
        self._handlers = dict(handlers)

    @property
    def consumed_topics(self) -> list[str]:
        """Topics dont cette boucle a la charge."""
        return list(self._handlers)

    def run(
        self,
        max_messages: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    ) -> int:
        """Consomme les messages disponibles et les ecrit en base.

        Args:
            max_messages: Nombre de messages a traiter, illimite si None.
            should_stop: Consulte avant chaque lecture pour interrompre la boucle.
            poll_timeout_seconds: Attente maximale d'un message.

        Returns:
            Le nombre de messages traites et acquittes.

        Raises:
            UnknownSiteReferenceError: Si un site reste introuvable apres redrainage.
            PersistenceError: Si une ecriture echoue pour une autre raison.
            ValueError: Si un message arrive d'un topic non pris en charge.
        """
        # Le referentiel precede les faits qui le referencent : les topics ne sont pas
        # ordonnes entre eux et site_id est une cle etrangere.
        self._refresh_site_registry()
        self._consumer.subscribe(self.consumed_topics)
        handled = 0

        while max_messages is None or handled < max_messages:
            if should_stop is not None and should_stop():
                break

            message = self._consumer.poll(poll_timeout_seconds)
            if message is None:
                continue

            broker_error = message.error()
            if broker_error is not None:
                # Rien a ecrire ni a acquitter : il n'y a pas de donnee derriere.
                logger.warning("broker_event_ignored", cause=str(broker_error))
                continue

            self._persist(message)
            handled += 1

        return handled

    def close(self) -> None:
        """Quitte le groupe et referme la connexion."""
        self._consumer.close()
        self._connection.close()

    def _persist(self, message: ConsumedMessage) -> None:
        """Ecrit un message puis acquitte sa position, dans cet ordre.

        Un site inconnu n'est pas une donnee invalide mais une course : sa fiche peut
        etre encore en route. Le referentiel est donc redraine et le message rejoue une
        fois. S'il echoue encore, l'exception remonte sans que l'offset soit acquitte,
        et le message reviendra au redemarrage.

        Args:
            message: Message lu sur le bus.

        Raises:
            UnknownSiteReferenceError: Si le site reste introuvable apres redrainage.
            ValueError: Si le topic du message n'est pas pris en charge.
        """
        handler = self._handlers.get(message.topic())
        if handler is None:
            raise ValueError(f"topic {message.topic()!r} is not handled by this consumer")

        try:
            handler(message)
        except UnknownSiteReferenceError:
            self._connection.rollback()
            self._refresh_site_registry()
            handler(message)

        self._connection.commit()
        self._consumer.commit(message=message, asynchronous=False)
