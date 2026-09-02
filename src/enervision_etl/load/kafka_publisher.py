"""Publication vers un broker Kafka.

Le broker lui meme est de l'infrastructure, declaree ailleurs. Ce module ne contient
que le client producteur, c'est a dire l'implementation Kafka du protocole de
publication.

Trois comportements du client confluent_kafka structurent ce code, et les ignorer
conduit a des pertes silencieuses.

La publication est asynchrone : produce met en file, il ne remet pas. Les callbacks de
livraison ne s'executent qu'a l'appel de poll ou de flush, sans quoi les echecs
n'atteignent jamais l'application.

La file locale a une capacite finie : produce leve BufferError quand elle est pleine.
Ignorer cette exception revient a jeter le message.

Enfin, tout ce qui reste en file a l'arret du processus est perdu. Le vidage explicite
a la fermeture n'est pas une precaution, c'est une obligation.
"""

from collections.abc import Callable
from types import TracebackType
from typing import Any, Optional, Protocol, cast

from enervision_contracts.envelope import MessageEnvelope

from .errors import MessagePublicationError
from .publisher import DEFAULT_FLUSH_TIMEOUT_SECONDS

QUEUE_DRAIN_TIMEOUT_SECONDS = 1.0


class DeliveredMessage(Protocol):
    """Sous ensemble de confluent_kafka.Message utilise par le callback de remise."""

    def topic(self) -> str:
        """Renvoie le topic sur lequel le message a ete remis."""
        ...


DeliveryCallback = Callable[[Optional[object], DeliveredMessage], None]
"""Signature du callback appele a l'issue de chaque tentative de remise."""


class ProducerLike(Protocol):
    """Sous ensemble de confluent_kafka.Producer reellement utilise ici."""

    def produce(
        self,
        topic: str,
        *,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        on_delivery: Optional[DeliveryCallback] = None,
    ) -> None:
        """Met un message dans la file locale du producer."""
        ...

    def poll(self, timeout: float = 0) -> int:
        """Sert les callbacks de livraison en attente."""
        ...

    def flush(self, timeout: float = 0) -> int:
        """Attend la remise des messages en file."""
        ...


def build_producer_configuration(bootstrap_servers: str) -> dict[str, Any]:
    """Assemble la configuration du producer.

    L'idempotence est activee : sans elle, un rejeu declenche par une coupure reseau
    republie le message et cree un doublon. Elle implique acks=all, qui est declare
    explicitement pour que l'intention reste lisible.

    Args:
        bootstrap_servers: Liste des brokers, separes par des virgules.

    Returns:
        La configuration a passer au producer.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "client.id": "enervision-collector",
    }


class KafkaPublisher:
    """Implementation Kafka du protocole de publication."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        producer: Optional[ProducerLike] = None,
    ) -> None:
        """Prepare la destination.

        Args:
            bootstrap_servers: Liste des brokers. Ignore si producer est fourni.
            producer: Producer deja construit, injecte par les tests.

        Raises:
            ValueError: Si ni bootstrap_servers ni producer ne sont fournis.
        """
        # Comparaison explicite a None plutot qu'un `or` : tout objet definissant
        # __len__ sans __bool__ est falsy lorsque sa file est vide, et un producer
        # injecte serait alors remplace par la construction par defaut.
        self._producer: ProducerLike = (
            producer if producer is not None else self._build_default_producer(bootstrap_servers)
        )
        self._delivered_counts: dict[str, int] = {}
        self._delivery_failures = 0

    @staticmethod
    def _build_default_producer(bootstrap_servers: Optional[str]) -> ProducerLike:
        """Construit un producer confluent_kafka a partir de la configuration.

        Args:
            bootstrap_servers: Liste des brokers, separes par des virgules.

        Returns:
            Le producer configure pour une remise idempotente.

        Raises:
            ValueError: Si aucune adresse de broker n'est fournie.
        """
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers is required when no producer is provided")

        from confluent_kafka import Producer

        return cast(
            ProducerLike, Producer(build_producer_configuration(bootstrap_servers))
        )

    @property
    def delivered_counts(self) -> dict[str, int]:
        """Nombre de messages effectivement remis, par topic."""
        return dict(self._delivered_counts)

    @property
    def delivery_failures(self) -> int:
        """Nombre de messages dont la remise a echoue."""
        return self._delivery_failures

    def publish(self, topic: str, envelope: MessageEnvelope[Any]) -> None:
        """Met un message en file vers un topic.

        Args:
            topic: Nom du topic de destination.
            envelope: Message a publier. Sa cle de partition determine la partition,
                donc l'ordre chronologique des mesures d'un meme site.

        Raises:
            MessagePublicationError: Si la file locale reste saturee apres un vidage.
        """
        partition_key = envelope.partition_key
        encoded_key = partition_key.encode("utf-8")
        encoded_value = envelope.model_dump_json().encode("utf-8")

        try:
            self._queue(topic, encoded_key, encoded_value)
        except BufferError:
            # La file locale est pleine. On sert les livraisons en attente pour la
            # liberer, puis on retente une fois avant de renoncer bruyamment.
            self._producer.poll(QUEUE_DRAIN_TIMEOUT_SECONDS)
            try:
                self._queue(topic, encoded_key, encoded_value)
            except BufferError as saturated_queue:
                raise MessagePublicationError(
                    topic, partition_key, str(saturated_queue)
                ) from saturated_queue

        # Sert les callbacks des messages precedents sans bloquer la collecte.
        self._producer.poll(0)

    def flush(self, timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS) -> int:
        """Attend la remise effective des messages en attente.

        Args:
            timeout_seconds: Delai maximal d'attente.

        Returns:
            Le nombre de messages encore en attente a l'expiration du delai.
        """
        return self._producer.flush(timeout_seconds)

    def close(self) -> None:
        """Vide la file avant de relacher le producer."""
        self.flush()

    def __enter__(self) -> "KafkaPublisher":
        """Entre dans le bloc de contexte et renvoie la destination."""
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Vide la file a la sortie du bloc de contexte.

        Args:
            exception_type: Type de l'exception ayant interrompu le bloc, si elle existe.
            exception_value: Instance de cette exception.
            exception_traceback: Pile d'appels associee.
        """
        self.close()

    def _queue(self, topic: str, encoded_key: bytes, encoded_value: bytes) -> None:
        self._producer.produce(
            topic,
            key=encoded_key,
            value=encoded_value,
            on_delivery=self._record_delivery,
        )

    def _record_delivery(self, error: Optional[object], message: DeliveredMessage) -> None:
        """Enregistre l'issue d'une remise.

        Sans ce callback, un echec de remise resterait invisible : le producer ne leve
        aucune exception au moment du produce, la publication etant asynchrone.

        Args:
            error: Erreur de remise, ou None en cas de succes.
            message: Message concerne, tel que rendu par le client.
        """
        if error is not None:
            self._delivery_failures += 1
            return

        topic = message.topic()
        self._delivered_counts[topic] = self._delivered_counts.get(topic, 0) + 1
