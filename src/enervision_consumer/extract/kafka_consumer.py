"""Acces au bus de messages cote consumer.

Deux comportements du client confluent_kafka structurent cette configuration, et les
laisser par defaut perd des messages en silence. L'acquittement automatique avance
l'offset sans savoir si l'ecriture en base a eu lieu. Et un groupe qui demarre reprend
par defaut a la fin du topic, donc ignore tout l'historique deja publie.
"""

from typing import Any, Optional, Protocol, cast


class ConsumedMessage(Protocol):
    """Sous ensemble de confluent_kafka.Message reellement utilise ici."""

    def topic(self) -> str:
        """Renvoie le topic d'origine, qui determine le type du payload attendu."""
        ...

    def value(self) -> Optional[bytes]:
        """Renvoie l'enveloppe serialisee, ou None pour un message sans corps."""
        ...

    def error(self) -> Optional[object]:
        """Renvoie l'erreur portee par le message, ou None s'il est exploitable."""
        ...


class ConsumerLike(Protocol):
    """Sous ensemble de confluent_kafka.Consumer reellement utilise ici."""

    def subscribe(self, topics: list[str]) -> None:
        """Abonne le consumer aux topics demandes."""
        ...

    def poll(self, timeout: float = 0) -> Optional[ConsumedMessage]:
        """Rend le prochain message disponible, ou None si le delai expire."""
        ...

    def commit(
        self,
        message: Optional[ConsumedMessage] = None,
        asynchronous: bool = True,
    ) -> None:
        """Acquitte la position de lecture, jusqu'au message inclus."""
        ...

    def close(self) -> None:
        """Quitte le groupe et relache les partitions."""
        ...


def build_consumer_configuration(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    """Assemble la configuration du consumer.

    Args:
        bootstrap_servers: Liste des brokers, separes par des virgules.
        group_id: Identifiant du consumer group, propre a chaque service.

    Returns:
        La configuration a passer au consumer.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        # L'offset n'avance qu'apres une ecriture reussie en base, jamais avant.
        "enable.auto.commit": False,
        # Un groupe qui demarre doit reprendre l'historique, pas le sauter.
        "auto.offset.reset": "earliest",
        "client.id": group_id,
    }


def create_consumer(bootstrap_servers: str, group_id: str) -> ConsumerLike:
    """Construit un consumer confluent_kafka.

    Args:
        bootstrap_servers: Liste des brokers, separes par des virgules.
        group_id: Identifiant du consumer group.

    Returns:
        Le consumer configure, pret a s'abonner.

    Raises:
        ValueError: Si aucune adresse de broker n'est fournie.
    """
    if not bootstrap_servers:
        raise ValueError("bootstrap_servers is required to reach the message bus")

    from confluent_kafka import Consumer

    return cast(ConsumerLike, Consumer(build_consumer_configuration(bootstrap_servers, group_id)))
