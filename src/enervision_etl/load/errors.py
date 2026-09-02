"""Exceptions de la couche de publication."""


class MessagePublicationError(Exception):
    """Un message n'a pas pu etre remis au bus.

    Levee lorsque la file locale du producer reste saturee malgre une tentative de
    vidage. Echouer bruyamment est preferable a un message perdu en silence : l'appelant
    peut alors ralentir, alerter, ou interrompre la collecte.

    Attributes:
        topic: Topic de destination du message refuse.
        partition_key: Cle de partition du message refuse.
    """

    def __init__(self, topic: str, partition_key: str, cause: str) -> None:
        """Construit l'erreur pour un message refuse.

        Args:
            topic: Topic de destination.
            partition_key: Cle de partition du message.
            cause: Description technique du refus.
        """
        super().__init__(
            f"message for key {partition_key!r} could not be queued on {topic}: {cause}"
        )
        self.topic = topic
        self.partition_key = partition_key
