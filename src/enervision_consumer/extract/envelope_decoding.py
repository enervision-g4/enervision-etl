"""Relecture des enveloppes publiees sur le bus.

Le type du payload se deduit du topic : chaque topic porte le nom de la table qu'il
alimente et ne transporte donc qu'une seule sorte de message. Un message illisible est
signale, jamais ignore : l'aval ne peut pas savoir ce qu'il n'a pas recu.
"""

from typing import Optional

from pydantic import ValidationError

from enervision_contracts.envelope import MessageEnvelope, SiteScopedPayload


class EnvelopeDecodingError(Exception):
    """Un message n'a pas pu etre relu selon le contrat attendu pour son topic."""

    def __init__(self, topic: str, reason: str) -> None:
        """Rassemble le topic fautif et la cause du rejet.

        Args:
            topic: Topic d'ou provient le message.
            reason: Description technique de l'ecart au contrat.
        """
        super().__init__(f"message on topic {topic!r} could not be decoded: {reason}")
        self.topic = topic
        self.reason = reason


def decode_envelope[PayloadT: SiteScopedPayload](
    topic: str,
    raw_value: Optional[bytes],
    envelope_type: type[MessageEnvelope[PayloadT]],
) -> MessageEnvelope[PayloadT]:
    """Relit une enveloppe serialisee selon le contrat attendu pour son topic.

    Args:
        topic: Topic d'ou provient le message, qui determine envelope_type.
        raw_value: Enveloppe serialisee, telle que rendue par le client Kafka.
        envelope_type: Enveloppe attendue, parametree par le type de payload que ce
            topic transporte, par exemple MessageEnvelope[MeasureRawPayload].

    Returns:
        L'enveloppe relue, payload compris.

    Raises:
        EnvelopeDecodingError: Si le message est vide ou ne respecte pas le contrat.
    """
    if raw_value is None:
        # Une suppression sur un topic compacte se presente ainsi. Le collecteur n'en
        # produit pas, donc en recevoir une signale un producteur inattendu.
        raise EnvelopeDecodingError(topic, "message carries no body")

    try:
        return envelope_type.model_validate_json(raw_value)
    except ValidationError as invalid_message:
        raise EnvelopeDecodingError(topic, str(invalid_message)) from invalid_message
