"""Consumer de persistance : ecrit dans la base ce que le collecteur a publie.

L'ordre des deux validations est la regle qui protege de la perte silencieuse. La
transaction est validee d'abord, l'offset Kafka acquitte ensuite. L'inverse perdrait un
message si le processus tombait entre les deux, sans que rien ne le signale.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

from enervision_contracts.envelope import (
    MeasureImputedPayload,
    MeasureRawPayload,
    MessageEnvelope,
    SitePayload,
)

from ..extract.envelope_decoding import decode_envelope
from ..extract.kafka_consumer import ConsumedMessage, ConsumerLike
from ..load import measure_imputed_repository, measure_raw_repository
from ..load.errors import UnknownSiteReferenceError
from ..load.postgres_connection import ConnectionLike
from ..load.site_repository import upsert_site

DEFAULT_POLL_TIMEOUT_SECONDS = 1.0
"""Attente maximale d'un message avant de rendre la main a la boucle."""


@dataclass
class ConsumptionReport:
    """Bilan d'une session de consommation.

    Attributes:
        sites_written: Fiches de site appliquees au referentiel.
        raw_measures_written: Mesures brutes inserees ou deja presentes.
        imputed_measures_written: Mesures reconstruites inserees ou deja presentes.
        unlinked_imputed_measures: Mesures reconstruites ecrites sans lien vers leur
            mesure brute, celle ci n'etant pas encore arrivee.
    """

    sites_written: int = 0
    raw_measures_written: int = 0
    imputed_measures_written: int = 0
    unlinked_imputed_measures: int = 0


class PersistenceConsumer:
    """Boucle de persistance des sites et des mesures."""

    def __init__(
        self,
        consumer: ConsumerLike,
        connection: ConnectionLike,
        site_topic: str,
        measure_raw_topic: str,
        measure_imputed_topic: str,
        refresh_site_registry: Callable[[], int],
    ) -> None:
        """Prepare le consumer.

        Args:
            consumer: Acces au bus de messages.
            connection: Connexion vers la base, en validation manuelle.
            site_topic: Topic du referentiel des sites.
            measure_raw_topic: Topic des mesures brutes.
            measure_imputed_topic: Topic des mesures reconstruites.
            refresh_site_registry: Relit le referentiel et l'applique en base, en
                rendant le nombre de sites appliques. Appele au demarrage, puis a
                chaque fois qu'un fait reference un site encore inconnu.
        """
        self._consumer = consumer
        self._connection = connection
        self._site_topic = site_topic
        self._measure_raw_topic = measure_raw_topic
        self._measure_imputed_topic = measure_imputed_topic
        self._refresh_site_registry = refresh_site_registry

    @property
    def consumed_topics(self) -> list[str]:
        """Topics dont ce consumer a la charge."""
        return [self._site_topic, self._measure_raw_topic, self._measure_imputed_topic]

    def run(
        self,
        max_messages: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    ) -> ConsumptionReport:
        """Consomme les messages disponibles et les ecrit en base.

        Args:
            max_messages: Nombre de messages a traiter, illimite si None.
            should_stop: Consulte avant chaque lecture pour interrompre la boucle.
            poll_timeout_seconds: Attente maximale d'un message.

        Returns:
            Le bilan de la session.

        Raises:
            UnknownSiteReferenceError: Si un site reste introuvable apres redrainage.
            PersistenceError: Si une ecriture echoue pour une autre raison.
            ValueError: Si un message arrive d'un topic non pris en charge.
        """
        # Le referentiel precede les faits qui le referencent : les topics ne sont pas
        # ordonnes entre eux et measure_raw.site_id est une cle etrangere.
        self._refresh_site_registry()
        self._consumer.subscribe(self.consumed_topics)
        report = ConsumptionReport()
        handled = 0

        while max_messages is None or handled < max_messages:
            if should_stop is not None and should_stop():
                break

            message = self._consumer.poll(poll_timeout_seconds)
            if message is None:
                continue

            self._persist(message, report)
            handled += 1

        return report

    def close(self) -> None:
        """Quitte le groupe et referme la connexion."""
        self._consumer.close()
        self._connection.close()

    def _persist(self, message: ConsumedMessage, report: ConsumptionReport) -> None:
        """Ecrit un message puis acquitte sa position, dans cet ordre.

        Un site inconnu n'est pas une donnee invalide mais une course : sa fiche peut
        etre encore en route. Le referentiel est donc redraine et le message rejoue une
        fois. S'il echoue encore, l'exception remonte sans que l'offset soit acquitte,
        et le message reviendra au redemarrage.

        Args:
            message: Message lu sur le bus.
            report: Bilan de la session, enrichi au passage.

        Raises:
            UnknownSiteReferenceError: Si le site reste introuvable apres redrainage.
            PersistenceError: Si une ecriture echoue pour une autre raison.
            ValueError: Si le topic du message n'est pas pris en charge.
        """
        try:
            self._write(message, report)
        except UnknownSiteReferenceError:
            self._connection.rollback()
            self._refresh_site_registry()
            self._write(message, report)

        self._connection.commit()
        self._consumer.commit(message=message, asynchronous=False)

    def _write(self, message: ConsumedMessage, report: ConsumptionReport) -> None:
        """Route le message vers le depot de sa table.

        Args:
            message: Message lu sur le bus.
            report: Bilan de la session, enrichi une fois l'ecriture reussie.

        Raises:
            ValueError: Si le topic du message n'est pas pris en charge.
        """
        topic = message.topic()

        if topic == self._site_topic:
            self._apply_site(message)
            report.sites_written += 1
        elif topic == self._measure_raw_topic:
            self._apply_raw_measure(message)
            report.raw_measures_written += 1
        elif topic == self._measure_imputed_topic:
            if not self._apply_imputed_measure(message):
                report.unlinked_imputed_measures += 1
            report.imputed_measures_written += 1
        else:
            raise ValueError(f"topic {topic!r} is not handled by the persistence consumer")

    def _apply_site(self, message: ConsumedMessage) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[SitePayload]
        )
        upsert_site(self._connection, envelope.payload)

    def _apply_raw_measure(self, message: ConsumedMessage) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[MeasureRawPayload]
        )
        measure_raw_repository.insert_if_new(self._connection, envelope.payload)

    def _apply_imputed_measure(self, message: ConsumedMessage) -> bool:
        """Ecrit une mesure reconstruite et la relie a sa mesure brute si possible.

        Attendre la mesure brute serait un interblocage : elle ne peut arriver que par
        la boucle qu'on bloquerait. La ligne est donc ecrite avec un lien vide, ce que
        le schema autorise, et l'appelant la compte pour que le trou reste visible.

        Args:
            message: Message lu sur le topic des mesures reconstruites.

        Returns:
            Vrai si la mesure brute correspondante a ete retrouvee.
        """
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[MeasureImputedPayload]
        )
        measure = envelope.payload
        correlated_raw_id = measure_imputed_repository.find_raw_id(
            self._connection, measure.site_id, measure.timestamp
        )
        measure_imputed_repository.insert_if_new(self._connection, measure, correlated_raw_id)
        return correlated_raw_id is not None
