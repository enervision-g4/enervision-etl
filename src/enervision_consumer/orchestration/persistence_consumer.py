"""Consumer de persistance : ecrit les sites et les mesures publies par le collecteur.

La boucle, l'ordre des validations et la reprise sur incident vivent dans
ConsumptionLoop. Ce module ne decrit que ce qui lui est propre : les topics dont il a
la charge, et ce qu'il fait de chacun.
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
from ..load.postgres_connection import ConnectionLike
from ..load.site_repository import upsert_site
from .consumption_loop import DEFAULT_POLL_TIMEOUT_SECONDS, ConsumptionLoop


@dataclass
class ConsumptionReport:
    """Bilan d'une session de persistance.

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
    """Consumer des sites, des mesures brutes et des mesures reconstruites."""

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
                rendant le nombre de sites appliques.
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
        """
        report = ConsumptionReport()
        self._loop_for(report).run(max_messages, should_stop, poll_timeout_seconds)
        return report

    def close(self) -> None:
        """Quitte le groupe et referme la connexion."""
        self._consumer.close()
        self._connection.close()

    def _loop_for(self, report: ConsumptionReport) -> ConsumptionLoop:
        return ConsumptionLoop(
            consumer=self._consumer,
            connection=self._connection,
            refresh_site_registry=self._refresh_site_registry,
            handlers={
                self._site_topic: lambda message: self._apply_site(message, report),
                self._measure_raw_topic: lambda message: self._apply_raw_measure(message, report),
                self._measure_imputed_topic: lambda message: self._apply_imputed_measure(
                    message, report
                ),
            },
        )

    def _apply_site(self, message: ConsumedMessage, report: ConsumptionReport) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[SitePayload]
        )
        upsert_site(self._connection, envelope.payload)
        report.sites_written += 1

    def _apply_raw_measure(self, message: ConsumedMessage, report: ConsumptionReport) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[MeasureRawPayload]
        )
        measure_raw_repository.insert_if_new(self._connection, envelope.payload)
        report.raw_measures_written += 1

    def _apply_imputed_measure(
        self,
        message: ConsumedMessage,
        report: ConsumptionReport,
    ) -> None:
        """Ecrit une mesure reconstruite et la relie a sa mesure brute si possible.

        Attendre la mesure brute serait un interblocage : elle ne peut arriver que par
        la boucle qu'on bloquerait. La ligne est donc ecrite avec un lien vide, ce que
        le schema autorise, et comptee pour que le trou reste visible.

        Args:
            message: Message lu sur le topic des mesures reconstruites.
            report: Bilan de la session, enrichi une fois l'ecriture reussie.
        """
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[MeasureImputedPayload]
        )
        measure = envelope.payload
        correlated_raw_id = measure_imputed_repository.find_raw_id(
            self._connection, measure.site_id, measure.timestamp
        )
        measure_imputed_repository.insert_if_new(self._connection, measure, correlated_raw_id)

        report.imputed_measures_written += 1
        if correlated_raw_id is None:
            report.unlinked_imputed_measures += 1
