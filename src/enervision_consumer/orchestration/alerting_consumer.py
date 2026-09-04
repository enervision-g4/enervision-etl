"""Consumer d'alerting : ecrit les alertes relevees par le collecteur.

Il partage la boucle du consumer de persistance mais pas son consumer group ni ses
topics. Il tient sa propre vue du referentiel : alert.site_id est une cle etrangere, et
rien ne garantit que l'autre service ait deja rattrape le sien.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

from enervision_contracts.envelope import AlertPayload, MessageEnvelope, SitePayload

from ..extract.envelope_decoding import decode_envelope
from ..extract.kafka_consumer import ConsumedMessage, ConsumerLike
from ..load import alert_repository
from ..load.postgres_connection import ConnectionLike
from ..load.site_repository import upsert_site
from .consumption_loop import DEFAULT_POLL_TIMEOUT_SECONDS, ConsumptionLoop


@dataclass
class AlertingReport:
    """Bilan d'une session d'alerting.

    Attributes:
        sites_written: Fiches de site appliquees au referentiel.
        alerts_written: Alertes inserees ou deja presentes.
    """

    sites_written: int = 0
    alerts_written: int = 0


class AlertingConsumer:
    """Consumer des alertes actives et du referentiel dont elles dependent."""

    def __init__(
        self,
        consumer: ConsumerLike,
        connection: ConnectionLike,
        site_topic: str,
        alert_topic: str,
        refresh_site_registry: Callable[[], int],
    ) -> None:
        """Prepare le consumer.

        Args:
            consumer: Acces au bus de messages.
            connection: Connexion vers la base, en validation manuelle.
            site_topic: Topic du referentiel des sites.
            alert_topic: Topic des alertes actives.
            refresh_site_registry: Relit le referentiel et l'applique en base, en
                rendant le nombre de sites appliques.
        """
        self._consumer = consumer
        self._connection = connection
        self._site_topic = site_topic
        self._alert_topic = alert_topic
        self._refresh_site_registry = refresh_site_registry

    @property
    def consumed_topics(self) -> list[str]:
        """Topics dont ce consumer a la charge."""
        return [self._site_topic, self._alert_topic]

    def run(
        self,
        max_messages: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    ) -> AlertingReport:
        """Consomme les alertes disponibles et les ecrit en base.

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
        report = AlertingReport()
        self._loop_for(report).run(max_messages, should_stop, poll_timeout_seconds)
        return report

    def close(self) -> None:
        """Quitte le groupe et referme la connexion."""
        self._consumer.close()
        self._connection.close()

    def _loop_for(self, report: AlertingReport) -> ConsumptionLoop:
        return ConsumptionLoop(
            consumer=self._consumer,
            connection=self._connection,
            refresh_site_registry=self._refresh_site_registry,
            handlers={
                self._site_topic: lambda message: self._apply_site(message, report),
                self._alert_topic: lambda message: self._apply_alert(message, report),
            },
        )

    def _apply_site(self, message: ConsumedMessage, report: AlertingReport) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[SitePayload]
        )
        upsert_site(self._connection, envelope.payload)
        report.sites_written += 1

    def _apply_alert(self, message: ConsumedMessage, report: AlertingReport) -> None:
        envelope = decode_envelope(
            message.topic(), message.value(), MessageEnvelope[AlertPayload]
        )
        alert_repository.insert_if_new(self._connection, envelope.payload)
        report.alerts_written += 1
