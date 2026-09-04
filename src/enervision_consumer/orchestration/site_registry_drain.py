"""Relecture du referentiel des sites depuis son topic compacte.

Le topic ne conserve qu'un message par site : le relire depuis son debut donne donc
l'etat courant du parc, pas un historique. Ce drainage n'acquitte jamais ses offsets,
ce qui lui fait reprendre au debut a chaque appel, comportement voulu ici.

La fin du topic est reconnue a son silence, faute d'API d'offsets dans le contrat de
consumer utilise ici. Encore faut il ne pas confondre deux silences : un groupe qui
vient de rejoindre ne recoit rien tant que sa partition ne lui est pas attribuee, ce qui
n'a rien d'une fin de topic. Le budget de lectures vides est donc plus large tant que
rien n'est arrive, et se resserre des le premier message recu.

Si le drainage s'arretait malgre tout trop tot, le fait qui reference un site manquant
echouerait sur la cle etrangere, ce qui declenche precisement un nouveau drainage.
"""

from collections.abc import Callable

from enervision_contracts.envelope import MessageEnvelope, SitePayload

from ..extract.envelope_decoding import decode_envelope
from ..extract.kafka_consumer import ConsumerLike
from ..load.postgres_connection import ConnectionLike
from ..load.site_repository import upsert_site
from ..logging_setup import get_logger

logger = get_logger("site_registry_drain")

DEFAULT_POLL_TIMEOUT_SECONDS = 1.0
"""Attente d'un message avant de compter une lecture comme silencieuse."""

DEFAULT_SILENT_POLLS_BEFORE_END = 2
"""Lectures vides consecutives valant fin du topic, une fois le premier message recu."""

DEFAULT_SILENT_POLLS_BEFORE_FIRST = 10
"""Lectures vides tolerees avant le premier message, le temps que le groupe rejoigne."""


class SiteRegistryDrain:
    """Applique en base l'integralite du referentiel publie sur le bus."""

    def __init__(
        self,
        open_consumer: Callable[[], ConsumerLike],
        connection: ConnectionLike,
        site_topic: str,
        poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
        silent_polls_before_end: int = DEFAULT_SILENT_POLLS_BEFORE_END,
        silent_polls_before_first: int = DEFAULT_SILENT_POLLS_BEFORE_FIRST,
    ) -> None:
        """Prepare le drainage.

        Args:
            open_consumer: Ouvre un consumer dedie, sur un groupe distinct du service.
            connection: Connexion vers la base, en validation manuelle.
            site_topic: Topic compacte du referentiel.
            poll_timeout_seconds: Attente maximale d'un message.
            silent_polls_before_end: Lectures vides valant fin du topic, une fois le
                premier message recu.
            silent_polls_before_first: Lectures vides tolerees avant le premier message,
                le temps que le groupe rejoigne et recoive sa partition.
        """
        self._open_consumer = open_consumer
        self._connection = connection
        self._site_topic = site_topic
        self._poll_timeout_seconds = poll_timeout_seconds
        self._silent_polls_before_end = silent_polls_before_end
        self._silent_polls_before_first = silent_polls_before_first

    def __call__(self) -> int:
        """Relit le referentiel et l'applique en base.

        Returns:
            Le nombre de fiches de site appliquees.

        Raises:
            EnvelopeDecodingError: Si un message du topic ne respecte pas le contrat.
            PersistenceError: Si le pilote refuse une ecriture.
        """
        consumer = self._open_consumer()
        applied_sites = 0

        try:
            consumer.subscribe([self._site_topic])
            silent_polls = 0

            while True:
                message = consumer.poll(self._poll_timeout_seconds)
                broker_error = None if message is None else message.error()
                if broker_error is not None:
                    logger.warning("broker_event_ignored", cause=str(broker_error))
                if message is None or broker_error is not None:
                    # Un evenement d'erreur ne porte pas de donnee : il compte comme une
                    # lecture vide, sans quoi un topic indisponible bouclerait sans fin.
                    silent_polls += 1
                    tolerated = (
                        self._silent_polls_before_end
                        if applied_sites
                        else self._silent_polls_before_first
                    )
                    if silent_polls >= tolerated:
                        break
                    continue

                silent_polls = 0
                envelope = decode_envelope(
                    message.topic(), message.value(), MessageEnvelope[SitePayload]
                )
                upsert_site(self._connection, envelope.payload)
                applied_sites += 1

            self._connection.commit()
        finally:
            consumer.close()

        logger.info("site_registry_drained", sites=applied_sites)
        return applied_sites
