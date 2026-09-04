"""Interface en ligne de commande des consumers.

Deux commandes, une par service. Elles ne se distinguent que par les topics consommes
et le consumer group : l'image est la meme, seule la commande change dans le compose.
"""

from typing import Optional

import typer

from .config import ConsumerSettings, load_alerting_settings, load_persistence_settings
from .extract.kafka_consumer import ConsumerLike, create_consumer
from .load.postgres_connection import ConnectionLike, create_connection
from .logging_setup import configure_logging, get_logger
from .orchestration.alerting_consumer import AlertingConsumer
from .orchestration.graceful_shutdown import ShutdownRequest
from .orchestration.persistence_consumer import PersistenceConsumer
from .orchestration.site_registry_drain import SiteRegistryDrain

application = typer.Typer(
    help="Consumers EnerVision : persistance des mesures et des alertes.",
    add_completion=False,
    # Une erreur metier doit se lire, pas se decoder dans une trace Python.
    pretty_exceptions_enable=False,
)
logger = get_logger("cli")

MESSAGES_OPTION = typer.Option(
    None, help="Nombre de messages a traiter. Sans limite par defaut."
)


def _open_service_consumer(settings: ConsumerSettings) -> ConsumerLike:
    return create_consumer(settings.kafka_bootstrap_servers, settings.kafka_consumer_group)


def _build_registry_drain(
    settings: ConsumerSettings,
    connection: ConnectionLike,
) -> SiteRegistryDrain:
    """Construit le redrainage du referentiel, sur son propre consumer group.

    Args:
        settings: Configuration du service.
        connection: Connexion partagee avec la boucle de consommation.

    Returns:
        Le drainage, appelable sans argument.
    """
    return SiteRegistryDrain(
        open_consumer=lambda: create_consumer(
            settings.kafka_bootstrap_servers, settings.registry_consumer_group
        ),
        connection=connection,
        site_topic=settings.kafka_topic_site,
    )


@application.command("consume-persistence")
def consume_persistence(max_messages: Optional[int] = MESSAGES_OPTION) -> None:
    """Ecrit en base les sites et les mesures publies par le collecteur.

    Args:
        max_messages: Nombre de messages a traiter, illimite si absent.
    """
    settings = load_persistence_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    shutdown = ShutdownRequest()
    shutdown.install()

    connection = create_connection(settings.database_url)
    consumer = PersistenceConsumer(
        consumer=_open_service_consumer(settings),
        connection=connection,
        site_topic=settings.kafka_topic_site,
        measure_raw_topic=settings.kafka_topic_measure_raw,
        measure_imputed_topic=settings.kafka_topic_measure_imputed,
        refresh_site_registry=_build_registry_drain(settings, connection),
    )

    try:
        report = consumer.run(
            max_messages=max_messages,
            should_stop=lambda: shutdown.requested,
        )
        logger.info(
            "persistence_completed",
            sites=report.sites_written,
            raw_measures=report.raw_measures_written,
            imputed_measures=report.imputed_measures_written,
            unlinked_imputed_measures=report.unlinked_imputed_measures,
        )
    finally:
        consumer.close()


@application.command("consume-alerting")
def consume_alerting(max_messages: Optional[int] = MESSAGES_OPTION) -> None:
    """Ecrit en base les alertes relevees par le collecteur.

    Args:
        max_messages: Nombre de messages a traiter, illimite si absent.
    """
    settings = load_alerting_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    shutdown = ShutdownRequest()
    shutdown.install()

    connection = create_connection(settings.database_url)
    consumer = AlertingConsumer(
        consumer=_open_service_consumer(settings),
        connection=connection,
        site_topic=settings.kafka_topic_site,
        alert_topic=settings.kafka_topic_alert,
        refresh_site_registry=_build_registry_drain(settings, connection),
    )

    try:
        report = consumer.run(
            max_messages=max_messages,
            should_stop=lambda: shutdown.requested,
        )
        logger.info(
            "alerting_completed",
            sites=report.sites_written,
            alerts=report.alerts_written,
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    application()
