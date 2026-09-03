"""Interface en ligne de commande du collecteur.

Deux commandes : la collecte temps reel et le rattrapage historique. La destination
des messages est choisie par configuration, ce qui permet de dérouler toute la chaine
sur la sortie standard avant qu'un broker existe.
"""

from datetime import UTC, datetime, timedelta
from typing import Optional

import typer

from .config import EtlSettings, PublisherTarget, load_settings
from .extract.errors import MockApiError
from .extract.http_client import ResilientHttpClient
from .extract.mock_api_client import MockApiClient
from .load.kafka_publisher import KafkaPublisher
from .load.publisher import MessagePublisher
from .load.stdout_publisher import StdoutPublisher
from .logging_setup import configure_logging, get_logger
from .orchestration.batch_backfill import BatchBackfill
from .orchestration.graceful_shutdown import ShutdownRequest
from .orchestration.realtime_collector import RealtimeCollector

application = typer.Typer(
    help="Collecteur EnerVision : extraction, normalisation, imputation, publication.",
    add_completion=False,
    # Une erreur metier doit se lire, pas se decoder dans une trace Python.
    pretty_exceptions_enable=False,
)
logger = get_logger("cli")


def _build_publisher(settings: EtlSettings) -> MessagePublisher:
    """Construit la destination des messages selon la configuration.

    Args:
        settings: Configuration du pipeline.

    Returns:
        La destination correspondant a PUBLISHER_TARGET.
    """
    if settings.publisher_target is PublisherTarget.KAFKA:
        return KafkaPublisher(bootstrap_servers=settings.kafka_bootstrap_servers)
    return StdoutPublisher()


@application.command("collect-realtime")
def collect_realtime(
    cycles: Optional[int] = typer.Option(
        None, help="Nombre de cycles a executer. Sans limite par defaut."
    ),
) -> None:
    """Interroge les sites a intervalle regulier et publie leurs mesures.

    Args:
        cycles: Nombre de cycles a executer, illimite si absent.
    """
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    # Detourne SIGTERM avant toute publication : sans cela, docker stop interromprait
    # le processus sans vider la file du producer.
    shutdown = ShutdownRequest()
    shutdown.install()

    with ResilientHttpClient(
        settings.api_mock_base_url, settings.api_mock_timeout_seconds
    ) as http_client:
        collector = RealtimeCollector(
            api_client=MockApiClient(http_client),
            publisher=_build_publisher(settings),
            site_topic=settings.kafka_topic_site,
            measure_raw_topic=settings.kafka_topic_measure_raw,
            measure_imputed_topic=settings.kafka_topic_measure_imputed,
            source_timezone=settings.api_mock_source_timezone,
            max_gap_measures=settings.imputation_max_gap_measures,
            site_refresh_interval_seconds=settings.site_refresh_interval_seconds,
            configured_sites=settings.sites,
        )
        try:
            collector.run(
                max_cycles=cycles,
                interval_seconds=settings.poll_interval_seconds,
                should_stop=lambda: shutdown.requested,
            )
        except KeyboardInterrupt:
            logger.info("arret_demande")
        except MockApiError as failure:
            logger.error("collecte_interrompue", cause=str(failure))
            raise typer.Exit(code=1) from failure
        finally:
            collector.close()


@application.command("backfill")
def backfill(
    site: str = typer.Option(..., help="Identifiant du site a rattraper."),
    hours: float = typer.Option(24.0, help="Profondeur de la periode, en heures."),
    resolution: float = typer.Option(60.0, help="Ecart vise entre deux mesures, en secondes."),
    force_degenerate: bool = typer.Option(
        False, help="Publier meme une fenetre jugee inexploitable."
    ),
) -> None:
    """Rejoue l'historique d'un site sur une periode et le publie.

    Args:
        site: Identifiant du site a rattraper.
        hours: Profondeur de la periode, en heures.
        resolution: Ecart vise entre deux mesures, en secondes.
        force_degenerate: Vrai pour publier malgre le garde fou.
    """
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    end_time = datetime.now(UTC).replace(tzinfo=None)
    start_time = end_time - timedelta(hours=hours)

    with ResilientHttpClient(
        settings.api_mock_base_url, settings.api_mock_timeout_seconds
    ) as http_client:
        publisher = _build_publisher(settings)
        rattrapage = BatchBackfill(
            api_client=MockApiClient(http_client),
            publisher=publisher,
            measure_raw_topic=settings.kafka_topic_measure_raw,
            measure_imputed_topic=settings.kafka_topic_measure_imputed,
            source_timezone=settings.api_mock_source_timezone,
            max_gap_measures=settings.imputation_max_gap_measures,
            publish_degenerate_windows=force_degenerate,
        )
        try:
            rattrapage.run(site, start_time, end_time, resolution)
        except MockApiError as failure:
            logger.error("rattrapage_impossible", site=site, cause=str(failure))
            raise typer.Exit(code=1) from failure
        finally:
            publisher.flush()
            publisher.close()


if __name__ == "__main__":
    application()
