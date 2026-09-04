"""Interface en ligne de commande du collecteur.

Deux commandes : la collecte temps reel et le rattrapage historique. La destination
des messages est choisie par configuration, ce qui permet de dérouler toute la chaine
sur la sortie standard avant qu'un broker existe.
"""

import sys
from datetime import UTC, datetime, timedelta
from math import ceil
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

MAX_POINTS_PER_REQUEST = 1000
"""Plafond de l'API : au dela, /api/v1/readings repond 422."""


def resolve_sampling(hours: float, points: int, resolution: Optional[float]) -> float:
    """Determine l'ecart entre deux mesures d'un rattrapage.

    Par defaut la periode entiere tient en une seule requete : le parametre limit de
    l'API fixe le nombre de points repartis dans la fenetre, donc c'est la resolution
    qui s'ajuste. Imposer une resolution fine force au contraire un decoupage en
    plusieurs requetes, ce que l'instance mock supporte mal.

    Args:
        hours: Profondeur de la periode, en heures.
        points: Nombre de mesures souhaitees sur toute la periode.
        resolution: Ecart impose entre deux mesures, ou None pour le deduire.

    Returns:
        L'ecart a demander, en secondes.

    Raises:
        ValueError: Si la periode, le nombre de points ou la resolution sont invalides.
    """
    if hours <= 0:
        raise ValueError(f"hours must be strictly positive, received {hours}")
    if resolution is not None:
        if resolution <= 0:
            raise ValueError(f"resolution must be strictly positive, received {resolution}")
        return resolution
    if not 1 <= points <= MAX_POINTS_PER_REQUEST:
        raise ValueError(
            f"points must be between 1 and {MAX_POINTS_PER_REQUEST}, received {points}"
        )
    return hours * 3600 / points


def _force_utf8_output() -> None:
    """Impose l'UTF-8 sur les sorties, quel que soit le systeme.

    Les noms de sites contiennent des accents. Sous Windows, l'encodage de sortie suit
    la page de code de la console, qui n'est pas UTF-8 par defaut : les messages
    ecrits dans un fichier seraient alors illisibles pour le consumer.
    """
    for flux in (sys.stdout, sys.stderr):
        reconfigure = getattr(flux, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


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
    _force_utf8_output()
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    # Detourne SIGTERM avant toute publication : sans cela, docker stop interromprait
    # le processus sans vider la file du producer.
    shutdown = ShutdownRequest()
    shutdown.install()

    with ResilientHttpClient(
        settings.api_mock_base_url,
        settings.api_mock_timeout_seconds,
        minimum_interval_seconds=settings.api_mock_min_request_interval_seconds,
    ) as http_client:
        collector = RealtimeCollector(
            api_client=MockApiClient(http_client),
            publisher=_build_publisher(settings),
            site_topic=settings.kafka_topic_site,
            measure_raw_topic=settings.kafka_topic_measure_raw,
            measure_imputed_topic=settings.kafka_topic_measure_imputed,
            alert_topic=settings.kafka_topic_alert,
            source_timezone=settings.api_mock_source_timezone,
            max_gap_measures=settings.imputation_max_gap_measures,
            site_refresh_interval_seconds=settings.site_refresh_interval_seconds,
            configured_sites=settings.sites,
        )
        # Le parc n'est connu qu'apres le premier cycle : on ne peut juger la cadence
        # qu'a partir de la, mais mieux vaut tard que jamais.
        try:
            collector.run(max_cycles=1, interval_seconds=settings.poll_interval_seconds)
            shortfall = collector.cadence_shortfall_seconds(
                settings.poll_interval_seconds,
                settings.api_mock_min_request_interval_seconds,
            )
            if shortfall:
                logger.warning(
                    "cadence_unsustainable",
                    shortfall_s=round(shortfall, 1),
                    advice="raise POLL_INTERVAL_SECONDS, narrow SITES, or lower "
                    "API_MOCK_MIN_REQUEST_INTERVAL_SECONDS",
                )

            remaining_cycles = None if cycles is None else max(0, cycles - 1)
            collector.run(
                max_cycles=remaining_cycles,
                interval_seconds=settings.poll_interval_seconds,
                should_stop=lambda: shutdown.requested,
            )
        except KeyboardInterrupt:
            logger.info("shutdown_requested")
        except MockApiError as failure:
            logger.error("collection_interrupted", cause=str(failure))
            raise typer.Exit(code=1) from failure
        finally:
            collector.close()


@application.command("backfill")
def backfill(
    site: str = typer.Option(..., help="Identifiant du site a rattraper."),
    hours: float = typer.Option(24.0, help="Profondeur de la periode, en heures."),
    points: int = typer.Option(
        MAX_POINTS_PER_REQUEST,
        help="Nombre de mesures sur toute la periode. La periode tient alors en une "
        "seule requete.",
    ),
    resolution: Optional[float] = typer.Option(
        None,
        help="Ecart impose entre deux mesures, en secondes. Force un decoupage en "
        "plusieurs requetes, ce que l'instance mock supporte mal.",
    ),
    force_degenerate: bool = typer.Option(
        False, help="Publier meme une fenetre jugee inexploitable."
    ),
) -> None:
    """Rejoue l'historique d'un site sur une periode et le publie.

    Args:
        site: Identifiant du site a rattraper.
        hours: Profondeur de la periode, en heures.
        points: Nombre de mesures souhaitees sur toute la periode.
        resolution: Ecart impose entre deux mesures, ou None pour le deduire.
        force_degenerate: Vrai pour publier malgre le garde fou.
    """
    _force_utf8_output()
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    try:
        sampling_seconds = resolve_sampling(hours, points, resolution)
    except ValueError as invalid_request:
        logger.error("invalid_sampling", cause=str(invalid_request))
        raise typer.Exit(code=1) from invalid_request

    logger.info(
        "backfill_requested",
        site=site,
        hours=hours,
        resolution_s=round(sampling_seconds, 1),
        requests=max(1, ceil(hours * 3600 / sampling_seconds / MAX_POINTS_PER_REQUEST)),
    )

    end_time = datetime.now(UTC).replace(tzinfo=None)
    start_time = end_time - timedelta(hours=hours)

    with ResilientHttpClient(
        settings.api_mock_base_url,
        settings.api_mock_timeout_seconds,
        minimum_interval_seconds=settings.api_mock_min_request_interval_seconds,
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
            rattrapage.run(site, start_time, end_time, sampling_seconds)
        except MockApiError as failure:
            logger.error("backfill_failed", site=site, cause=str(failure))
            raise typer.Exit(code=1) from failure
        finally:
            publisher.flush()
            publisher.close()


if __name__ == "__main__":
    application()
