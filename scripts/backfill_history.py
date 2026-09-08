"""Rejoue plusieurs mois d'historique pour tout le parc, mois calendaire par mois.

Necessaire parce que la commande `enervision-etl backfill` ne calcule sa fenetre que
depuis "maintenant" (--hours) et que MockApiClient refuse toute fenetre depassant
MAX_CHUNKS_PER_WINDOW chunks (500, soit ~347 jours a une resolution de 60s). Une
periode de plusieurs mois doit donc etre decoupee en plusieurs appels a
BatchBackfill.run, chacun sur une fenetre plus etroite.

Usage (a l'interieur du conteneur/environnement du collecteur, memes variables
d'environnement que `enervision-etl backfill`) :

    python scripts/backfill_history.py --months 13 --resolution 60
    python scripts/backfill_history.py --months 1 --sites SITE001
"""

import argparse
import calendar
import itertools
import sys
from datetime import UTC, datetime
from itertools import pairwise

from enervision_etl.config import load_settings
from enervision_etl.extract.errors import MockApiError
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import MockApiClient
from enervision_etl.load.kafka_publisher import KafkaPublisher
from enervision_etl.logging_setup import configure_logging, get_logger
from enervision_etl.orchestration.batch_backfill import BatchBackfill

logger = get_logger("backfill_history")


def months_before(reference: datetime, months: int) -> datetime:
    """Recule une date d'un nombre de mois calendaires, jour plafonne au mois cible.

    Args:
        reference: Date de depart.
        months: Nombre de mois a reculer.

    Returns:
        La date obtenue, avec le meme jour du mois si possible, sinon le dernier
        jour du mois cible (ex: 31 mars - 1 mois -> 28 ou 29 fevrier).
    """
    absolute_month_index = reference.year * 12 + (reference.month - 1) - months
    year, month = divmod(absolute_month_index, 12)
    month += 1
    day = min(reference.day, calendar.monthrange(year, month)[1])
    return reference.replace(year=year, month=month, day=day)


def monthly_windows(total_months: int, end_time: datetime) -> list[tuple[datetime, datetime]]:
    """Decoupe une profondeur en mois calendaires en fenetres jointives.

    Plus ancien en premier.

    Args:
        total_months: Nombre de mois a couvrir, en remontant depuis end_time.
        end_time: Borne la plus recente de la periode totale.

    Returns:
        Les fenetres (debut inclus, fin exclue), de la plus ancienne a la plus recente.
    """
    boundaries = [months_before(end_time, k) for k in range(total_months, -1, -1)]
    return list(pairwise(boundaries))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", type=int, default=13, help="Profondeur, en mois calendaires.")
    parser.add_argument(
        "--resolution", type=float, default=60.0, help="Ecart entre deux mesures, en secondes."
    )
    parser.add_argument(
        "--sites",
        nargs="*",
        default=None,
        help="Sites a rejouer. Sans argument, tout le parc expose par l'API.",
    )
    parser.add_argument(
        "--force-degenerate",
        action="store_true",
        help="Publier une fenetre meme jugee inexploitable (panne totale au moment de l'appel).",
    )
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level, settings.log_as_json)

    end_time = datetime.now(UTC).replace(tzinfo=None)
    windows = monthly_windows(args.months, end_time)

    with ResilientHttpClient(
        settings.api_mock_base_url,
        settings.api_mock_timeout_seconds,
        minimum_interval_seconds=settings.api_mock_min_request_interval_seconds,
    ) as http_client:
        api_client = MockApiClient(http_client)
        sites = args.sites or [site.site_id for site in api_client.fetch_site_registry()]

        publisher = KafkaPublisher(bootstrap_servers=settings.kafka_bootstrap_servers)
        rattrapage = BatchBackfill(
            api_client=api_client,
            publisher=publisher,
            measure_raw_topic=settings.kafka_topic_measure_raw,
            measure_imputed_topic=settings.kafka_topic_measure_imputed,
            source_timezone=settings.api_mock_source_timezone,
            max_gap_measures=settings.imputation_max_gap_measures,
            publish_degenerate_windows=args.force_degenerate,
        )

        logger.info(
            "history_backfill_starting",
            sites=sites,
            months=args.months,
            windows_per_site=len(windows),
            resolution_s=args.resolution,
        )
        print(
            f"Demarrage : {len(sites)} sites x {len(windows)} fenetres "
            f"= {len(sites) * len(windows)} appels. Ctrl+C interrompt proprement "
            "a tout moment (les fenetres deja publiees restent acquises).",
            flush=True,
        )

        failures: list[str] = []
        total_published = 0
        total_windows = len(sites) * len(windows)
        completed_windows = 0
        try:
            for site in sites:
                for window_start, window_end in windows:
                    completed_windows += 1
                    progress_percent = 100 * completed_windows / total_windows
                    try:
                        report = rattrapage.run(site, window_start, window_end, args.resolution)
                    except MockApiError as failure:
                        logger.error(
                            "window_failed",
                            site=site,
                            window_start=window_start.isoformat(),
                            window_end=window_end.isoformat(),
                            cause=str(failure),
                        )
                        failures.append(f"{site} {window_start.date()}..{window_end.date()}")
                        print(
                            f"[{completed_windows}/{total_windows} {progress_percent:5.1f}%] "
                            f"{site} {window_start.date()}..{window_end.date()} : "
                            f"ECHEC ({failure})",
                            flush=True,
                        )
                        continue
                    total_published += report.published_measures
                    logger.info(
                        "window_completed",
                        site=site,
                        window_start=window_start.isoformat(),
                        window_end=window_end.isoformat(),
                        published=report.published_measures,
                        null_ratio=round(report.null_ratio, 3),
                        refused_as_degenerate=report.refused_as_degenerate,
                    )
                    # Retour lisible directement dans le terminal, en plus du log JSON
                    # structure ci-dessus : utile pour suivre une execution interactive.
                    print(
                        f"[{completed_windows}/{total_windows} {progress_percent:5.1f}%] "
                        f"{site} {window_start.date()}..{window_end.date()} : "
                        f"{report.published_measures} mesures "
                        f"(null {report.null_ratio:.1%}, total publie {total_published})",
                        flush=True,
                    )
                    # Une fenetre publie ~90k messages (brut + impute) : sans ce flush,
                    # la file interne du producer (queue.buffering.max.messages, 100k
                    # par defaut) sature au bout de deux fenetres et chaque produce()
                    # suivant se met a bloquer sur un retry, ce qui degrade tout le run.
                    publisher.flush()
        finally:
            publisher.flush()
            publisher.close()

        logger.info(
            "history_backfill_completed",
            total_published=total_published,
            failed_windows=len(failures),
            failures=failures,
        )
        return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        # Les fenetres deja publiees avant l'interruption restent acquises (le
        # flush par fenetre les a deja livrees) : relancer plus tard reprend
        # simplement les fenetres manquantes, sans dupliquer (ON CONFLICT DO
        # NOTHING cote base sur (site_id, timestamp)).
        print("\nInterrompu (Ctrl+C) : les fenetres deja publiees restent en base.")
        sys.exit(130)
