"""Collecteur temps reel : interroge les sites a intervalle regulier et publie.

Assemble les couches precedentes en une boucle. Trois regles la gouvernent.

La panne d'un site n'interrompt jamais les autres : chaque interrogation est isolee.

L'imputation se fait sans anticipation. La mesure suivante etant inconnue au moment ou
la courante est traitee, seule la recopie de la derniere valeur connue est applicable,
a partir d'une courte fenetre gardee en memoire par site.

Le referentiel n'est relu qu'a intervalle configure, et republie seulement s'il a change.
"""

from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from time import perf_counter
from types import TracebackType
from typing import Optional, Protocol

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import (
    CollectionMode,
    envelope_for_imputed_reading,
    envelope_for_raw_reading,
)
from enervision_contracts.site import Site

from ..extract.errors import MockApiError
from ..extract.site_selection import resolve_site_identifiers
from ..load.publisher import MessagePublisher
from ..load.site_registry_publisher import SiteRegistryPublisher
from ..logging_setup import get_logger
from ..transform.imputation import impute_series
from ..transform.normalization import normalize_reading
from .drift_free_scheduler import DriftFreeScheduler

logger = get_logger("realtime_collector")


class SiteReadingSource(Protocol):
    """Sous ensemble du client d'API utilise par le collecteur."""

    def fetch_site_registry(self) -> list[Site]:
        """Renvoie la liste complete du parc."""
        ...

    def fetch_current_reading(self, site_id: str) -> EnergyReading:
        """Renvoie la mesure instantanee d'un site."""
        ...


@dataclass
class CycleReport:
    """Bilan d'un cycle de collecte.

    Attributes:
        readings_by_quality: Nombre de mesures par niveau de qualite.
        null_reasons_counts: Occurrences de chaque cause de valeur manquante.
        failed_sites: Sites dont l'interrogation a echoue.
        published_sites: Sites du referentiel republies pendant ce cycle.
        duration_seconds: Duree du cycle.
    """

    readings_by_quality: dict[str, int] = field(default_factory=dict)
    null_reasons_counts: dict[str, int] = field(default_factory=dict)
    failed_sites: list[str] = field(default_factory=list)
    published_sites: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class RealtimeCollector:
    """Boucle de collecte des mesures instantanees."""

    def __init__(
        self,
        api_client: SiteReadingSource,
        publisher: MessagePublisher,
        site_topic: str,
        measure_raw_topic: str,
        measure_imputed_topic: str,
        source_timezone: str,
        max_gap_measures: int,
        site_refresh_interval_seconds: float,
        configured_sites: Optional[Sequence[str]] = None,
    ) -> None:
        """Prepare le collecteur.

        Args:
            api_client: Acces a l'API mock.
            publisher: Destination des messages.
            site_topic: Topic de la table SITE.
            measure_raw_topic: Topic des mesures brutes.
            measure_imputed_topic: Topic des mesures reconstruites.
            source_timezone: Fuseau suppose des horodatages naifs de l'API.
            max_gap_measures: Longueur maximale d'un trou comblable.
            site_refresh_interval_seconds: Delai entre deux relectures du parc.
            configured_sites: Restriction eventuelle, vide pour tout le parc.
        """
        self._api_client = api_client
        self._publisher = publisher
        self._measure_raw_topic = measure_raw_topic
        self._measure_imputed_topic = measure_imputed_topic
        self._source_timezone = source_timezone
        self._max_gap_measures = max_gap_measures
        self._configured_sites = list(configured_sites) if configured_sites else []

        self._registry_publisher = SiteRegistryPublisher(
            publisher=publisher,
            topic=site_topic,
            refresh_interval_seconds=site_refresh_interval_seconds,
        )
        self._site_types: dict[str, str] = {}
        self._collected_site_ids: list[str] = []
        # Fenetre glissante par site : de quoi ancrer une recopie sans jamais grossir.
        self._recent_readings: dict[str, deque[EnergyReading]] = {}

    def cadence_shortfall_seconds(
        self,
        poll_interval_seconds: float,
        minimum_request_interval_seconds: float,
    ) -> float:
        """Mesure de combien la periode demandee est trop courte pour le parc collecte.

        Interroger N sites en respectant un espacement minimal prend N fois cet
        espacement. Si ce total depasse la periode, le collecteur ne tiendra jamais sa
        cadence : autant le dire au demarrage plutot que de laisser l'exploitant
        decouvrir des cycles sautes.

        Args:
            poll_interval_seconds: Periode visee entre deux cycles.
            minimum_request_interval_seconds: Espacement impose entre deux requetes.

        Returns:
            Le nombre de secondes manquantes, ou zero si la cadence est tenable ou si
            le parc n'est pas encore connu.
        """
        if not self._collected_site_ids:
            return 0.0

        needed = len(self._collected_site_ids) * minimum_request_interval_seconds
        return max(0.0, needed - poll_interval_seconds)

    def run(
        self,
        max_cycles: Optional[int] = None,
        interval_seconds: float = 60.0,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> list[CycleReport]:
        """Execute la boucle de collecte.

        L'arret est verifie entre deux cycles, jamais au milieu : un cycle entame va
        toujours a son terme, ce qui evite de publier une photo partielle du parc.

        Args:
            max_cycles: Nombre de cycles a executer, illimite si None.
            interval_seconds: Periode visee entre deux cycles.
            should_stop: Consulte avant chaque cycle pour interrompre la boucle.

        Returns:
            Le bilan de chaque cycle execute.
        """
        scheduler = DriftFreeScheduler(interval_seconds)
        reports: list[CycleReport] = []

        while max_cycles is None or len(reports) < max_cycles:
            if should_stop is not None and should_stop():
                logger.info("boucle_interrompue", cycles=len(reports))
                break

            tick = scheduler.wait_for_next_tick(should_stop)

            # Un signal recu pendant l'attente doit sortir sans lancer un cycle de plus.
            if should_stop is not None and should_stop():
                logger.info("boucle_interrompue", cycles=len(reports))
                break

            if tick.skipped_ticks:
                logger.warning(
                    "cycles_sautes",
                    sautes=tick.skipped_ticks,
                    retard_s=round(tick.lateness_seconds, 3),
                )
            reports.append(self.run_cycle())

        return reports

    def run_cycle(self) -> CycleReport:
        """Execute un cycle complet de collecte.

        Returns:
            Le bilan du cycle.
        """
        started_at = perf_counter()
        report = CycleReport()

        report.published_sites = self._refresh_registry_if_due()

        for site_id in self._collected_site_ids:
            self._collect_one_site(site_id, report)

        report.duration_seconds = perf_counter() - started_at
        logger.info(
            "cycle_termine",
            sites=len(self._collected_site_ids),
            qualite=report.readings_by_quality,
            echecs=report.failed_sites,
            duree_s=round(report.duration_seconds, 3),
        )
        return report

    def _refresh_registry_if_due(self) -> list[str]:
        """Relit le parc si l'intervalle est ecoule, et republie ce qui a change.

        Returns:
            Les identifiants des sites republies.
        """
        if not self._registry_publisher.is_refresh_due():
            return []

        site_registry = self._api_client.fetch_site_registry()
        self._site_types = {site.site_id: site.site_type for site in site_registry}
        self._collected_site_ids = resolve_site_identifiers(
            self._configured_sites, site_registry
        )
        published_sites = self._registry_publisher.publish_changes(site_registry)
        if published_sites:
            logger.info("referentiel_publie", sites=published_sites)
        return published_sites

    def _collect_one_site(self, site_id: str, report: CycleReport) -> None:
        """Interroge un site, publie sa mesure et sa reconstruction.

        Une panne reste circonscrite a ce site : elle est journalisee et le cycle
        continue, sans quoi un seul compteur muet priverait tout le parc de donnees.

        Args:
            site_id: Site a interroger.
            report: Bilan du cycle, enrichi au passage.
        """
        try:
            reading = self._api_client.fetch_current_reading(site_id)
        except MockApiError as failure:
            report.failed_sites.append(site_id)
            logger.warning("site_injoignable", site=site_id, cause=str(failure))
            return

        normalized = normalize_reading(reading, self._source_timezone)
        self._publisher.publish(
            self._measure_raw_topic,
            envelope_for_raw_reading(normalized, CollectionMode.REALTIME),
        )

        self._count_quality(normalized, report)
        self._publish_imputed(site_id, normalized)

    def _count_quality(self, reading: EnergyReading, report: CycleReport) -> None:
        quality_counts = Counter(report.readings_by_quality)
        quality_counts[reading.data_quality] += 1
        report.readings_by_quality = dict(quality_counts)

        reason_counts = Counter(report.null_reasons_counts)
        reason_counts.update(reading.null_reasons)
        report.null_reasons_counts = dict(reason_counts)

    def _publish_imputed(self, site_id: str, reading: EnergyReading) -> None:
        """Reconstruit la mesure courante a partir de la fenetre recente et la publie.

        Args:
            site_id: Site concerne.
            reading: Mesure normalisee qui vient d'arriver.
        """
        window = self._recent_readings.setdefault(
            site_id, deque(maxlen=self._max_gap_measures + 2)
        )
        window.append(reading)

        imputed_window = impute_series(
            list(window),
            site_type=self._site_types.get(site_id),
            max_gap_measures=self._max_gap_measures,
            lookahead_available=False,
        )
        self._publisher.publish(
            self._measure_imputed_topic,
            envelope_for_imputed_reading(imputed_window[-1], CollectionMode.REALTIME),
        )

    def close(self) -> None:
        """Vide la destination avant de rendre la main."""
        pending = self._publisher.flush()
        if pending:
            logger.error("messages_non_remis", en_attente=pending)
        self._publisher.close()

    def __enter__(self) -> "RealtimeCollector":
        """Entre dans le bloc de contexte."""
        return self

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> None:
        """Vide la destination a la sortie du bloc de contexte.

        Args:
            exception_type: Type de l'exception ayant interrompu le bloc.
            exception_value: Instance de cette exception.
            exception_traceback: Pile d'appels associee.
        """
        self.close()
