"""Rattrapage historique sur une periode donnee.

Deux differences avec le temps reel. La serie entiere etant connue, l'interpolation
devient possible. Et une fenetre integralement nulle est refusee : ce n'est pas un
historique mais l'etat d'une panne au moment de l'appel, que le simulateur projette sur
toute la periode demandee. La persister reviendrait a inventer des heures de coupure.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

from enervision_contracts.energy_reading import EnergyReading
from enervision_contracts.envelope import (
    CollectionMode,
    envelope_for_imputed_reading,
    envelope_for_raw_reading,
)
from enervision_contracts.site import Site

from ..load.publisher import MessagePublisher
from ..logging_setup import get_logger
from ..transform.imputation import impute_series
from ..transform.normalization import normalize_reading

logger = get_logger("batch_backfill")

DEGENERATE_NULL_RATIO = 0.9
"""Au dela de cette proportion de mesures nulles, la fenetre n'est plus un historique."""


class HistorySource(Protocol):
    """Sous ensemble du client d'API utilise par le rattrapage."""

    def fetch_site_registry(self) -> list[Site]:
        """Renvoie la liste complete du parc."""
        ...

    def fetch_readings_window(
        self,
        site_id: Optional[str],
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: float = 60.0,
    ) -> list[EnergyReading]:
        """Renvoie l'historique simule d'un site sur une periode."""
        ...


@dataclass
class BackfillReport:
    """Bilan d'un rattrapage.

    Attributes:
        collected_measures: Mesures renvoyees par l'API.
        published_measures: Mesures effectivement publiees.
        null_ratio: Proportion de mesures sans consommation.
        refused_as_degenerate: Vrai si la fenetre a ete jugee inexploitable.
    """

    collected_measures: int = 0
    published_measures: int = 0
    null_ratio: float = 0.0
    refused_as_degenerate: bool = False


class BatchBackfill:
    """Rejoue l'historique d'un site sur une periode et le publie."""

    def __init__(
        self,
        api_client: HistorySource,
        publisher: MessagePublisher,
        measure_raw_topic: str,
        measure_imputed_topic: str,
        source_timezone: str,
        max_gap_measures: int,
        publish_degenerate_windows: bool = False,
    ) -> None:
        """Prepare le rattrapage.

        Args:
            api_client: Acces a l'API mock.
            publisher: Destination des messages.
            measure_raw_topic: Topic des mesures brutes.
            measure_imputed_topic: Topic des mesures reconstruites.
            source_timezone: Fuseau suppose des horodatages naifs de l'API.
            max_gap_measures: Longueur maximale d'un trou comblable.
            publish_degenerate_windows: Vrai pour publier malgre le garde fou.
        """
        self._api_client = api_client
        self._publisher = publisher
        self._measure_raw_topic = measure_raw_topic
        self._measure_imputed_topic = measure_imputed_topic
        self._source_timezone = source_timezone
        self._max_gap_measures = max_gap_measures
        self._publish_degenerate_windows = publish_degenerate_windows

    def run(
        self,
        site_id: str,
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: float = 60.0,
    ) -> BackfillReport:
        """Rejoue une periode pour un site.

        Args:
            site_id: Site a rattraper.
            start_time: Debut de la periode.
            end_time: Fin de la periode.
            resolution_seconds: Ecart souhaite entre deux mesures.

        Returns:
            Le bilan du rattrapage.

        Raises:
            MockApiError: Si l'API refuse la requete ou reste injoignable. Un
                rattrapage vise un site precis : echouer est preferable au silence.
        """
        raw_series = self._api_client.fetch_readings_window(
            site_id, start_time, end_time, resolution_seconds
        )
        report = BackfillReport(collected_measures=len(raw_series))
        if not raw_series:
            logger.warning("fenetre_vide", site=site_id)
            return report

        series = [normalize_reading(reading, self._source_timezone) for reading in raw_series]
        missing = sum(1 for reading in series if reading.consumption_kw is None)
        report.null_ratio = missing / len(series)
        report.refused_as_degenerate = report.null_ratio >= DEGENERATE_NULL_RATIO

        if report.refused_as_degenerate and not self._publish_degenerate_windows:
            logger.warning(
                "fenetre_degeneree_refusee",
                site=site_id,
                mesures=len(series),
                taux_nul=round(report.null_ratio, 3),
                raison="le site etait en panne au moment de l'appel, la periode entiere "
                "est vide : ce n'est pas un historique",
                remedes="essayer un autre site, reessayer plus tard, ou forcer la "
                "publication avec --force-degenerate",
            )
            return report

        site_type = self._site_type_of(site_id)
        imputed_series = impute_series(
            series, site_type=site_type, max_gap_measures=self._max_gap_measures
        )
        for reading, imputed in zip(series, imputed_series, strict=True):
            self._publisher.publish(
                self._measure_raw_topic,
                envelope_for_raw_reading(reading, CollectionMode.BATCH),
            )
            self._publisher.publish(
                self._measure_imputed_topic,
                envelope_for_imputed_reading(imputed, CollectionMode.BATCH),
            )

        report.published_measures = len(series)
        logger.info(
            "rattrapage_termine",
            site=site_id,
            mesures=report.published_measures,
            taux_nul=round(report.null_ratio, 3),
        )
        return report

    def _site_type_of(self, site_id: str) -> Optional[str]:
        """Retrouve le type d'un site, qui determine la strategie d'imputation.

        Args:
            site_id: Site concerne.

        Returns:
            Le type du site, ou None s'il est introuvable.
        """
        for site in self._api_client.fetch_site_registry():
            if site.site_id == site_id:
                return site.site_type
        return None
