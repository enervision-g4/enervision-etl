"""Ordonnanceur a periode fixe, insensible a la duree des traitements.

Un simple sleep de la periode derive : il attend APRES le traitement, donc le cycle
reel dure la periode plus la duree du traitement, et l'ecart s'accumule. Ici les
instants de declenchement sont calcules depuis une ancre, jamais depuis l'instant
courant, ce qui les garde alignes indefiniment.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from math import floor
from typing import Optional


@dataclass(frozen=True)
class SchedulerTick:
    """Declenchement d'un cycle.

    Attributes:
        index: Rang du tick depuis le demarrage.
        skipped_ticks: Ticks sautes parce que le cycle precedent a deborde.
        lateness_seconds: Retard sur l'instant theorique de ce tick.
    """

    index: int
    skipped_ticks: int
    lateness_seconds: float


class DriftFreeScheduler:
    """Cadence une boucle a periode fixe sans accumuler de retard."""

    def __init__(
        self,
        interval_seconds: float,
        monotonic: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        """Prepare l'ordonnanceur.

        Args:
            interval_seconds: Periode souhaitee entre deux declenchements.
            monotonic: Source de temps monotone, injectee par les tests.
            sleep: Fonction d'attente, injectee par les tests.

        Raises:
            ValueError: Si la periode n'est pas strictement positive.
        """
        if interval_seconds <= 0:
            raise ValueError(
                f"interval_seconds must be strictly positive, received {interval_seconds}"
            )

        self._interval_seconds = interval_seconds
        # Horloge monotone et non horloge murale : un ajustement NTP ou un passage a
        # l'heure d'ete ferait sauter ou figer la boucle.
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep
        self._anchor: Optional[float] = None
        self._next_index = 0

    def wait_for_next_tick(self) -> SchedulerTick:
        """Attend le prochain declenchement et le decrit.

        Le premier appel rend la main immediatement : un collecteur doit produire des
        son demarrage. Si le cycle precedent a deborde d'une ou plusieurs periodes, les
        declenchements manques sont abandonnes plutot qu'empiles, faute de quoi la
        boucle tournerait en continu pour rattraper un retard deja pris.

        Returns:
            Le tick declenche, avec son rang, les ticks sautes et le retard constate.
        """
        if self._anchor is None:
            self._anchor = self._monotonic()
            self._next_index = 1
            return SchedulerTick(index=0, skipped_ticks=0, lateness_seconds=0.0)

        scheduled_at = self._anchor + self._next_index * self._interval_seconds
        now = self._monotonic()
        remaining_seconds = scheduled_at - now

        if remaining_seconds > 0:
            self._sleep(remaining_seconds)
            tick = SchedulerTick(
                index=self._next_index, skipped_ticks=0, lateness_seconds=0.0
            )
            self._next_index += 1
            return tick

        overshoot = -remaining_seconds
        skipped_ticks = floor(overshoot / self._interval_seconds)
        index = self._next_index + skipped_ticks
        lateness_seconds = overshoot - skipped_ticks * self._interval_seconds

        self._next_index = index + 1
        return SchedulerTick(
            index=index,
            skipped_ticks=skipped_ticks,
            lateness_seconds=lateness_seconds,
        )
