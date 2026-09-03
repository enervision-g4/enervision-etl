"""Arret propre sur signal.

Docker envoie SIGTERM puis, apres un delai de grace, SIGKILL. Or Python interrompt le
processus sur SIGTERM sans executer les blocs finally : la file du producer ne serait
jamais vidée et les messages en attente seraient perdus a chaque redemarrage.

Le signal ne fait donc que lever un drapeau. La boucle le consulte entre deux cycles,
termine celui qui est en cours, puis rend la main normalement.
"""

import signal
from types import FrameType
from typing import Optional

from ..logging_setup import get_logger

logger = get_logger("graceful_shutdown")

HANDLED_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class ShutdownRequest:
    """Drapeau leve par un signal d'arret, consulte par les boucles."""

    def __init__(self) -> None:
        """Prepare la demande, initialement non levee."""
        self._requested = False

    @property
    def requested(self) -> bool:
        """Indique si un arret a ete demande."""
        return self._requested

    def request(
        self,
        received_signal: Optional[int] = None,
        frame: Optional[FrameType] = None,
    ) -> None:
        """Leve le drapeau d'arret.

        La signature accepte les arguments passes par le systeme aux gestionnaires de
        signaux, ce qui permet d'utiliser directement cette methode comme gestionnaire.

        Args:
            received_signal: Numero du signal recu, absent en appel direct.
            frame: Pile d'execution interrompue, absente en appel direct.
        """
        if not self._requested:
            logger.info("arret_demande", signal=received_signal)
        self._requested = True

    def install(self) -> None:
        """Detourne SIGTERM et SIGINT vers ce drapeau."""
        for handled in HANDLED_SIGNALS:
            signal.signal(handled, self.request)
