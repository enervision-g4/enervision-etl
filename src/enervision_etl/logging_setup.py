"""Journalisation structuree.

Un evenement par ligne JSON plutot qu'une phrase libre : les champs restent
interrogeables une fois les logs agreges, sans expression reguliere.
"""

import logging
import sys
from typing import Any

import structlog


def configure_logging(level: str = "INFO", as_json: bool = True) -> None:
    """Installe la journalisation pour tout le processus.

    Args:
        level: Seuil de journalisation, par exemple INFO ou DEBUG.
        as_json: Vrai pour une sortie JSON, faux pour un rendu lisible en console.
    """
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=level.upper())

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if as_json
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        # Sans cache : le journal figerait sinon son flux de sortie au premier
        # evenement emis, et une reconfiguration ulterieure ecrirait dans le flux
        # d'origine, muet une fois celui-ci remplace.
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Renvoie un journal structure.

    Args:
        name: Nom du composant emetteur.

    Returns:
        Le journal, pret a recevoir des evenements nommes et des champs.
    """
    journal: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return journal
