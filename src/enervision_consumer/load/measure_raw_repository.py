"""Ecriture des mesures brutes, image fidele de ce qu'a renvoye l'API.

Aucune valeur n'est reinterpretee a l'insertion : un capteur muet reste un NULL
accompagne de sa cause, jamais un zero, sans quoi l'audit de fiabilite du parc
compterait des pannes comme des consommations nulles reelles.
"""

import psycopg

from enervision_contracts.envelope import MeasureRawPayload

from .errors import PersistenceError, UnknownSiteReferenceError
from .postgres_connection import ConnectionLike

TABLE = "measure_raw"

INSERT_MEASURE = """
    INSERT INTO measure_raw (
        site_id, "timestamp", consumption_kw, consumption_kwh, voltage_v, current_a,
        power_factor, temperature_celsius, humidity_percent, null_reasons, data_quality
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (site_id, "timestamp") DO NOTHING
"""
"""Insertion idempotente. DO NOTHING et jamais DO UPDATE : la premiere ecriture gagne."""


def insert_if_new(connection: ConnectionLike, measure: MeasureRawPayload) -> None:
    """Ecrit une mesure brute, sans effet si elle est deja en base.

    L'identifiant technique est laisse a la base : c'est la cle metier
    (site_id, timestamp) qui identifie une mesure et rend le rejeu inoffensif.

    Args:
        connection: Connexion ouverte vers la base.
        measure: Mesure brute telle que publiee par le collecteur.

    Raises:
        UnknownSiteReferenceError: Si le site n'est pas encore dans le referentiel.
        PersistenceError: Si le pilote refuse l'ecriture pour une autre raison.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                INSERT_MEASURE,
                (
                    measure.site_id,
                    measure.timestamp,
                    measure.consumption_kw,
                    measure.consumption_kwh,
                    measure.voltage_v,
                    measure.current_a,
                    measure.power_factor,
                    measure.temperature_celsius,
                    measure.humidity_percent,
                    list(measure.null_reasons),
                    measure.data_quality,
                ),
            )
    except psycopg.errors.ForeignKeyViolation as unknown_site:
        raise UnknownSiteReferenceError(TABLE, measure.site_id) from unknown_site
    except psycopg.Error as refused_write:
        raise PersistenceError(TABLE, str(refused_write)) from refused_write
