"""Ecriture des mesures reconstruites et de leur rattachement a la mesure brute.

La ligne imputee ne remplace jamais la brute : elle la double, en declarant la methode
qui l'a produite. C'est ce qui permet de distinguer plus tard une valeur mesuree d'une
valeur reconstruite, et de refaire le calcul si la strategie change.
"""

from datetime import datetime
from typing import Optional, cast
from uuid import UUID

import psycopg

from enervision_contracts.envelope import MeasureImputedPayload

from .errors import PersistenceError, UnknownSiteReferenceError
from .postgres_connection import ConnectionLike

TABLE = "measure_imputed"

SELECT_RAW_ID = """
    SELECT measure_raw_id
      FROM measure_raw
     WHERE site_id = %s AND "timestamp" = %s
"""
"""Correlation par la cle metier, seul lien disponible entre les deux tables."""

INSERT_MEASURE = """
    INSERT INTO measure_imputed (
        measure_raw_id, site_id, "timestamp", consumption_kw, consumption_kwh,
        voltage_v, current_a, power_factor, temperature_celsius, humidity_percent,
        imputation_method
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (site_id, "timestamp") DO NOTHING
"""
"""Insertion idempotente. DO NOTHING et jamais DO UPDATE : la premiere ecriture gagne."""


def find_raw_id(
    connection: ConnectionLike,
    site_id: str,
    measured_at: datetime,
) -> Optional[UUID]:
    """Retrouve l'identifiant de la mesure brute correspondante.

    Une insertion avec ON CONFLICT DO NOTHING ne renvoie rien lorsqu'elle absorbe un
    doublon : recuperer l'identifiant par RETURNING au moment d'ecrire la brute serait
    donc peu fiable, d'ou cette recherche separee.

    Args:
        connection: Connexion ouverte vers la base.
        site_id: Site concerne.
        measured_at: Horodatage de la mesure, en UTC.

    Returns:
        L'identifiant de la ligne brute, ou None si elle n'est pas encore arrivee. Ce
        n'est pas une erreur : les topics ne sont pas ordonnes entre eux, et c'est a
        l'orchestration de decider d'attendre.

    Raises:
        PersistenceError: Si le pilote refuse la lecture.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_RAW_ID, (site_id, measured_at))
            correlated_row = cursor.fetchone()
    except psycopg.Error as refused_read:
        raise PersistenceError(TABLE, str(refused_read)) from refused_read

    if correlated_row is None:
        return None
    return cast(UUID, correlated_row[0])


def insert_if_new(
    connection: ConnectionLike,
    measure: MeasureImputedPayload,
    measure_raw_id: Optional[UUID],
) -> None:
    """Ecrit une mesure reconstruite, sans effet si elle est deja en base.

    Args:
        connection: Connexion ouverte vers la base.
        measure: Mesure reconstruite telle que publiee par le collecteur.
        measure_raw_id: Identifiant de la mesure brute d'origine.

    Raises:
        UnknownSiteReferenceError: Si le site n'est pas encore dans le referentiel.
        PersistenceError: Si le pilote refuse l'ecriture pour une autre raison.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                INSERT_MEASURE,
                (
                    measure_raw_id,
                    measure.site_id,
                    measure.timestamp,
                    measure.consumption_kw,
                    measure.consumption_kwh,
                    measure.voltage_v,
                    measure.current_a,
                    measure.power_factor,
                    measure.temperature_celsius,
                    measure.humidity_percent,
                    measure.imputation_method.value,
                ),
            )
    except psycopg.errors.ForeignKeyViolation as unknown_site:
        raise UnknownSiteReferenceError(TABLE, measure.site_id) from unknown_site
    except psycopg.Error as refused_write:
        raise PersistenceError(TABLE, str(refused_write)) from refused_write
