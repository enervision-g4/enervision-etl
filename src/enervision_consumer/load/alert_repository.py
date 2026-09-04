"""Ecriture des alertes de consommation.

L'insertion est idempotente sur l'identifiant attribue par la source, ce qui absorbe la
remise d'un meme message par Kafka, au meme titre que pour les mesures.

Elle ne dedoublonne rien a la source, en revanche : l'instance mock fabrique une liste
d'alertes neuve a chaque appel, sans jamais reproposer les precedentes. La table accumule
donc autant d'alertes que le collecteur en releve. C'est une propriete du simulateur, pas
du pipeline.
"""

import psycopg

from enervision_contracts.envelope import AlertPayload

from .errors import PersistenceError, UnknownSiteReferenceError
from .postgres_connection import ConnectionLike

TABLE = "alert"

INSERT_ALERT = """
    INSERT INTO alert (
        source_alert_id, site_id, "timestamp", severity, type, message,
        value_kw, threshold_kw
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source_alert_id, "timestamp") DO NOTHING
"""
"""Insertion idempotente sur l'identifiant de la source, pas sur la cle technique."""


def insert_if_new(connection: ConnectionLike, alert: AlertPayload) -> None:
    """Ecrit une alerte, sans effet si elle est deja en base.

    alert_id est laisse a la base, qui l'engendre : c'est source_alert_id qui identifie
    l'alerte du point de vue metier et rend les remises multiples inoffensives.

    Args:
        connection: Connexion ouverte vers la base.
        alert: Alerte telle que publiee par le collecteur.

    Raises:
        UnknownSiteReferenceError: Si le site n'est pas encore dans le referentiel.
        PersistenceError: Si le pilote refuse l'ecriture pour une autre raison.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                INSERT_ALERT,
                (
                    alert.source_alert_id,
                    alert.site_id,
                    alert.timestamp,
                    alert.severity,
                    alert.type,
                    alert.message,
                    alert.value_kw,
                    alert.threshold_kw,
                ),
            )
    except psycopg.errors.ForeignKeyViolation as unknown_site:
        raise UnknownSiteReferenceError(TABLE, alert.site_id) from unknown_site
    except psycopg.Error as refused_write:
        raise PersistenceError(TABLE, str(refused_write)) from refused_write
