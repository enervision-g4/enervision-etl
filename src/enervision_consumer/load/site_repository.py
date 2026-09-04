"""Ecriture du referentiel des sites.

Seule table du modele a etre mise a jour plutot que preservee : elle decrit un etat
courant et non un fait date. Un site qui change de statut doit se refleter en base, la
ou une mesure rejouee ne doit jamais ecraser la premiere ecriture.
"""

import psycopg

from enervision_contracts.envelope import SitePayload

from .errors import PersistenceError
from .postgres_connection import ConnectionLike

UPSERT_SITE = """
    INSERT INTO site (site_id, site_type, site_name, location, capacity_kw, status)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (site_id) DO UPDATE SET
        site_type = EXCLUDED.site_type,
        site_name = EXCLUDED.site_name,
        location = EXCLUDED.location,
        capacity_kw = EXCLUDED.capacity_kw,
        status = EXCLUDED.status
"""
"""Insertion du referentiel, mise a jour si le site est deja connu."""


def upsert_site(connection: ConnectionLike, site: SitePayload) -> None:
    """Ecrit ou met a jour un site du referentiel.

    Ne valide pas la transaction : le commit appartient a l'orchestration, qui
    n'acquitte l'offset Kafka qu'apres lui.

    Args:
        connection: Connexion ouverte vers la base.
        site: Caracteristiques du site, telles que publiees par le collecteur.

    Raises:
        PersistenceError: Si le pilote refuse l'ecriture.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                UPSERT_SITE,
                (
                    site.site_id,
                    site.site_type,
                    site.site_name,
                    site.location,
                    site.capacity_kw,
                    site.status,
                ),
            )
    except psycopg.Error as refused_write:
        raise PersistenceError("site", str(refused_write)) from refused_write
