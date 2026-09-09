from typing import Any

import pytest
from psycopg.errors import ForeignKeyViolation

from enervision_consumer.load.errors import PersistenceError
from enervision_consumer.load.site_repository import upsert_site
from enervision_contracts.envelope import SitePayload


def build_site(status: str = "active") -> SitePayload:
    return SitePayload(
        site_id="SITE002",
        site_type="factory",
        site_name="Usine Lyon Venissieux",
        location="Lyon, France",
        capacity_kw=1000,
        status=status,
    )


def test_a_site_is_written_with_all_its_characteristics(connection: Any) -> None:
    upsert_site(connection, build_site())

    assert connection.opened_cursor.parameters[0] == (
        "SITE002",
        "factory",
        "Usine Lyon Venissieux",
        "Lyon, France",
        1000,
        "active",
    )


def test_a_known_site_is_updated_rather_than_duplicated(connection: Any) -> None:
    # Seule exception a la regle "jamais DO UPDATE" : site decrit un etat courant, pas
    # un fait date. Un site qui passe en maintenance doit se refleter en base, la ou
    # une mesure rejouee ne doit jamais ecraser la premiere ecriture.
    upsert_site(connection, build_site(status="maintenance"))

    statement = connection.opened_cursor.statements[0]
    assert "ON CONFLICT (site_id) DO UPDATE" in statement


def test_the_repository_leaves_the_transaction_to_its_caller(connection: Any) -> None:
    # L'offset Kafka ne doit etre acquitte qu'apres le commit : c'est donc
    # l'orchestration qui decide du moment, jamais le depot.
    upsert_site(connection, build_site())

    assert connection.commits == 0


def test_an_unexpected_driver_error_is_wrapped(failing_connection: Any) -> None:
    connection = failing_connection(ForeignKeyViolation("site absent"))

    with pytest.raises(PersistenceError):
        upsert_site(connection, build_site())
