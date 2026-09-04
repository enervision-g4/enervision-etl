from datetime import UTC, datetime
from typing import Any, Optional

import pytest
from psycopg.errors import ForeignKeyViolation

from enervision_consumer.load.alert_repository import insert_if_new
from enervision_consumer.load.errors import UnknownSiteReferenceError
from enervision_contracts.envelope import AlertPayload

RAISED_AT = datetime(2024, 6, 15, 14, 12, tzinfo=UTC)


def build_alert(
    value_kw: Optional[float] = 812.5,
    threshold_kw: Optional[float] = 720.0,
) -> AlertPayload:
    return AlertPayload(
        site_id="SITE002",
        timestamp=RAISED_AT,
        source_alert_id="ALR-SITE002-1718458320",
        severity="critical",
        type="outage",
        message="Risque de surcharge",
        value_kw=value_kw,
        threshold_kw=threshold_kw,
    )


def test_an_alert_is_written_with_the_identifier_of_its_source(connection: Any) -> None:
    insert_if_new(connection, build_alert())

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[0] == "ALR-SITE002-1718458320"
    assert parametres[1] == "SITE002"
    assert parametres[2] == RAISED_AT
    assert parametres[3] == "critical"
    assert parametres[4] == "outage"


def test_the_technical_identifier_is_left_to_the_database(connection: Any) -> None:
    # alert_id est un UUID genere a l'insertion, attendu ainsi par enervision-api.
    # L'identifiant de l'API source vit dans sa propre colonne.
    insert_if_new(connection, build_alert())

    assert "alert_id" not in connection.opened_cursor.statements[0].replace(
        "source_alert_id", ""
    )


def test_an_alert_still_active_is_absorbed_at_each_cycle(connection: Any) -> None:
    # Ici le rejeu n'est pas accidentel : le collecteur interroge /api/v1/alerts a
    # chaque cycle et l'API renvoie la meme alerte tant qu'elle n'est pas resolue.
    # Sans cette contrainte, une alerte d'une heure creerait soixante lignes.
    insert_if_new(connection, build_alert())

    statement = connection.opened_cursor.statements[0]
    assert 'ON CONFLICT (source_alert_id, "timestamp") DO NOTHING' in statement
    assert "DO UPDATE" not in statement


def test_an_alert_without_measured_value_keeps_its_nulls(connection: Any) -> None:
    insert_if_new(connection, build_alert(value_kw=None, threshold_kw=None))

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[-2] is None
    assert parametres[-1] is None


def test_an_alert_whose_site_is_unknown_names_that_site(failing_connection: Any) -> None:
    connection = failing_connection(ForeignKeyViolation("site absent"))

    with pytest.raises(UnknownSiteReferenceError) as rejet:
        insert_if_new(connection, build_alert())

    assert rejet.value.site_id == "SITE002"
