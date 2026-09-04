from datetime import UTC, datetime
from typing import Any, Optional

import pytest
from psycopg.errors import ForeignKeyViolation, UniqueViolation

from enervision_consumer.load.errors import PersistenceError, UnknownSiteReferenceError
from enervision_consumer.load.measure_raw_repository import insert_if_new
from enervision_contracts.envelope import MeasureRawPayload

MEASURED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)


def build_measure(
    consumption_kw: Optional[float] = 542.1,
    null_reasons: Optional[list[str]] = None,
    data_quality: str = "good",
) -> MeasureRawPayload:
    return MeasureRawPayload(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        consumption_kw=consumption_kw,
        consumption_kwh=consumption_kw,
        voltage_v=398.5,
        current_a=826.4,
        power_factor=0.921,
        temperature_celsius=18.3,
        humidity_percent=62.1,
        null_reasons=null_reasons if null_reasons is not None else [],
        data_quality=data_quality,
    )


def test_a_measure_is_written_with_its_business_key_first(connection: Any) -> None:
    insert_if_new(connection, build_measure())

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[0] == "SITE002"
    assert parametres[1] == MEASURED_AT
    assert parametres[2] == 542.1


def test_the_technical_identifier_is_left_to_the_database(connection: Any) -> None:
    # measure_raw_id est genere a l'insertion : le collecteur ne l'envoie pas, et le
    # consumer ne l'invente pas. C'est (site_id, timestamp) qui correle les tables.
    insert_if_new(connection, build_measure())

    assert "measure_raw_id" not in connection.opened_cursor.statements[0]


def test_a_replayed_measure_never_overwrites_the_first_write(connection: Any) -> None:
    # Kafka remet au moins une fois et l'API n'est pas deterministe : deux appels sur
    # le meme horodatage donnent des valeurs differentes. La premiere gagne, sinon la
    # donnee stockee changerait au gre des rejeux.
    insert_if_new(connection, build_measure())

    statement = connection.opened_cursor.statements[0]
    assert 'ON CONFLICT (site_id, "timestamp") DO NOTHING' in statement
    assert "DO UPDATE" not in statement


def test_a_silent_sensor_is_stored_as_null_never_as_zero(connection: Any) -> None:
    insert_if_new(
        connection,
        build_measure(
            consumption_kw=None,
            null_reasons=["network_loss"],
            data_quality="critical",
        ),
    )

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[2] is None
    assert parametres[3] is None
    assert parametres[9] == ["network_loss"]
    assert parametres[10] == "critical"


def test_a_measure_whose_site_is_unknown_names_that_site(failing_connection: Any) -> None:
    # L'orchestration doit pouvoir dire quel site manque avant de redrainer le
    # referentiel sans acquitter l'offset.
    connection = failing_connection(ForeignKeyViolation("site absent"))

    with pytest.raises(UnknownSiteReferenceError) as rejet:
        insert_if_new(connection, build_measure())

    assert rejet.value.site_id == "SITE002"


def test_another_driver_error_is_not_mistaken_for_a_missing_site(
    failing_connection: Any,
) -> None:
    connection = failing_connection(UniqueViolation("contrainte inattendue"))

    with pytest.raises(PersistenceError) as rejet:
        insert_if_new(connection, build_measure())

    assert not isinstance(rejet.value, UnknownSiteReferenceError)
