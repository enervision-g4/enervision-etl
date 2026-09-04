from datetime import UTC, datetime
from typing import Any, Optional
from uuid import UUID

import pytest
from psycopg.errors import ForeignKeyViolation

from enervision_consumer.load.errors import UnknownSiteReferenceError
from enervision_consumer.load.measure_imputed_repository import find_raw_id, insert_if_new
from enervision_contracts.envelope import MeasureImputedPayload
from enervision_contracts.imputed_reading import ImputationMethod

MEASURED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)
RAW_ID = UUID("2f1c8b3a-5d47-4e21-9a6f-0c3b7e8d1a52")


def build_imputed(
    consumption_kw: Optional[float] = 542.1,
    method: ImputationMethod = ImputationMethod.FORWARD_FILL,
) -> MeasureImputedPayload:
    return MeasureImputedPayload(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        consumption_kw=consumption_kw,
        consumption_kwh=consumption_kw,
        voltage_v=398.5,
        current_a=826.4,
        power_factor=0.921,
        temperature_celsius=18.3,
        humidity_percent=62.1,
        imputation_method=method,
    )


def test_the_raw_counterpart_is_found_by_the_business_key(connection_returning: Any) -> None:
    # Le collecteur n'envoie jamais measure_raw_id : il est genere a l'insertion de la
    # ligne brute. Seul (site_id, timestamp) permet de la retrouver.
    connection = connection_returning((RAW_ID,))

    trouve = find_raw_id(connection, "SITE002", MEASURED_AT)

    assert trouve == RAW_ID
    assert connection.opened_cursor.parameters[0] == ("SITE002", MEASURED_AT)


def test_an_absent_raw_counterpart_is_reported_without_raising(
    connection_returning: Any,
) -> None:
    # Les topics n'ont aucun ordre entre eux : la mesure brute peut arriver apres. Le
    # depot le constate, mais c'est a l'orchestration de decider d'attendre.
    connection = connection_returning(None)

    assert find_raw_id(connection, "SITE002", MEASURED_AT) is None


def test_an_imputed_measure_carries_the_identifier_of_its_raw_row(connection: Any) -> None:
    insert_if_new(connection, build_imputed(), RAW_ID)

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[0] == RAW_ID
    assert parametres[1] == "SITE002"
    assert parametres[2] == MEASURED_AT


def test_an_imputed_measure_records_the_method_that_produced_it(connection: Any) -> None:
    insert_if_new(connection, build_imputed(method=ImputationMethod.LINEAR_INTERPOLATION), RAW_ID)

    assert connection.opened_cursor.parameters[0][-1] == "linear_interpolation"


def test_a_replayed_imputed_measure_never_overwrites_the_first_write(connection: Any) -> None:
    insert_if_new(connection, build_imputed(), RAW_ID)

    statement = connection.opened_cursor.statements[0]
    assert 'ON CONFLICT (site_id, "timestamp") DO NOTHING' in statement
    assert "DO UPDATE" not in statement


def test_an_irrecoverable_value_stays_null(connection: Any) -> None:
    # Quand aucune strategie ne s'applique, la ligne imputee existe mais reste vide :
    # la tracabilite du trou vaut mieux qu'une valeur inventee.
    irrecuperable = build_imputed(consumption_kw=None, method=ImputationMethod.NONE)

    insert_if_new(connection, irrecuperable, RAW_ID)

    parametres = connection.opened_cursor.parameters[0]
    assert parametres[3] is None
    assert parametres[-1] == "none"


def test_an_imputed_measure_without_known_site_names_that_site(failing_connection: Any) -> None:
    connection = failing_connection(ForeignKeyViolation("site absent"))

    with pytest.raises(UnknownSiteReferenceError) as rejet:
        insert_if_new(connection, build_imputed(), RAW_ID)

    assert rejet.value.site_id == "SITE002"
