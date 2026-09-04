"""Verification des depots contre un vrai PostgreSQL avec TimescaleDB.

Les doubles utilises par la suite unitaire enregistrent la requete sans l'executer :
ils ne peuvent donc rien dire des semantiques qui appartiennent a la base, et ce sont
justement elles que porte l'idempotence du consumer. Ces tests les exercent pour de bon.

Ils sont exclus de `uv run pytest` par defaut. Pour les lancer :

    docker run -d --name g4_test_db -e POSTGRES_USER=g4_app -e POSTGRES_PASSWORD=test \\
      -e POSTGRES_DB=g4_db -p 5433:5432 \\
      -v "$PWD/../enervision-devops/db/init:/docker-entrypoint-initdb.d:ro" \\
      timescale/timescaledb:latest-pg16

    ENERVISION_TEST_DATABASE_URL=postgres://g4_app:test@localhost:5433/g4_db \\
      uv run pytest tests/consumer/integration -m integration
"""

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Optional

import psycopg
import pytest

from enervision_consumer.load import (
    alert_repository,
    measure_imputed_repository,
    measure_raw_repository,
)
from enervision_consumer.load.errors import UnknownSiteReferenceError
from enervision_consumer.load.site_repository import upsert_site
from enervision_contracts.envelope import (
    AlertPayload,
    MeasureImputedPayload,
    MeasureRawPayload,
    SitePayload,
)
from enervision_contracts.imputed_reading import ImputationMethod

pytestmark = pytest.mark.integration

DATABASE_URL_VARIABLE = "ENERVISION_TEST_DATABASE_URL"
MEASURED_AT = datetime(2024, 6, 15, 14, 32, tzinfo=UTC)
RAISED_AT = datetime(2024, 6, 15, 14, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def database_url() -> str:
    configured_url = os.environ.get(DATABASE_URL_VARIABLE)
    if configured_url is None:
        pytest.skip(f"{DATABASE_URL_VARIABLE} absente, voir le docstring du module")
    return configured_url


@pytest.fixture
def connection(database_url: str) -> Iterator[psycopg.Connection]:
    """Ouvre une transaction annulee a la fin : aucun test ne voit les donnees d'un autre."""
    opened = psycopg.connect(database_url, autocommit=False)
    try:
        yield opened
    finally:
        opened.rollback()
        opened.close()


@pytest.fixture
def known_site(connection: psycopg.Connection) -> SitePayload:
    site = SitePayload(
        site_id="SITE002",
        site_type="factory",
        site_name="Usine Lyon Venissieux",
        location="Lyon, France",
        capacity_kw=1000,
        status="active",
    )
    upsert_site(connection, site)
    return site


def build_measure(consumption_kw: Optional[float], quality: str = "good") -> MeasureRawPayload:
    return MeasureRawPayload(
        site_id="SITE002",
        timestamp=MEASURED_AT,
        consumption_kw=consumption_kw,
        consumption_kwh=consumption_kw,
        null_reasons=[] if consumption_kw is not None else ["network_loss"],
        data_quality=quality,
    )


def build_alert(source_alert_id: str) -> AlertPayload:
    return AlertPayload(
        site_id="SITE002",
        timestamp=RAISED_AT,
        source_alert_id=source_alert_id,
        severity="critical",
        type="outage",
        message="Risque de surcharge",
        value_kw=812.5,
        threshold_kw=720.0,
    )


def scalar(connection: psycopg.Connection, statement: str) -> object:
    with connection.cursor() as cursor:
        cursor.execute(statement)
        row = cursor.fetchone()
    return row[0] if row is not None else None


def test_the_schema_carries_the_constraints_the_consumer_relies_on(
    connection: psycopg.Connection,
) -> None:
    # Sans elles, ON CONFLICT est rejete par Postgres avec l'erreur 42P10.
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT conname FROM pg_constraint
             WHERE conname IN (
                'uq_measure_raw_site_timestamp',
                'uq_measure_imputed_site_timestamp',
                'uq_alert_source_alert_id'
             )
            """
        )
        found = {row[0] for row in cursor.fetchall()}

    assert len(found) == 3, f"schema incomplet, contraintes trouvees : {sorted(found)}"


def test_a_known_site_is_updated_not_duplicated(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    upsert_site(connection, known_site.model_copy(update={"status": "maintenance"}))

    assert scalar(connection, "SELECT count(*) FROM site WHERE site_id = 'SITE002'") == 1
    assert scalar(connection, "SELECT status FROM site WHERE site_id = 'SITE002'") == "maintenance"


def test_a_replayed_measure_keeps_the_first_write(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    # L'API n'est pas deterministe : deux appels sur le meme horodatage donnent des
    # valeurs differentes. Sans DO NOTHING, la donnee stockee changerait au gre des rejeux.
    measure_raw_repository.insert_if_new(connection, build_measure(542.1))
    measure_raw_repository.insert_if_new(connection, build_measure(999.9))

    assert scalar(connection, "SELECT count(*) FROM measure_raw") == 1
    assert scalar(connection, "SELECT consumption_kw FROM measure_raw") == 542.1


def test_a_silent_sensor_is_stored_as_null_never_as_zero(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    measure_raw_repository.insert_if_new(connection, build_measure(None, "critical"))

    assert scalar(connection, "SELECT consumption_kw FROM measure_raw") is None
    assert scalar(connection, "SELECT null_reasons FROM measure_raw") == ["network_loss"]


def test_an_imputed_measure_is_correlated_by_its_business_key(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    measure_raw_repository.insert_if_new(connection, build_measure(542.1))

    correlated = measure_imputed_repository.find_raw_id(connection, "SITE002", MEASURED_AT)
    measure_imputed_repository.insert_if_new(
        connection,
        MeasureImputedPayload(
            site_id="SITE002",
            timestamp=MEASURED_AT,
            consumption_kw=None,
            imputation_method=ImputationMethod.NONE,
        ),
        correlated,
    )

    assert correlated is not None
    assert scalar(connection, "SELECT measure_raw_id FROM measure_imputed") == correlated
    assert scalar(connection, "SELECT consumption_kw FROM measure_imputed") is None


def test_an_absent_raw_measure_is_reported_without_raising(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    assert measure_imputed_repository.find_raw_id(connection, "SITE002", MEASURED_AT) is None


def test_an_alert_still_active_is_absorbed_at_each_cycle(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    alert_repository.insert_if_new(connection, build_alert("ALR-SITE002-1718458320"))
    alert_repository.insert_if_new(connection, build_alert("ALR-SITE002-1718458320"))

    assert scalar(connection, "SELECT count(*) FROM alert") == 1


def test_two_distinct_alerts_at_the_same_instant_are_both_kept(
    connection: psycopg.Connection,
    known_site: SitePayload,
) -> None:
    # C'est ce que l'identifiant de la source protege : une cle deduite de
    # (site_id, timestamp, type) aurait perdu la seconde en silence.
    alert_repository.insert_if_new(connection, build_alert("ALR-SITE002-1718458320"))
    alert_repository.insert_if_new(connection, build_alert("ALR-SITE002-1718458321"))

    assert scalar(connection, "SELECT count(*) FROM alert") == 2


def test_a_fact_referencing_an_unknown_site_is_refused(
    connection: psycopg.Connection,
) -> None:
    unknown = build_measure(542.1).model_copy(update={"site_id": "SITE999"})

    with pytest.raises(UnknownSiteReferenceError) as rejet:
        measure_raw_repository.insert_if_new(connection, unknown)

    assert rejet.value.site_id == "SITE999"
