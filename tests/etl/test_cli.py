import json
from typing import Any

import pytest
from typer.testing import CliRunner

from enervision_etl.cli import application

runner = CliRunner()


@pytest.fixture
def stub_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Neutralise l'acces reseau en remplacant le client d'API par un double."""
    from datetime import UTC, datetime, timedelta

    from enervision_contracts.alert import Alert
    from enervision_contracts.energy_reading import EnergyReading
    from enervision_contracts.site import Site

    parc = [
        Site(
            site_id=f"SITE00{index}",
            site_type="factory",
            site_name=f"Site {index}",
            location="France",
            capacity_kw=1000,
            status="active",
        )
        for index in (1, 2)
    ]

    def fetch_site_registry(self: Any) -> list[Site]:
        return parc

    def fetch_current_reading(self: Any, site_id: str) -> EnergyReading:
        return EnergyReading(
            timestamp=datetime(2026, 9, 2, 10, 0, tzinfo=UTC),
            site_id=site_id,
            site_type="factory",
            consumption_kw=500.0,
            consumption_kwh=500.0,
            voltage_v=400.0,
            current_a=800.0,
            power_factor=0.92,
            temperature_celsius=18.0,
            humidity_percent=60.0,
            null_reasons=[],
            data_quality="good",
        )

    def fetch_active_alerts(self: Any) -> list[Alert]:
        return [
            Alert(
                alert_id="ALR-SITE001-1718458320",
                timestamp=datetime(2026, 9, 2, 10, 12),
                site_id="SITE001",
                severity="critical",
                type="outage",
                message="Risque de surcharge",
                value=812.5,
                threshold=720.0,
            )
        ]

    def fetch_readings_window(
        self: Any,
        site_id: Any,
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: float = 60.0,
    ) -> list[EnergyReading]:
        return [
            fetch_current_reading(self, site_id or "SITE001").model_copy(
                update={"timestamp": datetime(2026, 9, 2, 10, 0, tzinfo=UTC)
                        + timedelta(minutes=index)}
            )
            for index in range(3)
        ]

    from enervision_etl.extract.mock_api_client import MockApiClient

    monkeypatch.setattr(MockApiClient, "fetch_site_registry", fetch_site_registry)
    monkeypatch.setattr(MockApiClient, "fetch_current_reading", fetch_current_reading)
    monkeypatch.setattr(MockApiClient, "fetch_active_alerts", fetch_active_alerts)
    monkeypatch.setattr(MockApiClient, "fetch_readings_window", fetch_readings_window)
    monkeypatch.setenv("API_MOCK_BASE_URL", "http://192.0.2.10:8000")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("SITES", "ALL")
    monkeypatch.setenv("PUBLISHER_TARGET", "stdout")


def published_lines(output: str) -> list[dict[str, Any]]:
    lignes = []
    for ligne in output.splitlines():
        ligne = ligne.strip()
        if ligne.startswith("{") and '"topic"' in ligne:
            lignes.append(json.loads(ligne))
    return lignes


def test_the_help_lists_both_commands() -> None:
    result = runner.invoke(application, ["--help"])

    assert result.exit_code == 0
    assert "collect-realtime" in result.stdout
    assert "backfill" in result.stdout


def test_collect_realtime_runs_the_requested_number_of_cycles(stub_api: None) -> None:
    result = runner.invoke(application, ["collect-realtime", "--cycles", "1"])

    assert result.exit_code == 0
    published = published_lines(result.stdout)
    assert [line["topic"] for line in published].count("enervision.measure_raw") == 2


def test_collect_realtime_publishes_the_site_registry(stub_api: None) -> None:
    result = runner.invoke(application, ["collect-realtime", "--cycles", "1"])

    topics = [line["topic"] for line in published_lines(result.stdout)]
    assert topics.count("enervision.site") == 2


def test_collect_realtime_publishes_the_active_alerts(stub_api: None) -> None:
    result = runner.invoke(application, ["collect-realtime", "--cycles", "1"])

    assert result.exit_code == 0
    published_alerts = [
        line for line in published_lines(result.stdout)
        if line["topic"] == "enervision.alert"
    ]
    assert len(published_alerts) == 1
    payload = published_alerts[0]["value"]["payload"]
    assert payload["source_alert_id"] == "ALR-SITE001-1718458320"
    assert payload["value_kw"] == 812.5


def test_backfill_publishes_no_alert(stub_api: None) -> None:
    # Le rattrapage ne porte que des mesures : l'API n'expose pas d'historique
    # d'alertes, il n'y a donc rien a rejouer sur ce topic.
    result = runner.invoke(
        application, ["backfill", "--site", "SITE002", "--hours", "1"]
    )

    topics = [line["topic"] for line in published_lines(result.stdout)]
    assert "enervision.alert" not in topics


def test_backfill_publishes_the_requested_window(stub_api: None) -> None:
    result = runner.invoke(
        application, ["backfill", "--site", "SITE002", "--hours", "1"]
    )

    assert result.exit_code == 0
    published = published_lines(result.stdout)
    assert [line["topic"] for line in published].count("enervision.measure_raw") == 3


def test_backfill_requires_a_site(stub_api: None) -> None:
    result = runner.invoke(application, ["backfill"])

    assert result.exit_code != 0


def test_an_unknown_site_fails_cleanly_without_a_traceback(
    stub_api: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from enervision_etl.extract.errors import SiteNotFoundError
    from enervision_etl.extract.mock_api_client import MockApiClient

    def refuser(self: Any, *args: Any, **kwargs: Any) -> Any:
        raise SiteNotFoundError("SITE999")

    monkeypatch.setattr(MockApiClient, "fetch_readings_window", refuser)

    result = runner.invoke(application, ["backfill", "--site", "SITE999", "--hours", "1"])

    assert result.exit_code == 1
    assert "Traceback" not in result.stdout


def test_an_incomplete_configuration_fails_with_an_explicit_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("API_MOCK_BASE_URL", raising=False)
    monkeypatch.setenv("ENERVISION_IGNORE_DOTENV", "1")

    result = runner.invoke(application, ["collect-realtime", "--cycles", "1"])

    assert result.exit_code != 0
