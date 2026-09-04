import io
import sys

import pytest

from enervision_etl.logging_setup import configure_logging, get_logger


def test_a_second_configuration_writes_to_the_new_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Le flux etait fige au premier evenement journalise. Un processus qui reconfigure
    # sa journalisation ecrivait ensuite dans le flux d'origine, muet au mieux, ferme
    # au pire, et les evenements suivants disparaissaient sans bruit.
    # Le journal est obtenu une fois pour toutes, comme le font les modules du
    # projet qui appellent get_logger au moment de leur import.
    journal = get_logger("essai")

    premier_flux = io.StringIO()
    monkeypatch.setattr(sys, "stderr", premier_flux)
    configure_logging("INFO", as_json=True)
    journal.info("premier_evenement")

    second_flux = io.StringIO()
    monkeypatch.setattr(sys, "stderr", second_flux)
    configure_logging("INFO", as_json=True)
    journal.info("second_evenement")

    assert "second_evenement" in second_flux.getvalue()
    assert "second_evenement" not in premier_flux.getvalue()
