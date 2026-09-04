import io
import os
import signal
import sys
from collections.abc import Iterator

import pytest

from enervision_consumer.logging_setup import configure_logging, get_logger
from enervision_consumer.orchestration.graceful_shutdown import ShutdownRequest


@pytest.fixture
def restored_handlers() -> Iterator[None]:
    """Restaure les gestionnaires de signaux, qui sont un etat global du processus."""
    previous = {
        received: signal.getsignal(received)
        for received in (signal.SIGTERM, signal.SIGINT)
    }
    yield
    for received, handler in previous.items():
        signal.signal(received, handler)


def test_no_shutdown_is_requested_at_startup() -> None:
    assert ShutdownRequest().requested is False


def test_a_real_sigterm_raises_the_flag_instead_of_killing_the_process(
    restored_handlers: None,
) -> None:
    # Sans gestionnaire, SIGTERM interrompt le processus sans executer les blocs
    # finally : un message pourrait etre ecrit sans que son offset soit acquitte.
    request = ShutdownRequest()
    request.install()

    os.kill(os.getpid(), signal.SIGTERM)

    assert request.requested is True


def test_a_second_signal_leaves_the_flag_raised(restored_handlers: None) -> None:
    request = ShutdownRequest()
    request.install()

    os.kill(os.getpid(), signal.SIGTERM)
    os.kill(os.getpid(), signal.SIGTERM)

    assert request.requested is True


def test_a_second_configuration_writes_to_the_new_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Meme piege que cote collecteur : un journal obtenu a l'import figerait son flux
    # au premier evenement, et les suivants disparaitraient en silence.
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
