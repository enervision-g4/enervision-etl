import os
import signal
from collections.abc import Iterator

import pytest

from enervision_etl.orchestration.graceful_shutdown import ShutdownRequest


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


def test_requesting_a_shutdown_raises_the_flag() -> None:
    request = ShutdownRequest()

    request.request()

    assert request.requested is True


def test_installing_replaces_the_default_handlers(restored_handlers: None) -> None:
    request = ShutdownRequest()

    request.install()

    assert signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL
    assert signal.getsignal(signal.SIGINT) is not signal.SIG_DFL


def test_a_real_sigterm_raises_the_flag_instead_of_killing_the_process(
    restored_handlers: None,
) -> None:
    # Sans gestionnaire, SIGTERM interrompt le processus sans executer les blocs
    # finally : la file du producer ne serait jamais vidée et les messages perdus.
    request = ShutdownRequest()
    request.install()

    os.kill(os.getpid(), signal.SIGTERM)

    assert request.requested is True


def test_a_real_sigint_raises_the_flag(restored_handlers: None) -> None:
    request = ShutdownRequest()
    request.install()

    os.kill(os.getpid(), signal.SIGINT)

    assert request.requested is True


def test_a_second_signal_leaves_the_flag_raised(restored_handlers: None) -> None:
    request = ShutdownRequest()
    request.install()

    os.kill(os.getpid(), signal.SIGTERM)
    os.kill(os.getpid(), signal.SIGTERM)

    assert request.requested is True
