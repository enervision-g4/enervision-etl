from typing import Any, Optional

import pytest


class FakeConsumerMessage:
    """Message en memoire reproduisant ce que rend le client Kafka."""

    def __init__(self, topic: str, value: Optional[bytes]) -> None:
        self._topic = topic
        self._value = value

    def topic(self) -> str:
        return self._topic

    def value(self) -> Optional[bytes]:
        return self._value

    def error(self) -> Optional[object]:
        return None


class FakeConsumer:
    """Consumer pilote par le test, qui enregistre les acquittements d'offset.

    Reproduit le comportement qui compte du client reel : poll rend le message suivant
    puis None une fois la file epuisee, et commit n'a lieu que si on l'appelle.
    """

    def __init__(
        self,
        messages: Optional[list[FakeConsumerMessage]] = None,
        journal: Optional[list[str]] = None,
    ) -> None:
        self._messages = list(messages) if messages is not None else []
        self.journal = journal if journal is not None else []
        self.subscribed: list[str] = []
        self.committed: list[FakeConsumerMessage] = []
        self.closed = False

    @property
    def exhausted(self) -> bool:
        return not self._messages

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def poll(self, timeout: float = 0) -> Optional[FakeConsumerMessage]:
        if not self._messages:
            return None
        return self._messages.pop(0)

    def commit(
        self,
        message: Optional[FakeConsumerMessage] = None,
        asynchronous: bool = True,
    ) -> None:
        if message is not None:
            self.committed.append(message)
        self.journal.append("offset")

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def consumer() -> Any:
    def build(
        messages: Optional[list[FakeConsumerMessage]] = None,
        journal: Optional[list[str]] = None,
    ) -> FakeConsumer:
        return FakeConsumer(messages, journal)

    return build
