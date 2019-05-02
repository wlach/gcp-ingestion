from google.cloud.pubsub import PublisherClient, SubscriberClient
from .helpers import Topic
from typing import Generator
import pytest


@pytest.fixture
def input_(default_project: str, publisher: PublisherClient, subscriber: SubscriberClient) -> Generator[Topic, None, None]:
    with Topic("input", default_project, publisher, subscriber) as value:
        yield value


@pytest.fixture
def output(default_project: str, publisher: PublisherClient, subscriber: SubscriberClient) -> Generator[Topic, None, None]:
    with Topic("output", default_project, publisher, subscriber) as value:
        yield value
