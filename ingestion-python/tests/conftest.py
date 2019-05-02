# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from time import sleep
from typing import Any, Generator, List, Union
import grpc
import logging
import os
import psutil
import pytest
import subprocess
import sys

# importing from private module _pytest for types only
import _pytest.config
import _pytest.fixtures
import _pytest.nodes

PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST"


def pytest_collection_modifyitems(
    config: _pytest.config.Config, items: List[_pytest.nodes.Item]
):
    """Skip load tests unless a MARKEXPR is provided via -m."""
    if not config.getoption("-m"):
        skip_load = pytest.mark.skip(reason="load tests must be enabled via -m")
        for item in items:
            if "load" in item.keywords:
                item.add_marker(skip_load)


@pytest.fixture(scope="session")
def pubsub(
    request: _pytest.fixtures.SubRequest
) -> Generator[Union[str, subprocess.Popen], None, None]:
    host = os.environ[PUBSUB_EMULATOR_HOST]
    if not host:
        del os.environ[PUBSUB_EMULATOR_HOST]
        # emulator disabled, allow clients to function
        yield "google"
    elif host.endswith(":0"):
        process = subprocess.Popen([sys.executable, "-u", "-m", "ingestion.emulator.pubsub"])
        try:
            while process.poll() is None:
                ports = [
                    conn.laddr.port
                    for conn in psutil.Process(process.pid).connections()
                ]
                if ports:
                    break
                sleep(0.1)
            assert process.poll() is None  # server still running
            os.environ[PUBSUB_EMULATOR_HOST] = f"{host[:-1]}{ports.pop()}"
            yield process
        finally:
            try:
                # allow one second for graceful termination
                process.terminate()
                process.wait(1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    else:
        yield "remote"


@pytest.fixture
def default_project(pubsub: Any) -> str:
    if pubsub == "google":
        return google.auth.default()[1]
    else:
        return "test"


@pytest.fixture
def publisher(pubsub: Union[str, subprocess.Popen]) -> PublisherClient:
    return PublisherClient()


@pytest.fixture
def subscriber(pubsub: Union[str, subprocess.Popen]) -> SubscriberClient:
    # PUBSUB_EMULATOR_HOST will override a channel argument
    # so remove it in order to preserve channel options for
    # supporting large messages
    host = os.environ.pop(PUBSUB_EMULATOR_HOST, None)
    if host is None:
        return SubscriberClient()
    else:
        _subscriber = SubscriberClient(
            # transport=SubscriberGrpcTransport(
            channel=grpc.insecure_channel(
                host, options=[("grpc.max_receive_message_length", -1)]
            )
            # )
        )
        os.environ[PUBSUB_EMULATOR_HOST] = host
        return _subscriber
