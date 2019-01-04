# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from time import sleep
from typing import Generator

# importing from private module _pytest for types only
import _pytest.config.argparsing
import _pytest.fixtures
import grpc
import os
import psutil
import pytest
import requests
import subprocess
import sys


def pytest_addoption(parser: _pytest.config.argparsing.Parser):
    parser.addoption(
        "--server", dest="server", default=None, help="Server to run tests against"
    )
    parser.addoption(
        "--uses-11mb-queue",
        action="store_true",
        dest="uses_11mb_queue",
        default=False,
        help="Indicate --server has an 11MB volume for queue storage",
    )
    parser.addoption(
        "--uses-cluster",
        action="store_true",
        dest="uses_cluster",
        default=False,
        help="Indicate --server has more than one process",
    )
    parser.addoption(
        "--uses-nginx",
        action="store_true",
        dest="uses_nginx",
        default=False,
        help="Indicate --server is behind nginx",
    )
    parser.addoption(
        "--no-verify",
        action="store_false",
        dest="verify",
        default=True,
        help="Don't verify SSL certs",
    )


@pytest.fixture(scope="session")
def pubsub(request: _pytest.fixtures.SubRequest) -> Generator[str, None, None]:
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        yield "remote"
    elif request.config.getoption("server") is None:
        os.environ["PORT"] = "0"
        process = subprocess.Popen([sys.executable, "-u", "-m", "pubsub_emulator"])
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
            os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:%d" % ports.pop()
            yield str(process.pid)
        finally:
            try:
                # give the process one second to terminate gracefully and then kill it
                process.terminate()
                process.wait(1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    else:
        yield "google"


@pytest.fixture
def publisher(pubsub: str) -> PublisherClient:
    return PublisherClient()


@pytest.fixture
def subscriber(pubsub: str) -> SubscriberClient:
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        host = os.environ["PUBSUB_EMULATOR_HOST"]
        try:
            # PUBSUB_EMULATOR_HOST will override a channel argument
            # so remove it in order to preserve channel options for
            # supporting large messages
            del os.environ["PUBSUB_EMULATOR_HOST"]
            return SubscriberClient(
                channel=grpc.insecure_channel(
                    host, options=[("grpc.max_receive_message_length", -1)]
                )
            )
        finally:
            os.environ["PUBSUB_EMULATOR_HOST"] = host
    else:
        return SubscriberClient()


@pytest.fixture(scope="session")
def server(
    pubsub: str, request: _pytest.fixtures.SubRequest
) -> Generator[str, None, None]:
    _server = request.config.getoption("server")
    if _server is None:
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", "ingestion_edge.wsgi"], env=os.environ.copy()
        )
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
            yield "http://localhost:%d" % ports.pop()
        finally:
            try:
                # give the process one second to terminate gracefully and then kill it
                process.terminate()
                process.wait(1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    else:
        yield _server


@pytest.fixture
def requests_session(request: _pytest.fixtures.SubRequest) -> requests.Session:
    session = requests.Session()
    session.verify = request.config.getoption("verify")
    return session


@pytest.fixture
def uses_11mb_queue(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("uses_11mb_queue")


@pytest.fixture
def uses_cluster(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("uses_cluster")


@pytest.fixture
def uses_nginx(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("uses_nginx")
