# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .helpers import IntegrationTest
from google.cloud.pubsub_v1 import PublisherClient
from pubsub_emulator import PubsubEmulator
from typing import Any
import pytest


def test_submit_pubsub_timeout(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub response time
    publisher.update_topic({"name": topic}, {"paths": ["sleep=120"]})
    integration_test.assert_accepted_and_queued()

    # restore pubsub response time
    publisher.update_topic({"name": topic}, {"paths": ["sleep="]})
    integration_test.assert_flushed_and_delivered()


def test_submit_pubsub_server_error(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code=internal"]})
    integration_test.assert_accepted_and_queued()

    # restore pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code="]})
    integration_test.assert_flushed_and_delivered()


# requires 11MB disk queue and single-instance server with no autoscaling
# 11MB is just big enough to not interfere with other tests
def test_submit_pubsub_server_error_disk_full(
    uses_11mb_queue: bool,
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
):
    if not uses_11mb_queue:
        pytest.skip("requires 11MB queue")
    if integration_test.uses_cluster:
        pytest.skip("requires server is not a cluster")
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code=internal"]})
    integration_test.data = b"." * 6000000  # 6 MB
    integration_test.assert_accepted_and_queued()
    integration_test.assert_rejected(status=507)

    # restore pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code="]})
    integration_test.assert_flushed_and_delivered(data=data)


@pytest.mark.skip("reclaiming the emulator port hangs indefinitely")
def test_submit_pubsub_not_listening_on_port(
    integration_test: IntegrationTest, pubsub: Any
):
    if not isinstance(pubsub, PubsubEmulator):
        pytest.skip("requires local pubsub emulator")

    pubsub.server.stop(grace=None)
    integration_test.assert_accepted_and_queued()

    pubsub.new_server(max_workers=1, port=pubsub.port)
    integration_test.assert_flushed_and_delivered()
