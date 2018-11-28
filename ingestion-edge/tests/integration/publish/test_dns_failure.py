# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .helpers import IntegrationTest
import pytest
import os
import subprocess


@pytest.mark.skip("not implemented")
def test_submit_pubsub_dns(integration_test: IntegrationTest):
    zone = "pubsub.googleapis.com"
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        zone = os.environ["PUBSUB_EMULATOR_HOST"].split(":", 1)[0]
    if (
        zone
        not in subprocess.run(
            ["minidns", "list"], check=True, stdout=subprocess.PIPE
        ).stdout
    ):
        raise Exception(
            "this test requires minidns to own '%s' before the server is started" % zone
        )
    integration_test.assert_accepted_and_queued()
    subprocess.run(["minidns", "del", zone], check=True)
    integration_test.assert_flushed_and_delivered()


@pytest.mark.skip("not implemented")
def test_submit_pubsub_host_unreachable(integration_test: IntegrationTest):
    # TODO firewall deny pubsub
    integration_test.assert_accepted_and_queued()
    # TODO firewall allow pubsub
    integration_test.assert_flushed_and_delivered()
