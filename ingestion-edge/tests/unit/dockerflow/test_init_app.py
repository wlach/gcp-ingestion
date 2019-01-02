# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ..helpers import handle_request
from ingestion_edge.dockerflow import init_app
import pytest
import json


@pytest.fixture(autouse=True)
def init(app, mocker):
    mocker.patch("ingestion_edge.dockerflow.check_disk_bytes_free", lambda app, q: [])
    mocker.patch("ingestion_edge.dockerflow.check_queue_size", lambda q: [])
    init_app(app, {})


async def test_heartbeat(app):
    response = await handle_request(app, b"/__heartbeat__")
    assert response.status == 200
    assert json.loads(response.body.decode()) == {
        "status": "ok",
        "checks": {"check_disk_bytes_free": "ok", "check_queue_size": "ok"},
        "details": {},
    }


async def test_lbheartbeat(app):
    response = await handle_request(app, b"/__lbheartbeat__")
    assert response.status == 200
    assert response.body == b""


async def test_version(app):
    response = await handle_request(app, b"/__version__")
    assert response.status == 200
    assert set(json.loads(response.body.decode()).keys()) == {
        "build",
        "commit",
        "source",
        "version",
    }
