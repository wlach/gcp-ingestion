# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Reusable code for unit tests."""

from sanic import Sanic
from sanic.request import Request
from sanic.response import HTTPResponse
from typing import Optional


async def handle_request(
    app: Sanic, uri: bytes, method: str = "GET", data: Optional[bytes] = None, **headers
) -> HTTPResponse:
    """Handle a request to uri with app."""
    request = Request(uri, headers, "1.1", method, None)
    request._socket = None
    request._ip = "ip"
    if data is not None:
        request.body = data
    responses = []
    await app.handle_request(request, lambda r: responses.append(r), None)
    assert len(responses) == 1
    return responses.pop()
