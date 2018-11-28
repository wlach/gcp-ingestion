# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


def test_wsgi(mocker):
    create_app = mocker.patch("ingestion_edge.create_app.create_app")
    socket = mocker.patch("socket.socket")
    socket.return_value.getsockname.return_value = ("", 0)
    from ingestion_edge import wsgi
    create_app.assert_called_once_with()
    create_app.return_value.run.assert_not_called()
    socket.assert_not_called()
    mocker.patch.object(wsgi, "__name__", "__main__")
    mocker.patch.object(wsgi, "environ", {"HOST": "HOST", "PORT": "-1"})
    wsgi.main()
    socket.assert_called_once_with()
    socket.return_value.bind.assert_called_once_with(("HOST", -1))
    create_app.return_value.run.assert_called_once_with(sock=socket.return_value)
