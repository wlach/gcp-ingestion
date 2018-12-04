from base64 import b64decode
from flask import Flask, request
from heka.message_pb2 import Header, Message
from os import environ
import logging
import requests
import struct
import traceback

app = Flask(__name__)
EDGE_TARGET = environ["EDGE_TARGET"]


@app.route("/", methods=["POST"])
def publish():
    data = request.get_json(force=True, silent=True)["message"]["data"]
    raw = b64decode(data)
    msg = Message()
    if raw[:1] == b"\x1e":
        (header_length,) = struct.unpack("<B", raw[1:2])
        h = Header()
        h.ParseFromString(raw[2 : 2 + header_length])
        if raw[2 + header_length : 3 + header_length] != b"\x1f":
            raise ValueError("Missing unit separator character")
        if 3 + header_length + h.message_length != len(raw):
            raise ValueError("Trailing data")
        # drop heka protobuf framing
        raw = raw[3 + header_length :]
    msg.ParseFromString(raw)
    fields = {
        field.name: (field.value_string or field.value_bytes)[0]
        for field in msg.fields
        if field.name
        in {
            "DNT",
            "Date",
            "User-Agent",
            "X-Forwarded-For",
            "content",
            "content-length",
            "uri",
        }
    }
    uri = fields.pop("uri", "/submit")
    content = fields.pop("content", None)
    requests.post(EDGE_TARGET + uri, data=content, headers=fields)
    return "", 200


@app.errorhandler(500)
def server_error(e):
    logging.exception("Uncaught Exception")
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
