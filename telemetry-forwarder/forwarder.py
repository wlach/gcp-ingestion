from flask import Flask, request
from os import environ
from heka.message_pb2 import Message
from base64 import b64decode
import logging
import traceback
import httplib

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
EDGE_TARGET = environ["EDGE_TARGET"]


@app.route("/", methods=["POST"])
def publish():
    body = request.get_data()
    print(body)
    push = json.loads(body)
    data = push["message"]["data"]
    raw = b64decode(data)
    msg = Message()
    msg.ParseFromString(
        #b64decode(json.loads(request.get_data())["message"]["data"])
        raw
    )
    fields = {
        field.name: field.value_string[0]
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
    conn = httplib.HTTPSConnection(EDGE_TARGET)
    conn.request('POST', uri, content, fields)
    conn.getresponse().read()
    return "", 200


@app.errorhandler(500)
def server_error(e):
    logging.exception("Uncaught Exception")
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
