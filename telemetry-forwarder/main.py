from base64 import b64decode
from heka.message_pb2 import Header, Message
from os import environ
from sanic import Sanic, response
import aiohttp
import logging
import struct
import sys
import traceback
import ujson as json

app = Sanic(__name__)
EDGE_TARGET = environ["EDGE_TARGET"]
CLIENT_SESSION = None


@app.listener("before_server_start")
async def add_loop(app, loop):
    global CLIENT_SESSION
    CLIENT_SESSION = aiohttp.ClientSession(
        loop=loop, timeout=aiohttp.ClientTimeout(total=4.5)
    )


@app.route("/", methods=["POST"])
async def publish(request):
    data = request.json["message"]["data"]
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
    for key, value in fields.items():
        if type(value) is bytes:
            logging.warning("invalid %s value %s" % (key, value.__repr__))
            return response.text("") # reject non-utf-8 header
    for i in range(2):
        try:
            await CLIENT_SESSION.post(EDGE_TARGET + uri, data=content, headers=fields)
        except aiohttp.ClientResponseError as e:
            if e.code >= 500:
                continue
            else:
                break
        except aiohttp.ClientConnectionError:
            continue
        else:
            break
    return response.text("")


@app.exception(Exception)
def server_error(request, exception):
    logging.error(traceback.format_exc())
    return response.text(traceback.format_exc(), 500)


class JsonFormatter(logging.Formatter):
    def format(self, record = None):
        return json.dumps(
            {
                key: value
                for key, value in ({} if record is None else record.__dict__).items()
                if key
                not in (
                    "pathname",
                    "process",
                    "processName",
                    "thread",
                    "threadName",
                )
                and value
            }
        ).replace("\\/", "/")


handler = logging.StreamHandler(stream=sys.stdout)
handler.set_name("console")
handler.setFormatter(JsonFormatter())
logging.root.addHandler(handler)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=environ["PORT"])
