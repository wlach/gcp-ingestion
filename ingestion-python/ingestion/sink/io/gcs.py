from google.cloud.pubsub_v1.types import PubsubMessage
from google.cloud.storage import Client as GcsClient
from .pubsub import run_in_executor
from io import BufferedRandom
from os import environ
from socket import gethostname
from time import time
from typing import List
import logging

HOST = gethostname()

class Batch:
    def __init__(self, prefix: str, max_latency: int, max_messages: int, max_byte_size: int):
        self.bucket, _, self.prefix = prefix.partition("/")
        self.max_latency = max_latency
        self.max_byte_size = max_byte_size
        self.max_messages = max_messages
        self.size = 0
        self.byte_size = 0
        self.file_pointer = TemporaryFile()
        self.full = asyncio.Event()
        self.writes: List[Awaitable] = []
        self.executor = ThreadPoolExecutor(num_workers=1)
        self.done = asyncio.create_task(self.upload())

    def _upload(self):
        key = f"{self.prefix}{HOST}-{time()}.ndjson"
        self.file_pointer.seek(0)
        self.client.get_bucket(self.bucket).blob(key).upload_from_file(self.file_pointer)
        batch.close()

    async def upload(
        self,
        event: asyncio.Event,
        dest: str,
        batch: BufferedWriter,
        writes: List[Awaitable],
    ):
        try:
            await asyncio.wait_for(self.full.wait(), timeout=self.max_latency)
        except asyncio.TimeoutError:
            event.set()
        await asyncio.gather(*writes)
        await run_in_executor(self.executor, self._deliver)

    def _write(self, message: bytes):
        self.file_pointer.write(message)

    def write(self, message: bytes) -> Optional[asyncio.Task]:
        if self.full.is_set():
            return None
        new_byte_size = self.byte_size + len(value)
        new_size = self.size + 1
        if new_byte_size > self.max_byte_size or new_size > self.max_messages:
            self.full.set()
            return None
        self.byte_size = new_byte_size
        self.size = new_size
        task = asyncio.create_task(run_in_executor(self.executor, self._write, value))
        self.writes.append(task)
        return self.done


@dataclass
class Write:
    bucket: str = "gcp-ingestion-dev"
    batch_max_latency: Union[int, float] = 600  # 10 minutes
    batch_max_byte_size: Union[int, float] = 100 * 1024 * 1024  # 100 MiB
    batch_max_messages: Union[int, float] = 1000 * 1000  # 1 million
    batches: Dict[str, Tuple[asyncio.Task, asyncio.Event, BufferedWriter]] = field(
        init=False, default_factory=dict
    )
    client: GcsClient = field(init=False, default_factory=GcsClient)

    def route(self, message: PubsubMessage) -> str:
        return self.bucket + "/output"

    def callback(self, message: PubsubMessage) -> asyncio.Task:
        prefix = self.route(message)
        data = (ujson.dumps(MessageToDict(message)) + "\n").encode()
        try:
            future = self.batches[prefix].write(data)
            assert future is not None
        except (AssertionError, KeyError):
            batch = Batch(prefix, self.batch_max_latency, self.batch_max_messages, self.batch_max_byte_size)
            self.batches[prefix] = Batch
            future = batch.write(data)
            if future is None:
                raise ValueError("Single message exceeded batch limits")
        return future
