from functools import partial
from typing import Callable
from google.cloud.pubsub_v1.types import PubsubMessage, ReceivedMessage
from dataclasses import field
from google.cloud.pubsub_v1 import SubscriberClient
from signal import signal, SIGINT, SIGTERM
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple, Union
import asyncio


def run_in_executor(executor: Executor, func: Callable, *args) -> Awaitable:
    return asyncio.get_running_loop().run_in_executor(executor, func, *args)


@dataclass(eq=False)
class SleepWrapper:
    seconds: int

    async def __aenter__(self):
        self.task = asyncio.create_task(asyncio.sleep(self.seconds))
        self.tasks.add(self.task)

    async def __aexit__(self, *_):
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        self.tasks.discard(self.task)

    def cancel(self):
        self.task.cancel()


@dataclass
class Read:
    subscription: str
    callback: Callable[[PubsubMessage], Awaitable]
    ack_deadline: Union[int, float] = 600  # 10 minutes
    ack_frequency: Union[int, float] = 300  # 5 minutes
    max_ack_ids: int = 100  # for both modify and acknowledge
    max_messages: int = 10 * 1000 * 1000  # 10 million
    max_byte_size: int = 1024 * 1024 * 1024  # 1 GiB
    max_request_batch_size: int = 100
    max_request_batch_latency: Union[int, float] = 0.1  # 100 milliseconds
    num_workers: int = 100
    running: bool = field(init=False, default=False)
    byte_size: int = field(init=False, default=0)
    sleeps: Set[SleepWrapper] = field(init=False, default_factory=set)
    client: SubscriberClient = field(init=False, default_factory=SubscriberClient)
    leases: Dict[str, int] = field(init=False, default_factory=dict)
    acks: Set[str] = field(init=False, default_factory=set)
    nacks: Set[str] = field(init=False, default_factory=set)

    def sleep(self, seconds: Union[int, float]) -> SleepWrapper:
        wrapper = SleepWrapper(seconds)
        self.sleeps.add(wrapper)
        return wrapper

    async def stop(self):
        self.running = False
        for sleep in self.sleeps:
            sleep.cancel()

    async def periodic_batched_delivery(
        self,
        action: Callable[[List[str]], None],
        ack_ids: Set[str],
        seconds: Union[int, float],
    ):
        while self.running:
            async with self.sleep(self.ack_frequency):
                ack_ids_list = list(ack_ids)
                for i in range(0, len(ack_ids_list), self.max_ack_ids):
                    end = i + self.chunk_size
                    asyncio.create_task(run_in_executor(None, action, ack_ids_list[i:end]))

    def ack(self, ack_ids: List[str]):
        self.client.ack(self.subscription, ack_ids)

    def nack(self, ack_ids: List[str]):
        self.client.modify_ack_deadline(self.subscription, ack_ids, 0)

    def renew(self, ack_ids: List[str]):
        self.client.modify_ack_deadline(self.subscription, ack_ids, self.ack_deadline)

    def pull(self) -> List[ReceivedMessage]:
        return self.client.pull(self.subscription, self.max_request_batch_size).received_messages

    def done_callback(self, ack_id: str, task: asyncio.Task):
        try:
            task.result()
        except Exception:
            self.byte_size -= self.leases.pop(ack_id, 0)
            self.nacks.add(ack_id)
        else:
            self.byte_size -= self.leases.pop(ack_id, 0)
            self.acks.add(ack_id)

    async def worker(self):
        while self.running:
            received_messages = await run_in_executor(None, self.pull)
            for item in received_messages:
                byte_size = item.message.ByteSize()
                leases[item.ack_id] = byte_size
                self.byte_size += byte_size
                asyncio.ensure_task(self.callback(item.message)).add_done_callback(partial(self._done_callback, ack_id))
            while len(leases) >= self.max_messages or self.byte_size >= self.max_byte_size:
                async with self.sleep(self.max_request_batch_latency):
                    pass

    async def start(self):
        self.queue = asyncio.Queue(self.num_workers)
        asyncio.create_task(self.periodic_batched_delivery(self.ack, self.acks))
        asyncio.create_task(self.periodic_batched_delivery(self.nack, self.nacks))
        asyncio.create_task(self.periodic_batched_delivery(self.renew, self.leases))
        for i in range(self.num_workers):
            asyncio.create_task(self.worker())
