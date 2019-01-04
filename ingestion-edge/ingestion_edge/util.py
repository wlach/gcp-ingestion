# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Utilities."""

from google.cloud.pubsub_v1.gapic.publisher_client import PublisherClient
from google.cloud.pubsub_v1.publisher.exceptions import PublishError
from google.cloud.pubsub_v1.types import BatchSettings, PublishRequest, PubsubMessage
import asyncio


class AsyncioBatch:
    """Batch for google.cloud.pubsub_v1.PublisherClient in asyncio.

    Work around for https://github.com/googleapis/google-cloud-python/issues/7104
    """

    @staticmethod
    def make_lock():
        """Return an asyncio lock."""
        return asyncio.Lock()

    def __init__(
        self,
        client: PublisherClient,
        topic: str,
        settings: BatchSettings,
        autocommit: bool = True,
    ):
        """Initialize."""
        self.client = client
        self.topic = topic
        self.settings = settings

        self.full = asyncio.Event()
        self.messages = []
        # fix https://github.com/googleapis/google-cloud-python/issues/7108
        self.size = PublishRequest(topic=topic, messages=[]).ByteSize()

        # Create a task to commit when full
        self.result = asyncio.create_task(self.commit())

        # If max latency is specified start a task to monitor the batch
        # and commit when max latency is reached
        if autocommit and self.settings.max_latency < float("inf"):
            asyncio.create_task(self.monitor())

    async def monitor(self):
        """Sleep until max latency is reached then set the batch to full."""
        await asyncio.sleep(self.settings.max_latency)
        self.full.set()

    async def commit(self):
        """Publish this batch.

        Wait until full then publish when current task is result.

        Set full and await result when current task is not result.
        """
        if asyncio.current_task() != self.result:
            self.full.set()
            return await self.result

        await self.full.wait()

        if not self.messages:
            return []

        response = await asyncio.get_running_loop().run_in_executor(
            None, self.client.api.publish, self.topic, self.messages
        )

        if len(response.message_ids) != len(self.messages):
            raise PublishError("Some messages not successfully published")

        return response.message_ids

    def publish(self, message: PubsubMessage):
        """Asynchronously publish message."""
        if self.full.is_set() or len(self.messages) >= self.settings.max_messages:
            return  # the batch cannot accept a message

        index = len(self.messages)
        new_size = self.size + PublishRequest(messages=[message]).ByteSize()
        overflow = (
            new_size > self.settings.max_bytes
            or index + 1 > self.settings.max_messages
        )

        if overflow:
            # fix https://github.com/googleapis/google-cloud-python/issues/7107
            if not messages:
                raise ValueError("Message exceeds max bytes")
            self.full.set()
        else:
            # Store the message in the batch.
            self.messages.append(message)
            self.size = new_size

            # return a task to await for the message id
            return asyncio.create_task(self.message_id(index))

    async def message_id(self, index: int):
        return (await self.result)[index]


class HTTP_STATUS:
    """HTTP Status Codes for responses."""

    OK = 200
    BAD_REQUEST = 400
    REQUEST_HEADER_FIELDS_TOO_LARGE = 431
    INSUFFICIENT_STORAGE = 507
