# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Pubsub Server for testing."""

from google.pubsub.v1 import pubsub_grpc, pubsub_pb2
from google.protobuf import empty_pb2, json_format
from typing import Dict, List, Optional, Set
import asyncio
import grpclib.const
import grpclib.exceptions
import grpclib.server
import json
import logging
import os
import time
import uuid
import multiprocessing


class LazyFormat:
    """Container class for lazily formatting logged protobuf."""

    def __init__(self, value):
        """Initialize new container."""
        self.value = value

    def __str__(self):
        """Get str(dict(value)) without surrounding curly braces."""
        return str(json_format.MessageToDict(self.value))[1:-1]


class Subscription:
    """Container class for subscription messages."""

    def __init__(self):
        """Initialize subscription messages queue."""
        self.published = []
        self.pulled = {}
        self._condition = asyncio.Condition()

    async def notify_all(self):
        await self._condition.acquire()
        try:
            self._condition.notify_all()
        finally:
            self._condition.release()

    async def wait_for(self, min_messages):
        """Wait for at least min_messages to arrive."""
        def predicate():
            return not len(self.published) < min_messages

        await self._condition.acquire()
        try:
            await self._condition.wait_for(predicate)
        finally:
            self._condition.release()


class Publisher(pubsub_grpc.PublisherBase):
    """Publisher implementation for testing."""

    def __init__(self, logger: logging.Logger, topics: Dict[str, Set[Subscription]]):
        """Initialize a new Publisher."""
        self.logger = logger
        self.topics = topics
        self.subscriptions: Dict[str, Subscription] = {}
        self.status_codes: Dict[str, grpclib.const.Status] = {}
        self.sleep: Optional[float] = None

    async def CreateTopic(self, stream: grpclib.server.Stream):  # noqa: D403
        """CreateTopic implementation."""
        request: pubsub_pb2.Topic = await stream.recv_message()
        self.logger.debug("CreateTopic(%s)", LazyFormat(request))
        if request.name in self.topics:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.ALREADY_EXISTS, "Topic already exists"
            )
        self.topics[request.name] = set()
        await stream.send_message(request)

    async def DeleteTopic(self, stream: grpclib.server.Stream):  # noqa: D403
        """DeleteTopic implementation."""
        request: pubsub_pb2.DeleteTopicRequest = await stream.recv_message()
        self.logger.debug("DeleteTopic(%s)", LazyFormat(request))
        try:
            self.topics.pop(request.topic)
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Topic not found"
            )
        await stream.send_message(empty_pb2.Empty())

    async def Publish(self, stream: grpclib.server.Stream):
        """Publish implementation."""
        request: pubsub_pb2.PublishRequest = await stream.recv_message()
        self.logger.debug("Publish(%.100s)", LazyFormat(request))
        if request.topic in self.status_codes:
            raise grpclib.exceptions.GRPCError(
                self.status_codes[request.topic], "Override"
            )
        message_ids: List[str] = []
        try:
            subscriptions = self.topics[request.topic]
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Topic not found"
            )
        message_ids = [uuid.uuid4().hex for _ in request.messages]
        if self.sleep is not None:
            time.sleep(self.sleep)
            # return a valid response without recording messages
            return pubsub_pb2.PublishResponse(message_ids=message_ids)
        for _id, message in zip(message_ids, request.messages):
            message.message_id = _id
        for subscription in subscriptions:
            subscription.published.extend(request.messages)
            await subscription.notify_all()
        await stream.send_message(
            pubsub_pb2.PublishResponse(message_ids=message_ids)
        )

    async def UpdateTopic(self, stream: grpclib.server.Stream):
        """Repurpose UpdateTopic API for setting up test conditions.

        :param request.topic.name: Name of the topic that needs overrides.
        :param request.update_mask.paths: A list of overrides, of the form
        "key=value".

        Valid override keys are "status_code" and "sleep". An override value of
        "" disables the override.

        For the override key "status_code" the override value indicates the
        status code that should be returned with an empty response by Publish
        requests, and non-empty override values must be a property of
        `grpclib.const.Status` such as "UNIMPLEMENTED".

        For the override key "sleep" the override value indicates a number of
        seconds Publish requests should sleep before returning, and non-empty
        override values must be a valid float. Publish requests will return
        a valid response without recording messages.
        """
        request: pubsub_pb2.UpdateTopicRequest = await stream.recv_message()
        self.logger.debug("UpdateTopic(%s)", LazyFormat(request))
        for override in request.update_mask.paths:
            key, value = override.split("=", 1)
            if key.lower() in ("status", "status_code", "statuscode", "code"):
                if value:
                    try:
                        self.status_codes[request.topic.name] = getattr(
                            grpclib.const.Status, value.upper()
                        )
                    except AttributeError:
                        raise grpclib.exceptions.GRPCError(
                            grpclib.const.Status.INVALID_ARGUMENT, "Invalid status code"
                        )
                else:
                    try:
                        del self.status_codes[request.topic.name]
                    except KeyError:
                        raise grpclib.exceptions.GRPCError(
                            grpclib.const.Status.NOT_FOUND,
                            "Status code override not found",
                        )
            elif key.lower() == "sleep":
                if value:
                    try:
                        self.sleep = float(value)
                    except ValueError:
                        raise grpclib.exceptions.GRPCError(
                            grpclib.const.Status.INVALID_ARGUMENT, "Invalid sleep time"
                        )
                else:
                    self.sleep = None
            else:
                raise grpclib.exceptions.GRPCError(
                    grpclib.const.Status.NOT_FOUND, "Path not found"
                )
        await stream.send_message(request.topic)

    async def GetTopic(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ListTopics(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ListTopicSnapshots(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ListTopicSubscriptions(self, stream: grpclib.server.Stream):
        raise NotImplementedError()


class Subscriber(pubsub_grpc.SubscriberBase):
    """Subscriber implementation for testing."""

    def __init__(self, logger: logging.Logger, topics: Dict[str, Set[Subscription]]):
        """Initialize a new Subscriber."""
        self.logger = logger
        self.topics = topics
        self.subscriptions: Dict[str, Subscription] = {}

    async def CreateSubscription(self, stream: grpclib.server.Stream):  # noqa: D403
        """CreateSubscription implementation."""
        request: pubsub_pb2.Subscription = await stream.recv_message()
        self.logger.debug("CreateSubscription(%s)", LazyFormat(request))
        if request.name in self.subscriptions:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.ALREADY_EXISTS, "Subscription already exists"
            )
        elif request.topic not in self.topics:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Topic not found"
            )
        subscription = Subscription()
        self.subscriptions[request.name] = subscription
        self.topics[request.topic].add(subscription)
        await stream.send_message(request)

    async def DeleteSubscription(self, stream: grpclib.server.Stream):  # noqa: D403
        """DeleteSubscription implementation."""
        request: pubsub_pb2.DeleteSubscriptionRequest = await stream.recv_message()
        self.logger.debug("DeleteSubscription(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions.pop(request.subscription)
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Subscription not found"
            )
        for subscriptions in self.topics.values():
            subscriptions.discard(subscription)
        await stream.send_message(empty_pb2.Empty())

    async def _pull(self, subscription_name: str, max_messages: int = 100, return_immediately: bool = True):
        received_messages: List[pubsub_pb2.ReceivedMessage] = []
        try:
            subscription = self.subscriptions[subscription_name]
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Subscription not found"
            )
        max_messages = max_messages
        if not return_immediately:
            # wait up to 5 seconds for at least max_messages to arrive
            self.logger.debug("Waiting for messages")
            try:
                await asyncio.wait_for(subscription.wait_for(max_messages), timeout=5)
            except asyncio.TimeoutError:
                self.logger.debug("Timed out waiting for messages")
        messages = subscription.published[:max_messages]
        subscription.pulled.update(
            {message.message_id: message for message in messages}
        )
        for message in messages:
            try:
                subscription.published.remove(message)
            except ValueError:
                pass
        received_messages = [
            pubsub_pb2.ReceivedMessage(ack_id=message.message_id, message=message)
            for message in messages
        ]
        return received_messages

    async def Pull(self, stream: grpclib.server.Stream):
        """Pull implementation."""
        request: pubsub_pb2.PullRequest = await stream.recv_message()
        self.logger.debug("Pull(%.100s)", LazyFormat(request))
        await stream.send_message(pubsub_pb2.PullResponse(
            received_messages=await self._pull(request.subscription, request.max_messages or 100, request.return_immediately)
        ))

    def _acknowledge(self, subscription_name: str, ack_ids: List[str]):
        try:
            subscription = self.subscriptions[subscription_name]
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Subscription not found"
            )
        for ack_id in ack_ids:
            try:
                subscription.pulled.pop(ack_id)
            except KeyError:
                raise grpclib.exceptions.GRPCError(
                    grpclib.const.Status.NOT_FOUND, "Ack ID not found"
                )

    async def Acknowledge(self, stream: grpclib.server.Stream):
        """Acknowledge implementation."""
        request: pubsub_pb2.AcknowledgeRequest = await stream.recv_message()
        self.logger.debug("Acknowledge(%s)", LazyFormat(request))
        self._acknowledge(request.subscription, request.ack_ids)
        await stream.send_message(empty_pb2.Empty())

    def _modify_ack_deadline(self, subscription_name: str, ack_ids: List[str], ack_deadline_seconds: int):
        try:
            subscription = self.subscriptions[subscription_name]
        except KeyError:
            raise grpclib.exceptions.GRPCError(
                grpclib.const.Status.NOT_FOUND, "Subscription not found"
            )
        # deadline is not tracked so only handle expiration when set to 0
        if ack_deadline_seconds == 0:
            for ack_id in ack_ids:
                try:
                    # move message from pulled back to published
                    subscription.published.append(subscription.pulled.pop(ack_id))
                except KeyError:
                    raise grpclib.exceptions.GRPCError(
                        grpclib.const.Status.NOT_FOUND, "Ack ID not found"
                    )

    async def ModifyAckDeadline(self, stream: grpclib.server.Stream):  # noqa: D403
        """ModifyAckDeadline implementation."""
        request: pubsub_pb2.ModifyAckDeadlineRequest = await stream.recv_message()
        self.logger.debug("ModifyAckDeadline(%s)", LazyFormat(request))
        self._modify_ack_deadline(request.subscription, request.ack_deadline_seconds, request.ack_ids)
        await stream.send_message(empty_pb2.Empty())

    async def CreateSnapshot(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def DeleteSnapshot(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def GetSnapshot(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def GetSubscription(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ListSnapshots(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ListSubscriptions(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def ModifyPushConfig(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def Seek(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def StreamingPull(self, stream: grpclib.server.Stream):
        subscription = None
        while True:
            # async for request in stream:
            request: pubsub_pb2.StreamingPullRequest = await stream.recv_message()
            self.logger.debug("StreamingPullRequest(%s)", LazyFormat(request))
            if subscription is None:
                subscription = request.subscription
            if request.ack_ids:
                self._acknowledge(subscription, request.ack_ids)
            if request.modify_deadline_seconds:
                for ack_id, seconds in zip(request.modify_deadline_ack_ids, request.modify_deadline_seconds):
                    self._modify_ack_deadline(subscription, [ack_id], seconds)
            received_messages = await self._pull(subscription)
            if received_messages:
                response = pubsub_pb2.StreamingPullResponse(received_messages=received_messages)
                self.logger.debug("StreamingPullResponse(%s)", LazyFormat(response))
                await stream.send_message(response)

    async def UpdateSnapshot(self, stream: grpclib.server.Stream):
        raise NotImplementedError()

    async def UpdateSubscription(self, stream: grpclib.server.Stream):
        raise NotImplementedError()


async def main(
    host: str = os.environ.get("HOST", "0.0.0.0"),
    port: int = int(os.environ.get("PORT", 0)),
    topics: Optional[str] = os.environ.get("TOPICS"),
):
    """Run gRPC PubSub emulator."""
    logger = logging.getLogger("pubsub_emulator")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "DEBUG").upper()))
    topics: Dict[str, Set[Subscription]] = {
        topic: set() for topic in (json.loads(topics) if topics else [])
    }
    handlers = [Publisher(logger, topics), Subscriber(logger, topics)]
    # options=[
    #     ("grpc.max_receive_message_length", -1),
    #     ("grpc.max_send_message_length", -1),
    # ]
    server = grpclib.server.Server(handlers, loop=asyncio.get_event_loop())
    with grpclib.utils.graceful_exit([server], loop=asyncio.get_event_loop()):
        await server.start(host, port)
        for sock in server._server.sockets:
            host, port = sock.getsockname()[:2]
            logger.info(
                "Listening on %s:%d", host, port, # extras={"host": host, "port": port}
            )
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
