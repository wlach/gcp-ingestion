from dataclasses import dataclass
from google.cloud.pubsub import PublisherClient, SubscriberClient
import subprocess
import sys


@dataclass
class Topic:
    name: str
    project: str
    publisher: PublisherClient
    subscriber: SubscriberClient

    @property
    def topic_path(self) -> str:
        return PublisherClient.topic_path(self.project, self.name)

    @property
    def subscription_path(self) -> str:
        return SubscriberClient.subscription_path(self.project, self.name)

    def publish(self, data: bytes, **attributes) -> str:
        return self.publisher.publish(self.topic_path, data, **attributes).result()

    def pull(self, max_messages: int = 1, return_immediately: bool = False):
        return self.subscriber.pull(self.subscription_path, max_messages, return_immediately)

    def __enter__(self):
        self.publisher.create_topic(self.topic_path)
        self.subscriber.create_subscription(self.subscription_path, self.topic_path)
        return self

    def __exit__(self, *_):
        self.publisher.delete_topic(self.topic_path)
        self.subscriber.delete_subscription(self.subscription_path)


@dataclass
class App:
    submodule: str = "pubsub_to_gcs"

    def __enter__(self):
        self.process = subprocess.Popen([sys.executable, "-u", "-m", f"ingestion.sink.{self.submodule}"])
        return self

    def __exit__(self, *_):
        try:
            # allow one second for graceful termination
            self.process.terminate()
            self.process.wait(1)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
