from .helpers import App, Topic


def test_pubsub_to_pubsub(input_: Topic, output: Topic):
    with App("pubsub_to_pubsub"):
        input_.publish(b"data")
        assert [b"data"] == [item.message.data for item in output.pull()]
