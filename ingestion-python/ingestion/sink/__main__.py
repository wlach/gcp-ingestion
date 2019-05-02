from .io import gcs, pubsub

asyncio.run_until_complete(pubsub.Read(
    "projects/gcp-ingestion-dev/subscriptions/input",
    gcs.Write("gcp-ingestion-dev").callback
).start())
