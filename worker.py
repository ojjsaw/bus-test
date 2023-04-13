import asyncio
from enum import Enum
import json
import logging
import uuid
import nats
import nats.js
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError

PROLONGED_TOPIC = "dlwb.workitems"
PROLONGED_STREAM = "workitems"
PROLONGED_MIRROR = "workitems-mirror"

PROGRESS_SUBJECTS = "dlwb.progress.*.*"
PROGRESS_STREAM = "progress"
PROGRESS_MIRROR = "progress-mirror"

class DlwbStatus(str, Enum):
    """Status Codes"""

    ERROR = "ERROR",
    QUEUED = "QUEUED",
    RUNNING = "RUNNING",
    COMPLETED = "COMPLETED"

async def main():
    nats_client = await nats.connect("localhost")
    js_client = nats_client.jetstream()

    workitems_config = nats.js.api.StreamConfig(
        name=PROLONGED_STREAM,
        retention=nats.js.api.RetentionPolicy.WORK_QUEUE,
        max_age=3600, #60min
        storage=nats.js.api.StorageType.FILE,
        discard=nats.js.api.DiscardPolicy.OLD,
        subjects=[PROLONGED_TOPIC]
    )

    progress_config = nats.js.api.StreamConfig(
        name=PROGRESS_STREAM,
        retention=nats.js.api.RetentionPolicy.LIMITS,
        max_age=300, #5min
        storage=nats.js.api.StorageType.FILE,
        discard=nats.js.api.DiscardPolicy.OLD,
        subjects=[PROGRESS_SUBJECTS]
    )
   
    await js_client.add_stream(config=workitems_config)
    await js_client.add_stream(config=progress_config)
    
    psub = await js_client.pull_subscribe(subject=PROLONGED_TOPIC,
                                   durable="scheduler",
                                   stream=PROLONGED_STREAM)
    while True:
        try:
            msgs = await psub.fetch(batch=1, timeout=0.1)
            for msg in msgs:
                await msg.ack()
                task_id = str(msg.metadata.sequence.stream)
                data = json.loads(msg.data.decode())
                user_id = data["user"]
                logging.info("Received task(%s) from user(%s)", task_id, user_id)
                response_topic = "dlwb.progress." + user_id + "." + task_id
                for log_count in range(300):
                    await asyncio.sleep(0.05)
                    result = str(log_count) + " [" + user_id + "]"
                    response_data = { "status": DlwbStatus.RUNNING, "result": result}
                    await js_client.publish(response_topic, json.dumps(response_data).encode())

                response_data = { "status": DlwbStatus.COMPLETED, "result": "" }
                await js_client.publish(subject=response_topic,
                                 payload=json.dumps(response_data).encode(),
                                 stream=PROGRESS_STREAM)
                logging.info("COMPLETED task(%s) from user(%s)", task_id, user_id)
        except TimeoutError:
            continue

    await psub.unsubscribe()
    await nats_client.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
