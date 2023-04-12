import asyncio
from enum import Enum
import nats
import nats.js
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import uuid
import json
import logging

PROLONGED_TOPIC = "dlwb.workitems"
PROLONGED_STREAM = "workitems"
PROLONGED_MIRROR = "workitems-mirror"

PROGRESS_SUBJECTS = "dlwb.progress.*.*"
PROGRESS_STREAM = "progress"
PROGRESS_MIRROR = "progress-mirror"

class DlwbStatus(str, Enum):
    """Status Codes"""  
    ERROR = "ERROR"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    
async def main():
    """main program"""
    nats_client = await nats.connect("localhost")
    js_client = nats_client.jetstream()

    consumer_config = nats.js.api.ConsumerConfig(inactive_threshold=0.1)

    while True:
        operation = input("Enter 's' to submit, 'p' for progress, or 'q' to quit: ")

        if operation == 'q':
            await nats_client.close()
            break

        if operation == 's':
            user_id = input("Enter user_id: ")
            data = { "user": str(user_id)}
            pub_ack = await js_client.publish(subject=PROLONGED_TOPIC,
                                              stream=PROLONGED_STREAM,
                                              payload=json.dumps(data).encode())
            task_id = pub_ack.seq
            logging.info("Submitted task(%s) for user(%s)", task_id, user_id)
        elif operation == 'p':
            user_task_id = input("Enter (task_id,user_id): ")
            items = [item.strip() for item in user_task_id.split(",")]
            user_id = items[1]
            task_id = items[0]
            logging.info("Retrieving task(%s) for user(%s) ...", task_id, user_id)
            response_topic = "dlwb.progress." + user_id + "." + task_id
            psub = await js_client.pull_subscribe(subject=response_topic,
                                                  durable=str(uuid.uuid4()),
                                                  config=consumer_config,
                                                  stream=PROGRESS_STREAM)
            try:
                msgs = await psub.fetch(batch=100, timeout=0.1)
                for msg in msgs:
                    await msg.ack()
                    data = json.loads(msg.data.decode())
                    logging.info(data)
                await psub.unsubscribe()
            except TimeoutError:
                await psub.unsubscribe()
                continue

    await nats_client.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
