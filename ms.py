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
    
async def is_queued(js_client: nats.js.JetStreamContext, task_id: int):
    """check if it's still in scheduler queue"""

    consumer_info = await js_client.consumer_info(PROLONGED_STREAM, "scheduler")
    last_seq = consumer_info.delivered.consumer_seq
    logging.info(last_seq)
    if int(task_id) > last_seq:
        return True
    return False

async def update_progress(js_client: nats.js.JetStreamContext, task_id: int, user_id: int):
    """get progress, returns status, logs"""
    
    status = DlwbStatus.ERROR
    logs = []
    logging.info("Retrieving task(%s) for user(%s) ...", task_id, user_id)
    response_topic = "dlwb.progress." + user_id + "." + task_id
    durable_id = user_id + "-" + task_id
    psub = await js_client.pull_subscribe(subject=response_topic,
                                            durable=durable_id,
                                            stream=PROGRESS_STREAM)
    try:
        msgs = await psub.fetch(batch=500, timeout=1)
        for msg in msgs:
            await msg.ack()
            data = json.loads(msg.data.decode())
            status = data["status"]
            logs.append(data["result"])
            if status == DlwbStatus.COMPLETED or status == DlwbStatus.ERROR:
                await js_client.purge_stream(PROGRESS_STREAM, subject=response_topic)
    except TimeoutError:
        logs.append("Unknown")

    await psub.unsubscribe()
    await js_client.delete_consumer(PROGRESS_STREAM, durable_id)

    return status, logs


async def main():
    """main program"""
    nats_client = await nats.connect("localhost")
    js_client = nats_client.jetstream()

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
            status = DlwbStatus.QUEUED
            logs = []
            if await is_queued(js_client,task_id):
                status = DlwbStatus.QUEUED
            else:
                status, logs = await update_progress(js_client, task_id, user_id)

            logging.info("%s : %s", status, len(logs))


            


    await nats_client.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
