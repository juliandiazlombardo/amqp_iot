import asyncio
import time
import os
import logging

from dotenv import load_dotenv
from rstream import AMQPMessage, ConfirmationStatus, Producer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # This sends logs to stdout/stderr
    ]
)

logger = logging.getLogger(__name__)

STREAM= "stream-python"

MESSAGES = 16

STREAM_RETENTION = 2000000000


confirmed_messages = 0

all_confirmed_messages_cond = asyncio.Condition()

async def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
    global confirmed_messages
    if confirmation.is_confirmed:
        confirmed_messages = confirmed_messages + 1
        if confirmed_messages == 100:
            async with all_confirmed_messages_cond:
                all_confirmed_messages_cond.notify()

async def publish():
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    async with Producer("rabbitmq", username=user, password=password) as producer:

        await producer.create_stream(
            STREAM, exists_ok=True, arguments={"max-length-bytes": STREAM_RETENTION}
        )

        logger.info("Publishing {} messages".format(MESSAGES))

        for i in range(MESSAGES - 1):
            message_to_publish= f"hola número: {i}"
            logger.info(f"Publishing message: {message_to_publish}")
            amqp_message = AMQPMessage(
                body=bytes(message_to_publish, "utf-8"),
            )

            await producer.send(
                stream=STREAM,
                message=amqp_message,
                on_publish_confirm=_on_publish_confirm_client,
            )

            await asyncio.sleep(0.5)

        await asyncio.sleep(1.0)

        amqp_message = AMQPMessage(
            body=bytes("marker: {}".format(i + 1), "utf-8"),
        )

        await producer.send(
            stream=STREAM,
            message=amqp_message,
            on_publish_confirm=_on_publish_confirm_client,
        )
        

        async with all_confirmed_messages_cond:
            await all_confirmed_messages_cond.wait()

        logger.info("Messages confirmed: true")


if __name__ == "__main__":
    time.sleep(10)
    
    asyncio.run(publish())
