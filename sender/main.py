import asyncio
import time
import os
import logging


import pika #mi mod

from dotenv import load_dotenv
#from rstream import AMQPMessage, ConfirmationStatus, Producer #mi mod

load_dotenv()

logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
   handlers=[
       logging.StreamHandler()  # This sends logs to stdout/stderr
   ]
)

logger = logging.getLogger(__name__)

#STREAM= "stream-python"   # mi mod

QUEUE   = "iot_queue"  #mi mod

MESSAGES = 16

#STREAM_RETENTION = 2000000000   #mi mod


confirmed_messages = 0

all_confirmed_messages_cond = asyncio.Condition()

# async def _on_publish_confirm_client(confirmation: ConfirmationStatus) -> None:
#    global confirmed_messages
#    if confirmation.is_confirmed:
#        confirmed_messages = confirmed_messages + 1
#        if confirmed_messages == 100:
#            async with all_confirmed_messages_cond:
#                all_confirmed_messages_cond.notify()

# async def publish():
#    user = os.getenv("USER_AMQP")
#    password = os.getenv("PASSWORD_AMQP")
#    host = os.getenv("HOST_AMQP", "rabbitmq")
#    port = int(os.getenv("PORT_AMQP", "5552"))
#    vhost = os.getenv("VHOST_AMQP", "/")

#    user = os.getenv("USER_AMQP")
#    password = os.getenv("PASSWORD_AMQP")
#    host   = os.getenv("HOST_AMQP")
#    port   = int(os.getenv("PORT_AMQP", "5672"))
#    vhost  = os.getenv("VHOST_AMQP", "/")

#    async with Producer(host, port=port, username=user, password=password, virtual_host=vhost) as producer:

#        await producer.create_stream(
#            STREAM, exists_ok=True, arguments={"max-length-bytes": STREAM_RETENTION}
#        )

#        logger.info("Publishing {} messages".format(MESSAGES))

#        for i in range(MESSAGES - 1):
#            message_to_publish= f"hola Julian dice: {i}"
#            logger.info(f"Publishing message: {message_to_publish}")
#            amqp_message = AMQPMessage(
#                body=bytes(message_to_publish, "utf-8"),
#            )

#            await producer.send(
#                stream=STREAM,
#                message=amqp_message,
#                on_publish_confirm=_on_publish_confirm_client,
#            )

#            await asyncio.sleep(0.5)

#        await asyncio.sleep(1.0)

#        amqp_message = AMQPMessage(
#            body=bytes("marker: {}".format(i + 1), "utf-8"),
#        )

#        await producer.send(
#            stream=STREAM,
#            message=amqp_message,
#            on_publish_confirm=_on_publish_confirm_client,
#        )
       

#        async with all_confirmed_messages_cond:
#            await all_confirmed_messages_cond.wait()

#        logger.info("Messages confirmed: true")
"""
Toda esta funcion de publish es una version alternativa que hizo julian
"""
def publish():
    user = os.getenv("USER_AMQP")
    passwd = os.getenv("PASSWORD_AMQP")
    host   = os.getenv("HOST_AMQP")
    port   = int(os.getenv("PORT_AMQP", "5672"))
    vhost  = os.getenv("VHOST_AMQP", "/")

    creds  = pika.PlainCredentials(user, passwd)
    params = pika.ConnectionParameters(
                host=host, port=port,
                virtual_host=vhost,
                credentials=creds)

    conn = pika.BlockingConnection(params)
    ch   = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    #for i in range(MESSAGES):
    #    body = f"hola Julian dice: {i}"
    #    ch.basic_publish(exchange="",
    #                     routing_key=QUEUE,
    #                     body=body.encode(),
    #                     properties=pika.BasicProperties(delivery_mode=2))
    #    logging.info(f"Enviado → {body}")
    #    time.sleep(0.5)
    while True:
        body = input("mensaje: ")
        if body == "quit":
            break
        
        ch.basic_publish(exchange="",
                         routing_key=QUEUE,
                         body=body.encode(),
                         properties=pika.BasicProperties(delivery_mode=2))
        logging.info(f"Enviado → {body}")
        time.sleep(0.5)
    conn.close()
    logging.info(" Sender terminado")

if __name__ == "__main__":
   time.sleep(10)   
   #asyncio.run(publish()) # mi mod
   publish()
