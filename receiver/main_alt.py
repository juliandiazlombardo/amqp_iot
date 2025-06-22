import os, logging, queue, threading, pika, streamlit as st
from dotenv import load_dotenv; load_dotenv()

logging.getLogger("pika").setLevel(logging.WARNING)

QUEUE = "iot_queue"
msg_q = queue.Queue()

def consume():
    creds  = pika.PlainCredentials(os.getenv("USER_AMQP"), os.getenv("PASSWORD_AMQP"))
    params = pika.ConnectionParameters(
        host=os.getenv("HOST_AMQP"),
        port=int(os.getenv("PORT_AMQP", "5672")),
        virtual_host=os.getenv("VHOST_AMQP", "/"),
        credentials=creds
    )
    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE, durable=True)
        
        for _m, _p, body in ch.consume(QUEUE, inactivity_timeout=1):
            if body:
                msg_q.put(body.decode())

thread = threading.Thread(target=consume, daemon=True)

st.title("Mensajes IoT en CloudAMQP")
if st.button("Iniciar consumidor", disabled=thread.is_alive()):
    thread.start()
    st.success("Consumidor iniciado ✅")

while not msg_q.empty():
    st.write(msg_q.get())
