# import os, logging, queue, threading, time, pika, streamlit as st
# from dotenv import load_dotenv; load_dotenv()




# import sys, streamlit as st, pathlib, inspect
# print("Versión:", st.__version__)
# print("Path   :", pathlib.Path(inspect.getfile(st)).parent)






# logging.getLogger("pika").setLevel(logging.WARNING)

# QUEUE = "iot_queue"
# msg_q  = queue.Queue()

# # ---------- 1 · Hilo consumidor ----------
# def consume():
#     creds  = pika.PlainCredentials(os.getenv("USER_AMQP"), os.getenv("PASSWORD_AMQP"))
#     params = pika.ConnectionParameters(
#         host=os.getenv("HOST_AMQP"),
#         port=int(os.getenv("PORT_AMQP", "5672")),
#         virtual_host=os.getenv("VHOST_AMQP", "/"),
#         credentials=creds,
#     )
#     with pika.BlockingConnection(params) as conn:
#         ch = conn.channel()
#         ch.queue_declare(queue=QUEUE, durable=True)
#         for _m, _p, body in ch.consume(QUEUE, inactivity_timeout=1):
#             if body:
#                 msg_q.put(body.decode())
#                 print("THREAD → llegó:", body.decode())

# # ---------- 2 · UI ----------
# st.title("Mensajes IoT en CloudAMQP")

# if "running" not in st.session_state:
#     st.session_state["running"]  = False
#     st.session_state["messages"] = []

# if st.button("Iniciar consumidor", key="start_btn", disabled=st.session_state["running"]):
#     threading.Thread(target=consume, daemon=True).start()
#     st.session_state["running"] = True
#     st.success("Consumidor iniciado ✅")

# # Vacía la cola y acumula mensajes en sesión
# while not msg_q.empty():
#     st.session_state["messages"].append(msg_q.get())

# # Muestra todo lo recibido
# for line in st.session_state["messages"]:
#     st.markdown(f"**DEBUG**: cargados {len(st.session_state['messages'])} mensajes")  #debugging line
#     st.write(line)

# # Auto-refresco sencillo cada segundo
# if st.session_state["running"]:
#     time.sleep(1)
#     # compatibilidad: usa el método que exista
#     if hasattr(st, "experimental_rerun"):
#         st.experimental_rerun()
#     else:            # Streamlit < 1.25
#         st.rerun()


import os, logging, queue, threading, pika, streamlit as st
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv; load_dotenv()

logging.getLogger("pika").setLevel(logging.WARNING)

QUEUE = "iot_queue"
msg_q = queue.Queue()

# ---------- Hilo consumidor ----------
def consume():
    creds  = pika.PlainCredentials(os.getenv("USER_AMQP"), os.getenv("PASSWORD_AMQP"))
    params = pika.ConnectionParameters(
        host=os.getenv("HOST_AMQP"),
        port=int(os.getenv("PORT_AMQP", "5672")),
        virtual_host=os.getenv("VHOST_AMQP", "/"),
        credentials=creds)
    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE, durable=True)
        for _m, _p, body in ch.consume(QUEUE, inactivity_timeout=1):
            if body:
                msg_q.put(body.decode())
                print("THREAD → llegó:", body.decode())   # debug en terminal

# ---------- UI ----------
st.title("Mensajes IoT en CloudAMQP")

if "running" not in st.session_state:
    st.session_state.running  = False
    st.session_state.messages = []

# Botón para lanzar el hilo UNA sola vez
if st.button("Iniciar consumidor", key="start_btn", disabled=st.session_state.running):
    threading.Thread(target=consume, daemon=True).start()
    st.session_state.running = True
    st.success("Consumidor iniciado ✅")

# Vacía la cola (lo nuevo que haya llegado)
while not msg_q.empty():
    st.session_state.messages.append(msg_q.get())

# Dibuja la lista completa
for i, line in enumerate(st.session_state.messages, 1):
    st.write(f"{i:02d}: {line}")

# Auto-refresco cada 1 s para re-ejecutar el script
if st.session_state.running:
    st_autorefresh(interval=1000, key="refresh")
