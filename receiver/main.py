
import asyncio
import time
import os
import logging
import streamlit as st
import threading
from queue import Queue, Empty

from dotenv import load_dotenv
from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    OffsetType,
    amqp_decoder,
    OffsetSpecification
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

STREAM_NAME = "stream-python"
STREAM_RETENTION = 2000000000

# Create persistent queue that survives Streamlit reruns
@st.cache_resource
def get_message_queue():
    """Create a persistent queue that survives Streamlit reruns"""
    return Queue()

# Global variables - simple approach
message_queue = get_message_queue()  # This will always return the SAME queue object
global_consumer_running = False
global_consumer_thread = None

# Initialize session state  
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'consumer_running' not in st.session_state:
    st.session_state.consumer_running = False
if 'first_offset' not in st.session_state:
    st.session_state.first_offset = -1
if 'last_offset' not in st.session_state:
    st.session_state.last_offset = -1
if 'error_message' not in st.session_state:
    st.session_state.error_message = None

async def on_message(msg: AMQPMessage, message_context: MessageContext):
    """Handle incoming messages"""
    offset = message_context.offset
    message_body = msg.body.decode('utf-8') if isinstance(msg.body, bytes) else str(msg.body)
    
    logger.info(f"Received message: {message_body} at offset {offset}")
    
    # Put message in queue
    message_data = {
        'offset': offset,
        'message': message_body,
        'timestamp': time.time()
    }
    
    # DEBUG: Check queue identity
    logger.info(f"QUEUE DEBUG: Queue object ID before put: {id(message_queue)}")
    
    message_queue.put_nowait(message_data)
    
    # DEBUG: Verify message is actually there
    current_size = message_queue.qsize()
    logger.info(f"Message queued. Queue size now: {current_size}")
    logger.info(f"QUEUE DEBUG: Queue object ID after put: {id(message_queue)}")

async def consume_messages():
    """Consumer that actually stays running"""
    global global_consumer_running
    
    consumer = None
    try:
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        
        logger.info(f"Connecting to RabbitMQ as user: {user}")
        
        consumer = Consumer(
            host="rabbitmq",
            port=5552,
            username=user,
            password=password,
        )
        
        await consumer.start()
        
        await consumer.create_stream(
            STREAM_NAME, 
            exists_ok=True, 
            arguments={"max-length-bytes": STREAM_RETENTION}
        )
        
        logger.info("Consumer started successfully")
        
        # Subscribe
        await consumer.subscribe(
            stream=STREAM_NAME,
            callback=on_message,
            decoder=amqp_decoder,
            offset_specification=OffsetSpecification(OffsetType.FIRST, None),
        )
        
        # THIS IS THE KEY FIX: Use consumer.run() instead of a while loop
        # This keeps the consumer running until we explicitly stop it
        await consumer.run()
        
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
        message_queue.put_nowait({'type': 'error', 'message': str(e)})
    
    finally:
        logger.info("Consumer shutting down...")
        if consumer:
            try:
                await consumer.close()
                logger.info("Consumer closed")
            except:
                pass
        global_consumer_running = False

def run_consumer():
    """Run consumer in thread"""
    global global_consumer_running
    try:
        asyncio.run(consume_messages())
    except Exception as e:
        logger.error(f"Consumer thread error: {e}")
        message_queue.put_nowait({'type': 'error', 'message': str(e)})
    finally:
        global_consumer_running = False
        logger.info("Consumer thread finished")

def start_consumer():
    """Start consumer"""
    global global_consumer_running, global_consumer_thread
    
    if global_consumer_running and global_consumer_thread and global_consumer_thread.is_alive():
        logger.info("Consumer already running")
        return
    
    logger.info("Starting consumer...")
    global_consumer_running = True
    st.session_state.consumer_running = True
    
    global_consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    global_consumer_thread.start()

def stop_consumer():
    """Stop consumer"""
    global global_consumer_running
    logger.info("Stopping consumer...")
    global_consumer_running = False
    st.session_state.consumer_running = False

def process_message_queue():
    """Process messages from queue"""
    processed_count = 0
    max_batch_size = 50
    
    # DEBUG: Check queue identity and size
    logger.info(f"QUEUE DEBUG: Queue object ID in process: {id(message_queue)}")
    queue_size = message_queue.qsize()
    logger.info(f"process_message_queue() called. Queue size: {queue_size}")
    
    # DEBUG: Try to peek at queue contents without removing
    if queue_size > 0:
        logger.info("QUEUE DEBUG: Queue has messages, attempting to process...")
    
    while processed_count < max_batch_size:
        try:
            # DEBUG: Log before attempting to get message
            logger.info(f"QUEUE DEBUG: Attempting to get message {processed_count + 1}")
            
            message_data = message_queue.get_nowait()
            processed_count += 1
            
            logger.info(f"QUEUE DEBUG: Successfully got message: {message_data}")
            
            # Handle special messages
            if message_data.get('type') == 'error':
                st.session_state.error_message = message_data['message']
                st.session_state.consumer_running = False
                stop_consumer()
                continue
            
            # Regular message
            if st.session_state.first_offset == -1:
                st.session_state.first_offset = message_data['offset']
            
            st.session_state.messages.append(message_data)
            st.session_state.last_offset = message_data['offset']
            
            logger.info(f"Added message to session. Total: {len(st.session_state.messages)}")
                
        except Empty:
            logger.info("QUEUE DEBUG: Queue is empty (Empty exception)")
            break
        except Exception as e:
            logger.error(f"QUEUE DEBUG: Error processing message: {e}", exc_info=True)
            break
    
    logger.info(f"Processed {processed_count} messages")
    return processed_count > 0

def main():
    st.title("RabbitMQ Mensajería en Stream")
    st.write(f"**Stream:** {STREAM_NAME}")
    
    user = os.getenv("USER", "Not set")
    st.write(f"**Usuario de RabbitMQ:** {user}")
    
    # Control buttons
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("Iniciar consumidor", disabled=st.session_state.consumer_running):
            start_consumer()
            st.success("Iniciando consumidor...")
    
    with col2:
        if st.button("Detener consumidor", disabled=not st.session_state.consumer_running):
            stop_consumer()
            st.info("Consumidor detenido")
    
    with col3:
        if st.button("Procesar queue"):
            processed = process_message_queue()
            if processed:
                st.success(f"Mensajes procesados!")
                st.rerun()
            else:
                st.info("Sin mensajes que procesar")
    
    with col4:
        if st.button("Limpiar mensajes"):
            st.session_state.messages = []
            st.session_state.first_offset = -1
            st.session_state.last_offset = -1
            st.session_state.error_message = None
    
    with col5:
        if st.button("Checar Queue"):
            queue_size = message_queue.qsize()
            logger.info(f"MANUAL CHECK: Queue size: {queue_size}, Queue ID: {id(message_queue)}")
            st.info(f"Tamaño de queue: {queue_size}")
            if queue_size > 0:
                # Try to peek at first message without removing it
                try:
                    # This is a hack to peek at the queue
                    temp_items = []
                    for i in range(min(3, queue_size)):
                        item = message_queue.get_nowait()
                        temp_items.append(item)
                        logger.info(f"PEEK: Item {i}: {item}")
                    # Put them back
                    for item in temp_items:
                        message_queue.put_nowait(item)
                    st.success(f"Observando {len(temp_items)} mesajes")
                except Exception as e:
                    st.error(f"Error al abrir: {e}")
    
    # CRITICAL: Check queue size BEFORE processing - for auto-refresh logic
    queue_size_before = message_queue.qsize()
    
    # Always process queue FIRST
    messages_processed = process_message_queue()
    
    # Status and state sync - CRITICAL: check thread BEFORE processing
    thread_alive = global_consumer_thread is not None and global_consumer_thread.is_alive()
    
    # Update session state to match reality
    st.session_state.consumer_running = global_consumer_running or thread_alive
    
    if messages_processed:
        st.write(f"✅ **{messages_processed} mensajes procesados!**")
        st.rerun()  # Immediate refresh after processing
    
    if st.session_state.error_message:
        st.error(f"Error: {st.session_state.error_message}")
    
    if st.session_state.consumer_running or thread_alive:
        st.info(f"🟢 El consumidor está corriendo... (Estado del thread: {thread_alive}, global: {global_consumer_running})")
    else:
        st.warning("🔴 El consumidor está detenido")
    
    # Stats
    st.write(f"**Total de mensajes:** {len(st.session_state.messages)}")
    if st.session_state.first_offset != -1:
        st.write(f"**Primer offset:** {st.session_state.first_offset}")
    if st.session_state.last_offset != -1:
        st.write(f"**Último offset:** {st.session_state.last_offset}")
    
    # Show queue status prominently - CHECK CURRENT SIZE
    queue_size_after = message_queue.qsize()
    if queue_size_after > 0:
        st.error(f"🚨 {queue_size_after} MENSAJES EN QUEUE - PROCESANDO...")
    
    # Display messages
    if st.session_state.messages:
        st.subheader(f"Mensajes (Mostrando los últimos 20 de {len(st.session_state.messages)}):")
        
        recent_messages = st.session_state.messages[-20:]
        for i, msg in enumerate(reversed(recent_messages)):
            st.write(f"**{msg['offset']}**: {msg['message']}")
    else:
        st.info("Sin mensajes recibidos")
    
    # Debug info
    with st.expander("Debug Info"):
        st.write(f"Tamaño del queue antes: {queue_size_before}")
        st.write(f"Tamaño del queue después: {queue_size_after}")
        st.write(f"ID del queue: {id(message_queue)}")
        st.write(f"Mensajes de la sesión: {len(st.session_state.messages)}")
        st.write(f"Consumidor corriendo (sesión): {st.session_state.consumer_running}")
        st.write(f"Consumidor corriendo (global): {global_consumer_running}")
        st.write(f"Thread vivo: {thread_alive}")
        st.write(f"Thread: {global_consumer_thread}")
        # Debug the refresh decision
        st.write("**🔄 Auto-refresh Debug:**")
        st.write(f"- session_state.consumer_running: {st.session_state.consumer_running}")
        st.write(f"- thread_alive: {thread_alive}")
        st.write(f"- global_consumer_running: {global_consumer_running}")
        st.write(f"- queue_size_before: {queue_size_before}")
        st.write(f"- queue_size_after: {queue_size_after}")
    #     st.write(f"- SHOULD_REFRESH: {should_refresh}")
        
    #     if should_refresh:
    #         st.write("🟢 **Actualización en 0.2 segundos...**")
    #         time.sleep(0.2)  # Faster refresh
    #         st.rerun()
    #     else:
    #         st.write("🔴 **Sin actualización automática - todas las condiciones fallaron**")
    
    # # Auto-refresh logic - SIMPLIFIED and AGGRESSIVE - USE BEFORE SIZE
    # should_refresh = (
    #     st.session_state.consumer_running or 
    #     thread_alive or 
    #     global_consumer_running or
    #     queue_size_before > 0 or  # Use size BEFORE processing
    #     queue_size_after > 0      # Also check after
    # )
    
    

if __name__ == "__main__":
    main()