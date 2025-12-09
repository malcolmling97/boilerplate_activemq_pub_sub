import os
import time
import ssl
import stomp
from dotenv import load_dotenv

load_dotenv()
ACTIVEMQ_URL = os.getenv('ACTIVEMQ_URL', 'localhost')
ACTIVEMQ_URL_SECONDARY = os.getenv('ACTIVEMQ_URL_SECONDARY', '')
PORT = int(os.getenv('ACTIVEMQ_PORT', 61614))
USER = os.getenv('ACTIVEMQ_USERNAME', 'admin')
PASSWORD = os.getenv('ACTIVEMQ_PASSWORD', 'admin')
QUEUE = os.getenv('ACTIVEMQ_QUEUE', '/queue/test-queue')
USE_SSL = os.getenv('USE_SSL', 'true').lower() == 'true'

# Build initial broker hosts list for discovery
BROKER_HOSTS_INITIAL = [(ACTIVEMQ_URL, PORT)]
if ACTIVEMQ_URL_SECONDARY:
    BROKER_HOSTS_INITIAL.append((ACTIVEMQ_URL_SECONDARY, PORT))

# This will be set to the working broker after first successful connection
WORKING_BROKER = None

print(f"[DEBUG] BROKER_HOSTS_INITIAL configured: {BROKER_HOSTS_INITIAL}")

class ConsumerListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print(f"[ERROR] {frame.body}")
    def on_connected(self, frame):
        print("[INFO] Connected")
    def on_disconnected(self):
        print("[INFO] Disconnected")
    def on_message(self, frame):
        print(f"[CONSUMED] {frame.body}")

def connect_and_subscribe():
    """Connect to broker - uses discovered working broker if known, otherwise tries each one"""
    global WORKING_BROKER

    # If we already know which broker works, use only that one
    if WORKING_BROKER:
        broker_hosts = [WORKING_BROKER]
        print(f"[INFO] Connecting to known working broker: {WORKING_BROKER[0]}:{WORKING_BROKER[1]} (SSL: {USE_SSL})")
        print(f"[INFO] Queue: {QUEUE}")

        conn = stomp.Connection(broker_hosts, heartbeats=(10000, 10000))
        if USE_SSL:
            conn.set_ssl(for_hosts=broker_hosts, ssl_version=ssl.PROTOCOL_TLS)
        conn.set_listener('', ConsumerListener())
        conn.connect(USER, PASSWORD, wait=True, headers={'heart-beat': '10000,10000'})
        conn.subscribe(destination=QUEUE, id=1, ack='auto', headers={'activemq.prefetchSize': '1'})
        print("[INFO] Successfully subscribed")
        return conn

    # First time - try each broker individually to discover which one works
    print(f"[INFO] Discovering working broker... (SSL: {USE_SSL})")
    print(f"[INFO] Queue: {QUEUE}")

    for host, port in BROKER_HOSTS_INITIAL:
        try:
            print(f"[INFO] Trying broker: {host}:{port}")
            conn = stomp.Connection([(host, port)], heartbeats=(10000, 10000))

            if USE_SSL:
                conn.set_ssl(for_hosts=[(host, port)], ssl_version=ssl.PROTOCOL_TLS)

            conn.set_listener('', ConsumerListener())
            conn.connect(USER, PASSWORD, wait=True, headers={'heart-beat': '10000,10000'})

            # Success! Save this broker and subscribe
            WORKING_BROKER = (host, port)
            print(f"[INFO] Discovered working broker: {host}:{port}")

            conn.subscribe(destination=QUEUE, id=1, ack='auto', headers={'activemq.prefetchSize': '1'})
            print("[INFO] Successfully subscribed")
            return conn
        except Exception as e:
            print(f"[INFO] Broker {host}:{port} failed, trying next...")
            continue

    # If we get here, all brokers failed
    raise Exception("Failed to connect to any broker")

def main():
    conn = None
    while True:
        try:
            conn = connect_and_subscribe()
            while conn.is_connected():
                time.sleep(10)
            print("[INFO] Connection lost, will retry...")
        except Exception as e:
            print(f"[RETRY] Connection failed: {e}")
            if conn:
                try:
                    conn.disconnect()
                except:
                    pass
                conn = None
        finally:
            print("[INFO] Retrying in 3 seconds...")
            time.sleep(3)

if __name__ == "__main__":
    main()
