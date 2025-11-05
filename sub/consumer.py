import os
import time
import ssl
import stomp
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv('ACTIVEMQ_HOST', 'localhost')
PORT = int(os.getenv('ACTIVEMQ_PORT', 61614))
USER = os.getenv('ACTIVEMQ_USERNAME', 'admin')
PASSWORD = os.getenv('ACTIVEMQ_PASSWORD', 'admin')
QUEUE = os.getenv('ACTIVEMQ_QUEUE', '/queue/test-queue')
USE_SSL = os.getenv('USE_SSL', 'true').lower() == 'true'

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
    conn = stomp.Connection([(HOST, PORT)])
    if USE_SSL:
        conn.set_ssl(for_hosts=[(HOST, PORT)], ssl_version=ssl.PROTOCOL_TLS)
    conn.set_listener('', ConsumerListener())
    conn.connect(USER, PASSWORD, wait=True)
    conn.subscribe(destination=QUEUE, id=1, ack='auto', headers={'activemq.prefetchSize': '1'})
    return conn

def main():
    while True:
        try:
            conn = connect_and_subscribe()
            while conn.is_connected():
                time.sleep(10)
        except Exception as e:
            print(f"[RETRY] {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
