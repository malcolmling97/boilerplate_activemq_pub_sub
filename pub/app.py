import os
import stomp
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ActiveMQ Configuration from .env
ACTIVEMQ_URL = os.getenv('ACTIVEMQ_URL', 'localhost')
ACTIVEMQ_URL_SECONDARY = os.getenv('ACTIVEMQ_URL_SECONDARY', '')
ACTIVEMQ_PORT = int(os.getenv('ACTIVEMQ_PORT', 61614))
ACTIVEMQ_USER = os.getenv('ACTIVEMQ_USERNAME', 'admin')
ACTIVEMQ_PASSWORD = os.getenv('ACTIVEMQ_PASSWORD', 'admin')
ACTIVEMQ_QUEUE = os.getenv('ACTIVEMQ_QUEUE', '/queue/test')
USE_SSL = os.getenv('USE_SSL', 'true').lower() == 'true'

# Build initial broker hosts list for discovery
BROKER_HOSTS_INITIAL = [(ACTIVEMQ_URL, ACTIVEMQ_PORT)]
if ACTIVEMQ_URL_SECONDARY:
    BROKER_HOSTS_INITIAL.append((ACTIVEMQ_URL_SECONDARY, ACTIVEMQ_PORT))

# This will be set to the working broker after first successful connection
WORKING_BROKER = None

# Message counter
message_counter = 0


def get_connection():
    """Create and return a STOMP connection - uses discovered working broker if known"""
    import ssl
    global WORKING_BROKER

    # If we already know which broker works, use only that one
    if WORKING_BROKER:
        broker_hosts = [WORKING_BROKER]
        conn = stomp.Connection(broker_hosts)

        if USE_SSL:
            conn.set_ssl(for_hosts=broker_hosts, ssl_version=ssl.PROTOCOL_TLS)

        conn.connect(ACTIVEMQ_USER, ACTIVEMQ_PASSWORD, wait=True)
        return conn

    # First time - try each broker individually to discover which one works
    for host, port in BROKER_HOSTS_INITIAL:
        try:
            conn = stomp.Connection([(host, port)])

            if USE_SSL:
                conn.set_ssl(for_hosts=[(host, port)], ssl_version=ssl.PROTOCOL_TLS)

            conn.connect(ACTIVEMQ_USER, ACTIVEMQ_PASSWORD, wait=True)

            # Success! Save this broker
            WORKING_BROKER = (host, port)
            print(f"[INFO] Discovered working broker: {host}:{port}")
            return conn
        except Exception as e:
            continue

    # If we get here, all brokers failed
    raise Exception("Failed to connect to any broker")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/publish', methods=['POST'])
def publish():
    """Publish a message to ActiveMQ"""
    global message_counter
    try:
        message_counter += 1
        conn = get_connection()
        
        message = f"Message #{message_counter}"
        conn.send(body=message, destination=ACTIVEMQ_QUEUE)
        
        conn.disconnect()
        
        return jsonify({
            'status': 'success',
            'message': f'Published: {message}',
            'counter': message_counter
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
