import os
import stomp
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ActiveMQ Configuration from .env
ACTIVEMQ_HOST = os.getenv('ACTIVEMQ_HOST', 'localhost')
ACTIVEMQ_PORT = int(os.getenv('ACTIVEMQ_PORT', 61614))
ACTIVEMQ_USER = os.getenv('ACTIVEMQ_USERNAME', 'admin')  # Changed to match .env
ACTIVEMQ_PASSWORD = os.getenv('ACTIVEMQ_PASSWORD', 'admin')
ACTIVEMQ_QUEUE = os.getenv('ACTIVEMQ_QUEUE', '/queue/test')
USE_SSL = os.getenv('USE_SSL', 'true').lower() == 'true'

# Message counter
message_counter = 0


def get_connection():
    """Create and return a STOMP connection"""
    import ssl
    
    conn = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
    
    # Configure SSL if enabled
    if USE_SSL:
        conn.set_ssl(
            for_hosts=[(ACTIVEMQ_HOST, ACTIVEMQ_PORT)],
            ssl_version=ssl.PROTOCOL_TLS  # Use TLS (auto-negotiates version)
        )
    
    conn.connect(ACTIVEMQ_USER, ACTIVEMQ_PASSWORD, wait=True)
    return conn


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
