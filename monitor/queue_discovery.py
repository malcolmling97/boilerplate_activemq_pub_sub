import os
import time
import ssl
import uuid
import stomp
from dotenv import load_dotenv

load_dotenv()

ACTIVEMQ_URL = os.getenv('ACTIVEMQ_URL', 'localhost')
ACTIVEMQ_URL_SECONDARY = os.getenv('ACTIVEMQ_URL_SECONDARY', '')
ACTIVEMQ_PORT = int(os.getenv('ACTIVEMQ_PORT', 61614))
USER = os.getenv('ACTIVEMQ_USERNAME', 'monitor')
PASSWORD = os.getenv('ACTIVEMQ_PASSWORD', 'monitor')
USE_SSL = os.getenv('USE_SSL', 'true').lower() == 'true'

BROKER_HOSTS = [(ACTIVEMQ_URL, ACTIVEMQ_PORT)]
if ACTIVEMQ_URL_SECONDARY:
    BROKER_HOSTS.append((ACTIVEMQ_URL_SECONDARY, ACTIVEMQ_PORT))

class StatisticsListener(stomp.ConnectionListener):
    def __init__(self):
        self.queues = {}
        self.topics = {}
        self.connected = False
        self.response_received = False
        self.raw_response = None

    def on_error(self, frame):
        print(f"\n[ERROR FRAME RECEIVED]")
        print(f"  Body: {frame.body}")
        print(f"  Headers: {frame.headers}")
        print("\n  ^^ This error might indicate:")
        print("     - Permission denied for Statistics destination")
        print("     - Statistics plugin not responding")
        print("     - Invalid destination format")

    def on_connected(self, frame):
        print("[INFO] Connected to broker")
        self.connected = True

    def on_disconnected(self):
        print("[INFO] Disconnected from broker")
        self.connected = False

    def on_message(self, frame):
        """
        Handle MapMessage response from StatisticsBrokerPlugin.
        AWS MQ returns statistics as XML in the message body when transformation: jms-map-xml
        """
        print("[INFO] Received statistics response")
        self.response_received = True
        self.raw_response = frame

        try:
            headers = frame.headers

            print(f"\n[DEBUG] All message headers:")
            for key, value in sorted(headers.items()):
                print(f"  {key}: {value}")

            print(f"\n[DEBUG] Message body (RAW XML):")
            print("=" * 80)
            if frame.body:
                print(frame.body)
            else:
                print("(empty)")
            print("=" * 80)

            # AWS MQ returns MapMessage as XML when transformation: jms-map-xml
            if frame.body and '<map>' in frame.body:
                # Parse XML to extract statistics
                import xml.etree.ElementTree as ET
                root = ET.fromstring(frame.body)

                # Extract all map entries
                for entry in root.findall('.//entry'):
                    key = entry.find('string')
                    value_elem = entry.find('./*[2]')  # Second child is the value

                    if key is not None and value_elem is not None:
                        key_name = key.text
                        value = value_elem.text

                        print(f"[STAT] {key_name}: {value}")

                        # Parse destination-specific stats
                        # Keys like: queue.queue1.size, queue.queue1.enqueueCount, etc.
                        if key_name.startswith('queue.'):
                            parts = key_name.split('.', 2)
                            if len(parts) >= 3:
                                queue_name = parts[1]
                                metric = parts[2]

                                if queue_name not in self.queues:
                                    self.queues[queue_name] = {}
                                self.queues[queue_name][metric] = value

                        elif key_name.startswith('topic.'):
                            parts = key_name.split('.', 2)
                            if len(parts) >= 3:
                                topic_name = parts[1]
                                metric = parts[2]

                                if topic_name not in self.topics:
                                    self.topics[topic_name] = {}
                                self.topics[topic_name][metric] = value

        except Exception as e:
            print(f"[WARN] Error processing statistics response: {e}")
            import traceback
            traceback.print_exc()

def discover_destinations():
    """
    Discover all queues and topics using StatisticsBrokerPlugin request/response pattern.

    The correct pattern is:
    1. Subscribe to a temporary reply queue
    2. Send a request message to ActiveMQ.Statistics.Broker with reply-to header
    3. Receive MapMessage response on reply queue
    4. Parse the MapMessage (data is in headers, not body)
    """
    broker_list = ', '.join([f"{h}:{p}" for h, p in BROKER_HOSTS])
    print(f"\n[INFO] Starting destination discovery (SSL: {USE_SSL})")
    print(f"[INFO] Broker(s): {broker_list}")
    print(f"[INFO] Using credentials for user: {USER}")
    print("=" * 80)

    listener = StatisticsListener()
    conn = stomp.Connection(BROKER_HOSTS, heartbeats=(10000, 10000))

    if USE_SSL:
        conn.set_ssl(for_hosts=BROKER_HOSTS, ssl_version=ssl.PROTOCOL_TLS)

    conn.set_listener('statistics', listener)

    try:
        print("[INFO] Connecting to broker...")
        conn.connect(USER, PASSWORD, wait=True, headers={'heart-beat': '10000,10000'})

        # Create unique reply queue for this session
        reply_queue = f'/temp-queue/stats.reply.{uuid.uuid4().hex[:8]}'
        print(f"[INFO] Created reply queue: {reply_queue}")

        # Subscribe to reply queue FIRST (before sending request)
        print(f"[INFO] Subscribing to reply queue...")
        conn.subscribe(destination=reply_queue, id=1, ack='auto')
        print("[INFO] Subscribed to reply queue")

        # Send request to StatisticsBrokerPlugin
        # Request destination can be:
        # - ActiveMQ.Statistics.Broker (for broker-level stats)
        # - ActiveMQ.Statistics.Destination.<queue-name> (for specific destination)
        # - ActiveMQ.Statistics.Destination.> (wildcard for all destinations?)

        # Try multiple statistics destinations
        statistics_destinations = [
            'ActiveMQ.Statistics.Destination.>',  # Wildcard - try this first
            'ActiveMQ.Statistics.Broker',         # Broker-level stats
        ]

        for idx, statistics_destination in enumerate(statistics_destinations, start=1):
            print(f"\n[INFO] Attempt {idx}/{len(statistics_destinations)}")
            print(f"[INFO] Sending statistics request to: {statistics_destination}")
            print(f"[INFO] Reply-to: {reply_queue}")

            # Reset response flag for each attempt
            listener.response_received = False
            listener.raw_response = None

            try:
                conn.send(
                    body='',  # Empty body
                    destination=statistics_destination,
                    headers={'reply-to': reply_queue}
                )
                print("[SUCCESS] Request sent successfully")
            except Exception as send_error:
                print(f"[ERROR] Failed to send statistics request: {send_error}")
                print("  This likely means:")
                print("    - User doesn't have permission to write to Statistics destinations")
                print("    - Statistics destination doesn't exist/isn't accessible")
                continue  # Try next destination instead of failing

            print("[INFO] Waiting for response...")

            # Wait for response (should be quick, usually under 1 second)
            timeout = 10
            for i in range(timeout, 0, -1):
                if listener.response_received:
                    print(f"\n[INFO] Response received!")
                    break
                print(f"[INFO] Waiting for response... {i} seconds remaining", end='\r')
                time.sleep(1)

            print("\n")

            # If we got a response with queue/topic data, stop trying other destinations
            if listener.queues or listener.topics:
                print(f"[SUCCESS] Found destination statistics with: {statistics_destination}")
                break
            elif listener.response_received:
                print(f"[INFO] Response received but no queue/topic stats found. Trying next destination...")
            else:
                print(f"[WARN] No response received. Trying next destination...")

        if not listener.response_received:
            print("[WARN] No response received within timeout period")
            print("\nTroubleshooting:")
            print("  1. Verify StatisticsBrokerPlugin is enabled in broker configuration")
            print("  2. Check user permissions for ActiveMQ.Statistics.* destinations")
            print("  3. Ensure broker is accessible and responsive")

        return listener

    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if conn.is_connected():
            conn.disconnect()

def print_results(listener):
    """Print discovered queues and topics in a formatted way"""
    print("=" * 80)
    print("DISCOVERY RESULTS")
    print("=" * 80)

    if listener.queues:
        print(f"\n📋 QUEUES ({len(listener.queues)} found):\n")

        for queue_name, stats in sorted(listener.queues.items()):
            # Filter out advisory queues for cleaner output
            if not queue_name.startswith('ActiveMQ.'):
                print(f"\nQueue: {queue_name}")
                print("-" * 40)
                for key, value in sorted(stats.items()):
                    print(f"  {key}: {value}")
    else:
        print("\n[INFO] No queues discovered.")

    if listener.topics:
        print(f"\n📡 TOPICS ({len(listener.topics)} found):\n")

        for topic_name, stats in sorted(listener.topics.items()):
            # Filter out advisory topics for cleaner output
            if not topic_name.startswith('ActiveMQ.'):
                print(f"\nTopic: {topic_name}")
                print("-" * 40)
                for key, value in sorted(stats.items()):
                    print(f"  {key}: {value}")
    else:
        print("\n[INFO] No topics discovered.")

    if not listener.queues and not listener.topics:
        print("\n⚠️  No destinations discovered.")
        print("\nPossible reasons:")
        print("  1. StatisticsBrokerPlugin may not be enabled on the broker")
        print("  2. User lacks permissions to send to ActiveMQ.Statistics.* destinations")
        print("  3. No destinations have been created yet")
        print("  4. Broker didn't respond to statistics request")

    print("\n" + "=" * 80 + "\n")

def main():
    print("\n" + "=" * 80)
    print("AWS MQ / ActiveMQ Queue Discovery Tool")
    print("(Using StatisticsBrokerPlugin Request/Response Pattern)")
    print("=" * 80)

    listener = discover_destinations()

    if listener:
        print_results(listener)
    else:
        print("[ERROR] Failed to connect to broker")

if __name__ == "__main__":
    main()
