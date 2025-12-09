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
        self.any_response_received = False
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
        self.any_response_received = True
        self.raw_response = frame

        try:
            # AWS MQ returns MapMessage as XML when transformation: jms-map-xml
            if frame.body and '<map>' in frame.body:
                # Parse XML to extract statistics
                import xml.etree.ElementTree as ET
                root = ET.fromstring(frame.body)

                # Extract all map entries into a dict
                stats = {}
                destination_name = None

                # Each entry has: <string>key</string><type>value</type>
                # where type can be: string, long, int, double, etc.
                for entry in root.findall('.//entry'):
                    children = list(entry)
                    if len(children) >= 2:
                        key_elem = children[0]
                        value_elem = children[1]

                        if key_elem.tag == 'string' and key_elem.text:
                            key_name = key_elem.text
                            value = value_elem.text if value_elem.text is not None else ''
                            stats[key_name] = value

                            # Extract destination name
                            if key_name == 'destinationName':
                                destination_name = value

                # If this response has a destinationName, it's a specific queue/topic
                if destination_name:
                    # Parse destination name: "queue://queue1" or "topic://topicName"
                    if destination_name.startswith('queue://'):
                        queue_name = destination_name.replace('queue://', '')
                        self.queues[queue_name] = stats
                        print(f"  -> Found queue: {queue_name} with {len(stats)} metrics")
                    elif destination_name.startswith('topic://'):
                        topic_name = destination_name.replace('topic://', '')
                        self.topics[topic_name] = stats
                        print(f"  -> Found topic: {topic_name} with {len(stats)} metrics")

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

            print("[INFO] Waiting for responses (may receive multiple)...")

            # Wait for all responses - since wildcard returns one message per destination
            # We'll wait longer and check for a pause in messages
            timeout = 15
            last_response_time = time.time()

            for i in range(timeout, 0, -1):
                if listener.response_received:
                    # Reset timer when we receive a message
                    last_response_time = time.time()
                    listener.response_received = False  # Reset to detect next message

                # If no response for 3 seconds after last one, we're probably done
                if listener.queues or listener.topics:
                    time_since_last = time.time() - last_response_time
                    if time_since_last > 3:
                        print(f"\n[INFO] No more responses for 3 seconds, assuming complete")
                        break

                print(f"[INFO] Waiting... {i}s remaining (Queues: {len(listener.queues)}, Topics: {len(listener.topics)})", end='\r')
                time.sleep(1)

            print("\n")

            # If we got a response with queue/topic data, stop trying other destinations
            if listener.queues or listener.topics:
                print(f"[SUCCESS] Found {len(listener.queues)} queues and {len(listener.topics)} topics using: {statistics_destination}")
                break
            else:
                print(f"[WARN] No destination statistics found. Trying next destination...")

        if not listener.any_response_received:
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
    print("\n" + "=" * 80)
    print("QUEUE & TOPIC STATISTICS")
    print("=" * 80)

    if listener.queues:
        print(f"\n📋 QUEUES ({len(listener.queues)} found):")
        print("=" * 80)

        for queue_name, stats in sorted(listener.queues.items()):
            # Filter out advisory queues for cleaner output
            if not queue_name.startswith('ActiveMQ.'):
                print(f"\n🔹 Queue: {queue_name}")
                print("-" * 80)

                # Show key metrics
                key_metrics = [
                    ('size', 'Queue Size (Pending)'),
                    ('enqueueCount', 'Total Enqueued'),
                    ('dequeueCount', 'Total Dequeued'),
                    ('consumerCount', 'Active Consumers'),
                    ('producerCount', 'Active Producers'),
                ]

                # Print key metrics
                found_any = False
                for key, label in key_metrics:
                    if key in stats:
                        print(f"  {label:.<35} {stats[key]}")
                        found_any = True

                if not found_any:
                    # No standard metrics found, print all stats
                    print(f"  [Available metrics: {', '.join(stats.keys())}]")
                    for key, value in sorted(stats.items()):
                        if key not in ['brokerId', 'brokerName', 'destinationName']:
                            print(f"  {key}: {value}")
    else:
        print("\n[INFO] No queues discovered.")

    if listener.topics:
        print(f"\n\n📡 TOPICS ({len(listener.topics)} found):")
        print("=" * 80)

        for topic_name, stats in sorted(listener.topics.items()):
            # Filter out advisory topics for cleaner output
            if not topic_name.startswith('ActiveMQ.'):
                print(f"\n🔹 Topic: {topic_name}")
                print("-" * 80)

                # Show key metrics first
                key_metrics = [
                    ('enqueueCount', 'Total Enqueued'),
                    ('dequeueCount', 'Total Dequeued'),
                    ('consumerCount', 'Active Consumers'),
                    ('producerCount', 'Active Producers'),
                    ('dispatchCount', 'Dispatched'),
                    ('averageEnqueueTime', 'Avg Enqueue Time (ms)'),
                    ('averageMessageSize', 'Avg Message Size (bytes)'),
                ]

                # Print key metrics
                for key, label in key_metrics:
                    if key in stats:
                        print(f"  {label:.<40} {stats[key]}")

                # Print any additional metrics not in key_metrics
                printed_keys = {k for k, _ in key_metrics}
                other_stats = {k: v for k, v in stats.items() if k not in printed_keys and k not in ['destinationName', 'brokerId', 'brokerName']}

                if other_stats:
                    print(f"\n  Other Metrics:")
                    for key, value in sorted(other_stats.items()):
                        print(f"    {key}: {value}")
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
