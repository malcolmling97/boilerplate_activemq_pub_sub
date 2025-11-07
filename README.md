# ActiveMQ Pub/Sub Demo

Simple ActiveMQ publisher/subscriber deployment using Docker Compose.

## Quick Start

1. **Create environment files:**
   
   Create `pub/.env` (common settings for all publishers):
   ```bash
   ACTIVEMQ_HOST=your-broker-host
   ACTIVEMQ_PORT=61614
   ACTIVEMQ_USERNAME=admin
   ACTIVEMQ_PASSWORD=admin
   USE_SSL=true
   ```
   
   Create `sub/.env` (common settings for all subscribers):
   ```bash
   ACTIVEMQ_HOST=your-broker-host
   ACTIVEMQ_PORT=61614
   ACTIVEMQ_USERNAME=admin
   ACTIVEMQ_PASSWORD=admin
   USE_SSL=true
   ```
   
   Note: Queue names are configured in `docker-compose.yml` for each service.

2. **Start everything:**
   ```bash
   docker-compose up --build
   ```

3. **Access publisher UIs:**
   - Publisher 1: http://localhost:5001 (queue1)
   - Publisher 2: http://localhost:5002 (queue2)
   - Publisher 3: http://localhost:5003 (queue3)

4. **Stop specific subscriber:**
   ```bash
   docker-compose stop sub1   # Stop subscriber for queue1
   docker-compose stop sub2   # Stop subscriber for queue2
   docker-compose stop sub3   # Stop subscriber for queue3
   ```

## What's Included

- **pub/** - Flask web app for publishing messages to ActiveMQ (3 instances)
- **sub/** - Python consumer that listens and prints messages from ActiveMQ (3 instances)
- Each pub/sub pair communicates through a dedicated queue (queue1, queue2, queue3)

## Notes

- Queue names can be changed in `docker-compose.yml` under the `environment` section
- Common broker settings (host, port, credentials) are in `.env` files
- Subscriber auto-reconnects and prints messages to console
- Use `docker-compose logs -f sub1` (or sub2, sub3) to watch subscriber output
- To change queue names, edit the `ACTIVEMQ_QUEUE` values in `docker-compose.yml`

