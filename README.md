# ActiveMQ Pub/Sub Demo

Simple ActiveMQ publisher/subscriber deployment using Docker Compose.

## Quick Start

1. **Create environment files:**
   
   Create `pub/.env`:
   ```bash
   ACTIVEMQ_HOST=your-broker-host
   ACTIVEMQ_PORT=61614
   ACTIVEMQ_USERNAME=admin
   ACTIVEMQ_PASSWORD=admin
   ACTIVEMQ_QUEUE=/queue/test
   USE_SSL=true
   ```
   
   Create `sub/.env`:
   ```bash
   ACTIVEMQ_HOST=your-broker-host
   ACTIVEMQ_PORT=61614
   ACTIVEMQ_USERNAME=admin
   ACTIVEMQ_PASSWORD=admin
   ACTIVEMQ_QUEUE=/queue/test
   USE_SSL=true
   ```

2. **Start everything:**
   ```bash
   docker-compose up --build
   ```

3. **Access publisher UI:** http://localhost:5000

4. **Stop subscriber:**
   ```bash
   docker-compose stop sub
   ```

## What's Included

- **pub/** - Flask web app for publishing messages to ActiveMQ
- **sub/** - Python consumer that listens and prints messages from ActiveMQ

## Notes

- Update `.env` files in each service directory as needed
- Subscriber auto-reconnects and prints messages to console
- Use `docker-compose logs -f sub` to watch subscriber output

