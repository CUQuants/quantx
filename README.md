# QuantX

QuantX is a real-time market simulation platform that serves three main purposes:

- Educate members about market dynamics
- Provide a space for members to apply what they learn in meetings about reasoning about market making
- Serve as the market infrastructure for the HackCU CUQuants trading game track

## Prerequisites

Before setting up the project, you'll need:

1. **Docker and Docker Compose** - Required for running the application
2. **Python 3.12+** - Required for running test scripts
3. **Firebase Service Account JSON file** - Required for authentication

### Creating the service-account.json File

You need to create a `service-account.json` file in the `quantx` directory with your Firebase service account credentials. This file contains the necessary information for Firebase authentication and should be placed in the root of the `quantx` directory.

**Important**: This file contains sensitive credentials and should never be committed to version control.

## Getting Started with Docker Compose

The recommended way to run QuantX is using Docker Compose, which sets up both the PostgreSQL database and the API service.

### Initial Setup

1. **Navigate to the quantx directory:**
   ```bash
   cd quantx
   ```

2. **Create the service-account.json file:**
   - Create a file named `service-account.json` in the `quantx` directory
   - Add your Firebase service account credentials to this file

3. **Start the services:**
   ```bash
   docker compose up --build
   ```

   The `--build` flag ensures that Docker builds the images before starting the containers. Use `--build`:
   - On first run
   - After making changes to the Dockerfile
   - After updating dependencies in `requirements.txt`
   - If you want to ensure you're using the latest build

4. **To run in the background (detached mode):**
   ```bash
   docker compose up --build -d
   ```

5. **To stop the services:**
   ```bash
   docker compose down
   ```

6. **To view logs:**
   ```bash
   docker compose logs -f quantx-api
   ```

The API will be available at `http://localhost:8000`. The PostgreSQL database will be running in a separate container.

## Application Flow

QuantX uses a service-oriented architecture where all services communicate via an event bus. Here's how the application works:

### Service Architecture

The system consists of six core services managed by the `ServiceContainer`:

1. **MatchingEngine** - Manages order books and executes trades by matching buy and sell orders
2. **RiskEngine** - Validates orders before they reach the matching engine
3. **PersistenceService** - Handles all database operations (saving orders, trades, updating account balances)
4. **BroadcastingService** - Manages WebSocket connections and client authentication
5. **MarketDataBroadcaster** - Distributes market data (order book snapshots) to subscribed clients
6. **EventStreamBroadcaster** - Streams real-time events (trades, orders, cancellations) to clients

### Request Flow

1. **Client Connection**: A client connects via WebSocket to `/ws/api/{ticker}` (API key authentication) or `/ws/{ticker}` (Firebase token authentication)

2. **Order Submission**: 
   - Client sends an order through the WebSocket connection
   - BroadcastingService receives the order and authenticates the client
   - Order is published to the event bus as a `RAW_ORDER` event

3. **Order Processing**:
   - RiskEngine validates the order (checks account balance, position limits, etc.)
   - If valid, publishes `VALIDATED_ORDER` event
   - MatchingEngine receives the validated order and attempts to match it with existing orders
   - If a match is found, a trade is executed and `TRADE_EXECUTED` event is published
   - PersistenceService saves the order and any resulting trades to the database

4. **Market Updates**:
   - MarketDataBroadcaster sends order book snapshots to subscribed clients
   - EventStreamBroadcaster sends real-time trade and order updates
   - Clients receive updates through their WebSocket connections

### Event Bus

All services communicate through an asynchronous event bus implemented with `asyncio.Queue`. This decouples services and allows for easy scaling and testing.

## Running Test Scripts

Two test scripts are provided to interact with the QuantX API:

### api_test.py

This script uses the QuantX client API to connect and interact with the trading platform.

**Setup:**

1. **Create a virtual environment:**
   ```bash
   cd quantx
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # venv\Scripts\activate   # On Windows
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Generate an API key:**
   - Navigate to the frontend application
   - Go to the API Keys page
   - Create a new API key
   - Copy the generated API key (it will look like `qntx_live_...`)

4. **Update api_test.py:**
   - Open `api_test.py`
   - Replace the `API_KEY` variable with your generated API key:
     ```python
     API_KEY = "qntx_live_your_key_here"
     ```

5. **Run the script:**
   ```bash
   python api_test.py
   ```

The script will connect to the API, subscribe to public market data, and display trades and order fills in real-time.

### order_transmitter.py

This script sends randomized orders to test the order processing system. It uses the `websockets` library directly.

**Setup:**

1. **Ensure you have the virtual environment activated** (same as above)

2. **Install dependencies** (should already be installed from step 2 above):
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the script:**
   - Open `order_transmitter.py`
   - Update the `WS_URL`, `TICKER`, and `token` variables as needed
   - Adjust price ranges, quantities, and delays as desired

4. **Run the script:**
   ```bash
   python order_transmitter.py
   ```

The script will connect to the WebSocket server and send randomized buy and sell orders.

## Project Structure

```
quantx/
├── api/                    # FastAPI application and routes
│   ├── main_v2.py         # Main API entry point (service-oriented architecture)
│   ├── routes/            # REST API routes
│   ├── services/          # Core services (MatchingEngine, RiskEngine, etc.)
│   └── security/          # Authentication and authorization
├── client_api/            # Python client SDK for connecting to QuantX
├── engine_server/         # Matching engine implementation
├── models/                # Database models
├── services/              # Core business logic services
├── manual_agent/          # Manual trading agent
├── tests/                 # Test suite
├── configs/               # Configuration files
├── utils/                 # Utility functions
├── docker-compose.yml     # Docker Compose configuration
├── Dockerfile             # Docker image definition
├── requirements.txt       # Python dependencies
├── api_test.py           # Example script using the client API
└── order_transmitter.py  # Example script for sending test orders
```

## Dependencies

- **FastAPI** - Web framework and WebSocket support
- **SQLAlchemy** - Database ORM
- **PostgreSQL** - Database engine (via Docker)
- **Pydantic** - Data validation
- **SortedContainers** - Efficient data structures for orderbook
- **Firebase Admin** - Authentication service
- **websockets** - WebSocket client library
- **Pytest** - Testing framework

## Additional Resources

- The API documentation is available at `http://localhost:8000/docs` when the service is running
- Health check endpoint: `http://localhost:8000/health`
- Simple health check: `http://localhost:8000/health/simple`
