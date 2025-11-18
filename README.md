# QuantX

QuantX is a real-time market simulation platform that serves three main purposes:

- Educate members about market dynamics
- Provide a space for members to apply what they learn in meetings about reasoning about market making
- Serve as the market infrastructure for the HackCU CUQuants trading game track

## Architecture

This project includes four core services that actively communicate with each other:

### Matching Engine
- Receives orders and handles them in real time
- Stores all buy and sell orders, attempting to match the highest bid with the lowest ask whenever an order is placed
- Updates database fields including order statuses, positions, and account balances when trades are executed
- Maintains an OrderBook class instance for each ticker with min heap for asks and max heap for bids
- Provides O(1) access to highest bid and lowest ask, with O(log(n)) insertion/removal for high throughput
- Includes boot-up recovery function that fetches all orders from database and loads them into orderbooks

### Broadcasting Service
- Acts as the communication layer between clients and the matching engine
- Enables client connections and delivers live updates when orders are placed
- Maintains cached orderbook snapshots for each ticker to avoid costly queries
- Validates incoming orders from clients, adds them to database, then forwards to matching engine
- Receives trades from matching engine and broadcasts orderbook updates to clients
- Hosts FastAPI WebSocket server for real-time communication
- Includes MarketData class that caches price-to-volume mappings using sorted dictionaries for efficient access

### Event Bus
- Serves as the bridge between the matching engine and broadcasting service
- Allows services to subscribe to specific events (ORDER, TRADE, INITIAL_SNAPSHOT)
- Prevents direct coupling between matching engine and broadcasting service, avoiding circular reference issues

### FastAPI HTTP Endpoint
- Provides REST API for frontend database interactions
- Features middleware and route protection with role-based access control
- Includes authentication middleware (require_admin, require_moderator, require_auth)
- Uses SQLite database for data persistence

## Getting Started

1. Navigate to the quantx directory:
   ```bash
   cd quantx
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # venv\Scripts\activate   # On Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the server:
   ```bash
   uvicorn api.main:app --reload --port 8000
   ```

## Project Structure

```
quantx/
├── api/                    # FastAPI application and routes
├── engine_server/          # Matching engine implementation
├── models/                 # Database models
├── services/               # Core business logic services
├── manual_agent/           # Manual trading agent
├── tests/                  # Test suite
├── configs/                # Configuration files
└── utils/                  # Utility functions
```

## Dependencies

- FastAPI - Web framework and WebSocket support
- SQLAlchemy - Database ORM
- SQLite - Database engine
- Pydantic - Data validation
- SortedContainers - Efficient data structures for orderbook
- Firebase Admin - Authentication service
- Pytest - Testing framework
