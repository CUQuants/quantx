"""
Service Container for dependency injection and lifecycle management.

This module provides a centralized container that:
    - Creates and wires up all services
    - Manages service lifecycle (start/stop)
    - Provides access to services for the FastAPI app
"""

import logging
from typing import List, Optional

from .event_bus import EventBus
from .matching_engine import MatchingEngine
from .risk_engine import RiskEngine
from .persistence_service import PersistenceService
from .broadcasting_service import BroadcastingService
from .market_data_broadcaster import MarketDataBroadcaster
from .event_stream_broadcaster import EventStreamBroadcaster

logger = logging.getLogger(__name__)


class ServiceContainer:
    """
    Dependency injection container for all services.
    
    Manages the creation, wiring, and lifecycle of all services
    in the trading system.
    
    Usage:
        # In FastAPI lifespan
        container = ServiceContainer(
            session_factory=SessionFactory,
            auth_service=FirebaseAuth(),
            tickers=["QNTX"],
        )
        await container.start()
        
        # ... app runs ...
        
        await container.stop()
    """
    
    def __init__(
        self,
        session_factory,
        auth_service,
        tickers: List[str],
    ):
        """
        Initialize the service container.
        
        Args:
            session_factory: Async session factory for database operations
            auth_service: Firebase authentication service
            tickers: List of supported ticker symbols
        """
        self._session_factory = session_factory
        self._auth_service = auth_service
        self._tickers = [t.upper() for t in tickers]
        
        # Create event bus
        self._event_bus = EventBus()
        
        # Create services (order matters due to dependencies)
        self._matching_engine = MatchingEngine(
            event_bus=self._event_bus,
            tickers=self._tickers,
            session_factory=self._session_factory,
        )
        
        self._risk_engine = RiskEngine(
            event_bus=self._event_bus,
            session_factory=self._session_factory,
            matching_engine=self._matching_engine,
        )
        
        self._persistence_service = PersistenceService(
            event_bus=self._event_bus,
            session_factory=self._session_factory,
        )
        
        self._broadcasting_service = BroadcastingService(
            event_bus=self._event_bus,
            auth_service=self._auth_service,
            tickers=self._tickers,
        )
        
        self._market_data_broadcaster = MarketDataBroadcaster(
            event_bus=self._event_bus,
            tickers=self._tickers,
        )
        
        self._event_stream_broadcaster = EventStreamBroadcaster(
            event_bus=self._event_bus,
            auth_service=self._auth_service,
            tickers=self._tickers,
        )
        
        self._started = False
    
    # =========================================================================
    # Service Accessors
    # =========================================================================
    
    @property
    def event_bus(self) -> EventBus:
        """Get the event bus."""
        return self._event_bus
    
    @property
    def matching_engine(self) -> MatchingEngine:
        """Get the matching engine service."""
        return self._matching_engine
    
    @property
    def risk_engine(self) -> RiskEngine:
        """Get the risk engine service."""
        return self._risk_engine
    
    @property
    def persistence_service(self) -> PersistenceService:
        """Get the persistence service."""
        return self._persistence_service
    
    @property
    def broadcasting_service(self) -> BroadcastingService:
        """Get the broadcasting service."""
        return self._broadcasting_service
    
    @property
    def market_data_broadcaster(self) -> MarketDataBroadcaster:
        """Get the market data broadcaster service."""
        return self._market_data_broadcaster
    
    @property
    def event_stream_broadcaster(self) -> EventStreamBroadcaster:
        """Get the event stream broadcaster service."""
        return self._event_stream_broadcaster
    
    @property
    def tickers(self) -> List[str]:
        """Get the list of supported tickers."""
        return self._tickers
    
    # =========================================================================
    # Lifecycle Management
    # =========================================================================
    
    async def start(self) -> None:
        """
        Start all services.
        
        Services are started in dependency order:
        1. Event bus
        2. Matching engine (no dependencies)
        3. Hydrate order books from database
        4. Risk engine (depends on matching engine)
        5. Persistence service (no service dependencies)
        6. Broadcasting service (no service dependencies)
        7. Market data broadcaster (no service dependencies)
        """
        if self._started:
            logger.warning("ServiceContainer already started")
            return
        
        logger.info("Starting service container...")
        
        # Start event bus first
        await self._event_bus.start()
        
        # Start services in order
        await self._matching_engine.start()
        
        # Hydrate order books from database before other services start
        # This ensures market data is available when clients connect
        logger.info("Hydrating order books from database...")
        await self._matching_engine.hydrate_all_books()
        
        await self._risk_engine.start()
        await self._persistence_service.start()
        await self._broadcasting_service.start()
        await self._market_data_broadcaster.start()
        await self._event_stream_broadcaster.start()
        
        self._started = True
        logger.info("Service container started - all services running")
    
    async def stop(self) -> None:
        """
        Stop all services.
        
        Services are stopped in reverse order to respect dependencies.
        """
        if not self._started:
            return
        
        logger.info("Stopping service container...")
        
        # Stop services in reverse order
        await self._event_stream_broadcaster.stop()
        await self._market_data_broadcaster.stop()
        await self._broadcasting_service.stop()
        await self._persistence_service.stop()
        await self._risk_engine.stop()
        await self._matching_engine.stop()
        
        # Stop event bus last
        await self._event_bus.stop()
        
        self._started = False
        logger.info("Service container stopped")
    
    # =========================================================================
    # Health Check
    # =========================================================================
    
    async def health_check(self) -> dict:
        """
        Get health status of all services.
        
        Returns:
            Dictionary with health status of each service
        """
        return {
            "container_started": self._started,
            "event_bus": {
                "running": self._event_bus.is_running,
                "queue_size": self._event_bus.queue_size,
            },
            "services": {
                "matching_engine": await self._matching_engine.health_check(),
                "risk_engine": await self._risk_engine.health_check(),
                "persistence_service": await self._persistence_service.health_check(),
                "broadcasting_service": await self._broadcasting_service.health_check(),
                "market_data_broadcaster": await self._market_data_broadcaster.health_check(),
                "event_stream_broadcaster": await self._event_stream_broadcaster.health_check(),
            },
        }
