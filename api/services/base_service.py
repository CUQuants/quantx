"""
Base service class for all services in the QuantX trading system.

Provides common functionality for service lifecycle management,
event subscription, and health checking.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple

from .event_bus import EventBus, EventHandler
from .events import EventType

logger = logging.getLogger(__name__)


class BaseService(ABC):
    """
    Abstract base class for all services.
    
    Provides:
        - Service lifecycle management (start/stop)
        - Event subscription helper
        - Health check interface
        - Logging setup
    
    Usage:
        class MyService(BaseService):
            @property
            def service_name(self) -> str:
                return "MyService"
            
            def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
                return [
                    (EventType.RAW_ORDER, self._handle_raw_order),
                ]
            
            async def _start(self) -> None:
                # Custom initialization
                pass
            
            async def _stop(self) -> None:
                # Custom cleanup
                pass
            
            async def _handle_raw_order(self, event: Event) -> None:
                # Handle the event
                pass
    """
    
    def __init__(self, event_bus: EventBus):
        """
        Initialize the base service.
        
        Args:
            event_bus: The shared event bus instance
        """
        self._event_bus = event_bus
        self._running = False
        self._logger = logging.getLogger(f"quantx.services.{self.service_name}")
    
    @property
    @abstractmethod
    def service_name(self) -> str:
        """
        Return the service name for logging and identification.
        
        Returns:
            Service name string
        """
        pass
    
    @abstractmethod
    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        """
        Return the list of event subscriptions for this service.
        
        Returns:
            List of (EventType, handler) tuples
        """
        pass
    
    async def _start(self) -> None:
        """
        Custom initialization logic. Override in subclasses.
        
        Called after event subscriptions are set up.
        """
        pass
    
    async def _stop(self) -> None:
        """
        Custom cleanup logic. Override in subclasses.
        
        Called before event subscriptions are removed.
        """
        pass
    
    async def start(self) -> None:
        """
        Start the service.
        
        Sets up event subscriptions and calls custom initialization.
        """
        if self._running:
            self._logger.warning(f"{self.service_name} already running")
            return
        
        self._logger.info(f"Starting {self.service_name}...")
        
        # Subscribe to events
        for event_type, handler in self._get_subscriptions():
            self._event_bus.subscribe(event_type, handler)
            self._logger.debug(f"Subscribed to {event_type.name}")
        
        # Run custom initialization
        await self._start()
        
        self._running = True
        self._logger.info(f"{self.service_name} started")
    
    async def stop(self) -> None:
        """
        Stop the service.
        
        Runs custom cleanup and removes event subscriptions.
        """
        if not self._running:
            return
        
        self._logger.info(f"Stopping {self.service_name}...")
        
        # Run custom cleanup
        await self._stop()
        
        # Unsubscribe from events
        for event_type, handler in self._get_subscriptions():
            self._event_bus.unsubscribe(event_type, handler)
            self._logger.debug(f"Unsubscribed from {event_type.name}")
        
        self._running = False
        self._logger.info(f"{self.service_name} stopped")
    
    async def publish(self, event_type: EventType, payload: dict, correlation_id: str | None = None) -> None:
        """
        Helper to publish an event with automatic Event wrapping.
        
        Args:
            event_type: Type of event to publish
            payload: Event payload dictionary
            correlation_id: Optional correlation ID for tracing
        """
        from .events import Event
        from datetime import datetime, timezone
        import uuid
        
        event = Event(
            type=event_type,
            payload=payload,
            timestamp=datetime.now(timezone.utc),
            correlation_id=correlation_id or str(uuid.uuid4())
        )
        await self._event_bus.publish(event)
    
    @property
    def is_running(self) -> bool:
        """Whether the service is currently running."""
        return self._running
    
    async def health_check(self) -> dict:
        """
        Return health status of the service.
        
        Override in subclasses to add custom health metrics.
        
        Returns:
            Dictionary with health status
        """
        return {
            "service": self.service_name,
            "running": self._running,
            "healthy": self._running,
        }
