"""
Base service class for all services in the QuantX trading system.

This provides common methods for managing service life cycle, event subscription, and health checks.
"""

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
    """

    def __init__(self, event_bus: EventBus):
        self._event_bus = event_bus
        self._running = False
        self._logger = logging.getLogger(
            f"quantx.services.{self.service_name}")

    @property
    @abstractmethod
    def service_name(self) -> str:
        """
        Return the service name as a string for logging and identification.
        """
        pass

    @abstractmethod
    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        """
        Returns the list of events the service is subscribed to, along with their callback functions
        """
        pass

    async def _start(self) -> None:
        """
        Called after event subscriptions are set up.
        """
        pass

    async def _stop(self) -> None:
        """
        Called before event subscriptions are removed.
        """
        pass

    async def start(self) -> None:
        """
        Sets up event subscriptions and calls custom initialization.
        """
        if self._running:
            self._logger.warning(f"{self.service_name} already running")
            return

        self._logger.info(f"Starting {self.service_name}...")

        for event_type, handler in self._get_subscriptions():
            self._event_bus.subscribe(event_type, handler)
            self._logger.debug(f"Subscribed to {event_type.name}")

        # Run the custom start method needed in subclasses
        await self._start()

        self._running = True
        self._logger.info(f"{self.service_name} started")

    async def stop(self) -> None:
        """
        Runs custom cleanup and removes event subscriptions.
        """
        if not self._running:
            return

        self._logger.info(f"Stopping {self.service_name}...")

        # Run custom cleanup in base classes
        await self._stop()

        for event_type, handler in self._get_subscriptions():
            self._event_bus.unsubscribe(event_type, handler)
            self._logger.debug(f"Unsubscribed from {event_type.name}")

        self._running = False
        self._logger.info(f"{self.service_name} stopped")

    async def publish(self, event_type: EventType, payload: dict, correlation_id: str | None = None) -> None:
        """
        Helper to publish an event with automatic Event wrapping.

        Note that the correlation_id field is optional, but can be used for event tracing
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
        return self._running

    async def health_check(self) -> dict:
        """
        Override in subclasses to add custom health metrics.
        """
        return {
            "service": self.service_name,
            "running": self._running,
            "healthy": self._running,
        }
