"""
Event Bus implementation using asyncio.Queue for pub/sub messaging.

This module provides the central message queue that enables loose coupling
between services. Each service subscribes to event types it cares about
and publishes events for other services to consume.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Dict, List, Set
from collections import defaultdict

from .events import Event, EventType

logger = logging.getLogger(__name__)

# Type alias for event handler callbacks
EventHandler = Callable[[Event], Awaitable[None]]


class EventBus:
    """
    Async event bus using asyncio.Queue for pub/sub messaging.
    
    Features:
        - Multiple subscribers per event type
        - Async event processing
        - Graceful shutdown support
        - Event logging for debugging
    
    Usage:
        bus = EventBus()
        
        # Subscribe to events
        async def handle_order(event: Event):
            payload = event.payload
            ...
        
        bus.subscribe(EventType.RAW_ORDER, handle_order)
        
        # Publish events
        await bus.publish(Event(
            type=EventType.RAW_ORDER,
            payload={"ticker": "QNTX", ...}
        ))
        
        # Start processing (in lifespan)
        await bus.start()
        
        # Shutdown (in lifespan cleanup)
        await bus.stop()
    """
    
    def __init__(self, max_queue_size: int = 10000):
        """
        Initialize the event bus.
        
        Args:
            max_queue_size: Maximum number of events that can be queued
        """
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._subscribers: Dict[EventType, List[EventHandler]] = defaultdict(list)
        self._running: bool = False
        self._processor_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
    
    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """
        Subscribe a handler to an event type.
        
        Args:
            event_type: The type of events to subscribe to
            handler: Async callback function that receives Event objects
        """
        if handler not in self._subscribers[event_type]:
            self._subscribers[event_type].append(handler)
            logger.debug(f"Subscribed {handler.__name__} to {event_type.name}")
    
    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """
        Unsubscribe a handler from an event type.
        
        Args:
            event_type: The type of events to unsubscribe from
            handler: The handler to remove
        """
        if handler in self._subscribers[event_type]:
            self._subscribers[event_type].remove(handler)
            logger.debug(f"Unsubscribed {handler.__name__} from {event_type.name}")
    
    async def publish(self, event: Event) -> None:
        """
        Publish an event to the bus.
        
        Events are queued and processed asynchronously by the processor task.
        
        Args:
            event: The event to publish
        """
        try:
            await self._queue.put(event)
            logger.debug(
                f"Published {event.type.name} (correlation_id={event.correlation_id[:8]}...)"
            )
        except asyncio.QueueFull:
            logger.error(f"Event queue full, dropping event: {event.type.name}")
            raise
    
    async def publish_nowait(self, event: Event) -> bool:
        """
        Publish an event without waiting if queue is full.
        
        Args:
            event: The event to publish
            
        Returns:
            True if event was queued, False if queue was full
        """
        try:
            self._queue.put_nowait(event)
            logger.debug(
                f"Published {event.type.name} (correlation_id={event.correlation_id[:8]}...)"
            )
            return True
        except asyncio.QueueFull:
            logger.warning(f"Event queue full, could not publish: {event.type.name}")
            return False
    
    async def start(self) -> None:
        """
        Start the event processor.
        
        This should be called during application startup.
        """
        async with self._lock:
            if self._running:
                logger.warning("EventBus already running")
                return
            
            self._running = True
            self._processor_task = asyncio.create_task(
                self._process_events(),
                name="event_bus_processor"
            )
            logger.info("EventBus started")
    
    async def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the event processor gracefully.
        
        Waits for pending events to be processed up to the timeout.
        
        Args:
            timeout: Maximum seconds to wait for pending events
        """
        async with self._lock:
            if not self._running:
                return
            
            self._running = False
            
            if self._processor_task:
                # Wait for queue to drain (with timeout)
                try:
                    await asyncio.wait_for(
                        self._drain_queue(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"EventBus shutdown timeout, {self._queue.qsize()} events remaining"
                    )
                
                self._processor_task.cancel()
                try:
                    await self._processor_task
                except asyncio.CancelledError:
                    pass
                
                self._processor_task = None
            
            logger.info("EventBus stopped")
    
    async def _drain_queue(self) -> None:
        """Wait for the queue to become empty."""
        while not self._queue.empty():
            await asyncio.sleep(0.01)
    
    async def _process_events(self) -> None:
        """
        Main event processing loop.
        
        Continuously pulls events from the queue and dispatches
        them to registered handlers.
        """
        while self._running:
            try:
                # Use timeout to allow checking _running flag periodically
                event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=0.1
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            
            await self._dispatch_event(event)
            self._queue.task_done()
    
    async def _dispatch_event(self, event: Event) -> None:
        """
        Dispatch an event to all registered handlers.
        
        Handlers are called concurrently using asyncio.gather.
        Errors in handlers are logged but don't stop other handlers.
        
        Args:
            event: The event to dispatch
        """
        handlers = self._subscribers.get(event.type, [])
        
        if not handlers:
            logger.debug(f"No handlers for event type: {event.type.name}")
            return
        
        logger.debug(
            f"Dispatching {event.type.name} to {len(handlers)} handlers "
            f"(correlation_id={event.correlation_id[:8]}...)"
        )
        
        # Run all handlers concurrently
        results = await asyncio.gather(
            *(self._safe_call_handler(handler, event) for handler in handlers),
            return_exceptions=True
        )
        
        # Log any exceptions
        for handler, result in zip(handlers, results):
            if isinstance(result, Exception):
                logger.error(
                    f"Handler {handler.__name__} failed for {event.type.name}: {result}",
                    exc_info=result
                )
    
    async def _safe_call_handler(
        self, 
        handler: EventHandler, 
        event: Event
    ) -> None:
        """
        Safely call a handler, catching any exceptions.
        
        Args:
            handler: The handler to call
            event: The event to pass to the handler
        """
        try:
            await handler(event)
        except Exception as e:
            logger.error(
                f"Error in handler {handler.__name__} for {event.type.name}: {e}"
            )
            raise
    
    @property
    def queue_size(self) -> int:
        """Current number of events in the queue."""
        return self._queue.qsize()
    
    @property
    def is_running(self) -> bool:
        """Whether the event bus is currently running."""
        return self._running
    
    def get_subscriber_count(self, event_type: EventType) -> int:
        """Get the number of subscribers for an event type."""
        return len(self._subscribers.get(event_type, []))
