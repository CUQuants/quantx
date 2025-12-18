"""
Event Bus implementation using asyncio.Queue for pub/sub messaging.

This module provides the central message queue that enables loose coupling
between services. Each service subscribes to certain event types
and publishes events for other services to consume.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Dict, List
from collections import defaultdict

from .events import Event, EventType

logger = logging.getLogger(__name__)

EventHandler = Callable[[Event], Awaitable[None]]


class EventBus:
    """
    Async event bus using asyncio.Queue for pub/sub messaging.

    Features:
        - Multiple subscribers per event type
        - Async event processing
        - Graceful shutdown support
        - Event logging for debugging

    """

    def __init__(self, max_queue_size: int = 10000):
        self._queue: asyncio.Queue[Event] = asyncio.Queue(
            maxsize=max_queue_size)
        self._subscribers: Dict[EventType,
                                List[EventHandler]] = defaultdict(list)
        self._running: bool = False
        self._processor_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        if handler not in self._subscribers[event_type]:
            self._subscribers[event_type].append(handler)
            logger.debug(f"Subscribed {handler.__name__} to {event_type.name}")

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        if handler in self._subscribers[event_type]:
            self._subscribers[event_type].remove(handler)
            logger.debug(
                f"Unsubscribed {handler.__name__} from {event_type.name}")

    async def publish(self, event: Event) -> None:
        """
        This will wait for the queue to have space, and then publish the event
        NOTE - this results in blocking behavior
        """
        try:
            await self._queue.put(event)
            logger.debug(
                f"Published {event.type.name} (correlation_id={event.correlation_id[:8]}...)"
            )
        except asyncio.QueueFull:
            logger.error(
                f"Event queue full, dropping event: {event.type.name}")
            raise

    async def publish_nowait(self, event: Event) -> bool:
        """
        This will publish an event without waiting if queue is full.
        If the queue is full, it will throw an error.
        """
        try:
            self._queue.put_nowait(event)
            logger.debug(
                f"Published {event.type.name} (correlation_id={event.correlation_id[:8]}...)"
            )
            return True
        except asyncio.QueueFull:
            logger.warning(
                f"Event queue full, could not publish: {event.type.name}")
            return False

    async def start(self) -> None:
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
        """
        async with self._lock:
            if not self._running:
                return

            self._running = False

            if self._processor_task:
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
        while not self._queue.empty():
            await asyncio.sleep(0.01)

    async def _process_events(self) -> None:
        while self._running:
            try:
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
        handlers = self._subscribers.get(event.type, [])

        if not handlers:
            logger.debug(f"No handlers for event type: {event.type.name}")
            return

        logger.debug(
            f"Dispatching {event.type.name} to {len(handlers)} handlers "
            f"(correlation_id={event.correlation_id[:8]}...)"
        )

        results = await asyncio.gather(
            *(self._safe_call_handler(handler, event) for handler in handlers),
            return_exceptions=True
        )

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
        try:
            await handler(event)
        except Exception as e:
            logger.error(
                f"Error in handler {handler.__name__} for {event.type.name}: {e}"
            )
            raise

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def is_running(self) -> bool:
        return self._running

    def get_subscriber_count(self, event_type: EventType) -> int:
        return len(self._subscribers.get(event_type, []))
