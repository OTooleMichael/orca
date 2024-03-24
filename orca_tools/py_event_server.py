import time
import traceback
import queue
import redis
from dataclasses import dataclass
from collections.abc import Callable, Generator
from typing import Protocol, Any
from threading import Thread
from functools import cached_property

from orca_tools.protos import decode_event, Event, encode_event
from orca_tools.redis_orca import get_connection
from generated_grpc import orca_pb2 as pb2

MESSAGE_CHANNEL = "orca:events_TestPipe1337"  # Integration Test Channel - needs to be dynamic for prod/local_test -> hence can't be fixed here in main package


class EventListener(Protocol):
    def __call__(self, event: Event, emitter: "EventBus") -> bool | None:
        """Listener Func."""


class EventBus(Protocol):
    def subscribe_thread(
        self,
        callback: EventListener,
        sub_name: str | None = None,
    ) -> Any:
        """Start listening on a new thread."""
        ...

    def publish(self, event: Event) -> None:
        """Publish an event to other listeners"""
        ...


@dataclass
class MemoryThreadWrapper:
    listener: EventListener
    queue: queue.Queue
    emitter: EventBus
    name: str

    def worker_thread(self):
        while True:
            try:
                should_break = self._work()
                if should_break:
                    break
            except Exception as e:
                print(f"Error in Thread {self.name} {e=}")
                traceback.print_exc()
                break

    def _work(self) -> bool:
        message = self.queue.get()
        if message == "stop":
            return True

        res = self.listener(message, self.emitter)
        return res or False

    def stop(self):
        self.queue.put("stop")


class MemoryBus(EventBus):
    def __init__(self) -> None:
        self._subscribers: list[tuple[MemoryThreadWrapper, Thread]] = []

    def subscribe_thread(
        self, callback: EventListener, sub_name: str | None = None
    ) -> Thread:
        wrapper = MemoryThreadWrapper(
            callback, queue.Queue(), self, name=sub_name or str(len(self._subscribers))
        )
        thread = Thread(
            target=wrapper.worker_thread,
            name=sub_name or "b-" + str(len(self._subscribers)),
        )
        self._subscribers.append((wrapper, thread))
        thread.start()
        return thread

    def stop(self):
        for wrapper, thread in self._subscribers:
            wrapper.stop()
            thread.join()

    def close(self):
        self.stop()

    def publish(self, event: Event) -> None:
        for wrapper, _ in self._subscribers:
            wrapper.queue.put(event)


class RedisBus(EventBus):
    @cached_property
    def connection(self) -> redis.Redis:
        return get_connection()

    def _tail_events(self) -> Generator[Event, None, None]:
        subpub = get_connection(should_decode=False).pubsub()
        subpub.subscribe(MESSAGE_CHANNEL)
        for message in subpub.listen():
            if message["type"] != "message":
                continue
            if isinstance(message["data"], (bytes,)):
                yield decode_event(message["data"])

    def _subscribe(
        self, callback: Callable[[Event, "EventBus"], None | bool], sub_name: str | None
    ) -> None:
        started_at = int(time.time() * 1000)
        if sub_name:
            print("BIRTH", sub_name)

        for event in self._tail_events():
            if sub_name:
                print(sub_name, event)
            _event: pb2.EventCore = event.event
            if _event.event_at.ToMilliseconds() <= started_at:
                continue

            if callback(event, self):
                break

        if sub_name:
            print("DEATH", sub_name)

    def subscribe_thread(
        self,
        callback: Callable[[Event, "EventBus"], None | bool],
        sub_name: str | None = None,
    ) -> Thread:
        thread = Thread(
            target=self._subscribe,
            args=(callback, sub_name),
        )
        thread.start()
        return thread

    def publish(self, event: Event) -> None:
        self.connection.publish(MESSAGE_CHANNEL, encode_event(event))


emitter = RedisBus()
