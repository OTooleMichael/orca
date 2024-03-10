import time
import redis
from collections.abc import Callable, Generator
from threading import Lock, Thread
from functools import cached_property

from orca_tools.protos import decode_event, Event, encode_event
from orca_tools.redis_orca import get_connection
from generated_grpc import orca_pb2 as pb2

MESSAGE_CHANNEL = "orca:events_TestPipe1337"  # Integration Test Channel - needs to be dynamic for prod/local_test -> hence can't be fixed here in main package


def _tail_events() -> Generator[Event, None, None]:
    subpub = get_connection(should_decode=False).pubsub()
    subpub.subscribe(MESSAGE_CHANNEL)
    for message in subpub.listen():
        if message["type"] != "message":
            continue
        if isinstance(message["data"], (bytes,)):
            yield decode_event(message["data"])


class EventBus:
    _subscribers: list[Callable[[Event, "EventBus"], None | bool]]

    def __init__(self) -> None:
        self.thread_lock = Lock()

    @cached_property
    def connection(self) -> redis.Redis:
        return get_connection()

    def _subscribe(
        self, callback: Callable[[Event, "EventBus"], None | bool], sub_name: str | None
    ) -> None:
        started_at = int(time.time() * 1000)
        if sub_name:
            print("BIRTH", sub_name)

        for event in _tail_events():
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
    ) -> None:
        thread = Thread(
            target=self._subscribe,
            args=(callback, sub_name),
        )
        thread.start()

    def publish(self, event: Event) -> None:
        self.connection.publish(MESSAGE_CHANNEL, encode_event(event))


emitter = EventBus()
