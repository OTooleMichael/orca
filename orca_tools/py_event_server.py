import json
import time
from collections.abc import Callable, Generator
from functools import partial
from threading import Lock, Thread

from orca_tools.models import Event, EventName, EventType
from orca_tools.redis_orca import get_connection

MESSAGE_CHANNEL = "orca:events_TestPipe1337"  # Integration Test Channel - needs to be dynamic for prod/local_test -> hence can't be fixed here in main package

def _tail_events() -> Generator[dict, None, None]:
    subpub = get_connection().pubsub()
    subpub.subscribe(MESSAGE_CHANNEL)
    for message in subpub.listen():
        print(message)
        if message["type"] != "message":
            continue
        if isinstance(message["data"], str):
            yield json.loads(message["data"])


class EventBus:
    Event = Event

    _subscribers: list[Callable[[Event, "EventBus"], None | bool]]
    def __init__(self) -> None:
        self.thread_lock = Lock()

    def _subscribe(self, callback: Callable[[Event, "EventBus"], None | bool], sub_name: str | None) -> None:
        started_at = int(time.time() * 1000)
        if sub_name:
            print("BIRTH", sub_name)

        for data_dict in _tail_events():
            if sub_name:
                print(sub_name, data_dict)
            event = Event(**data_dict)
            if event.event_epoch <= started_at:
                continue
            if callback(event, self):
                break

        if sub_name:
            print("DEATH", sub_name)

    def subscribe_thread(self, callback: Callable[[Event, "EventBus"], None | bool], sub_name: str | None = None) -> None:
        thread = Thread(
            target=self._subscribe,
            args=(callback, sub_name),
        )
        thread.start()

    def publish(self, event: Event) -> None:
        redis = get_connection()
        redis.publish(MESSAGE_CHANNEL, event.model_dump_json())

    def request(self, event: Event) -> Event:
        state = {"res": None}
        callback = partial(
            _updater,
            task_matcher=event.task_matcher,
            event_name=event.name,
            state=state,
        )
        self.subscribe_thread(callback, f"request:{event.name}")
        emitter.publish(event)
        waited_for: float = 0
        wait_interval = 0.1
        max_wait = 60*1
        while state["res"] is None:
            if waited_for > max_wait:
                raise TimeoutError(f"Waited for {max_wait} seconds")
            time.sleep(wait_interval)
            waited_for += wait_interval

        res = state["res"]
        assert res

        return res


def _updater(event: Event, _: EventBus, task_matcher: str, event_name: str | EventName, state: dict) -> bool:
    if event.event_type != EventType.response:
        return False
    if (event.task_matcher, event.name) == (task_matcher, event_name):
        state["res"] = event
        return True
    return False


emitter = EventBus()
