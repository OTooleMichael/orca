import dataclasses
import fcntl
import json
import sys
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from multiprocessing import Manager, Process
from multiprocessing.managers import DictProxy
from pathlib import Path

new_entry = "foobar"
event_file = "/tmp/event_file.txt"
if not Path(event_file).exists():
    with open(event_file, "w") as f:
        f.write("")

EVENT_BUFFER = 100

def _tail_events() -> Generator[str, None, None]:
    start_time = int(time.time() * 1000)
    delivered: list[tuple[str, int]] = []
    while True:
        time.sleep(0.1)
        lines: list[str] = []
        with open(event_file) as fp:
            lines = fp.readlines()

        for line in filter(None, lines):
            event = json.loads(line.strip())
            e_epoch = event["event_epoch"]
            e_id = event["event_id"]
            # When we start, we don't want old events
            if e_epoch <= start_time:
                continue
            # We don't want to deliver the same event twice
            if (e_id, e_epoch) in delivered:
                continue

            # It might be that event come in so fast that they are out of order
            # So we keep a small buffer and only update the start_time, when it is
            # safely in the past. Updating on every event could cause a skip
            delivered.append((e_id, e_epoch))
            if len(delivered) > EVENT_BUFFER:
                delivered = sorted(delivered, key=lambda x: x[1])
                delivered = delivered[-50:]
                start_time = delivered.pop(0)[1]
            yield line


@dataclass
class Event:
    task_matcher: str
    name: str
    source_server_id: str
    payload: dict | None = None
    event_epoch: int = field(default_factory=lambda: int(time.time() * 1000))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def event_at(self) -> datetime:
        return datetime.fromtimestamp(self.event_epoch/1000, tz=UTC)



class EventBus:
    Event = Event

    _subscribers: list[Callable[[Event, "EventBus"], None | bool]]
    def __init__(self) -> None:
        self._subscribers = []

    def subscribe(self, callback: Callable[[Event, "EventBus"], None | bool], *, kill:bool = False) -> None:
        started_at = int(time.time() * 1000)
        self._subscribers.append(callback)
        for line in _tail_events():
            event = Event(**json.loads(line))
            if event.event_epoch <= started_at:
                continue
            unsub_prep: list[Callable] = []
            for subscriber in self._subscribers:
                res = subscriber(event, self)
                if res:
                    unsub_prep.append(subscriber)


            for unsub in unsub_prep:
                self._subscribers.remove(unsub)

            if kill and unsub_prep:
                break

    def subscribe_thread(self, callback: Callable[[Event, "EventBus"], None | bool]) -> Callable[[], None]:
        thread = Process(
            target=self.subscribe,
            args=(callback, ),
            kwargs={"kill": True},
        )
        thread.start()
        def _unsub() -> None:
            thread.kill()
        return _unsub

    def publish(self, event: Event) -> None:
        with open(event_file, "a") as f:
            json_evet_str = json.dumps(dataclasses.asdict(event))
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(json_evet_str + "\n")
            fcntl.flock(f, fcntl.LOCK_UN)

    def request(self, event: Event) -> Event:
        res_event_name = f"{event.name}:res".replace(":req", "")
        manager = Manager()
        state = manager.dict()
        state["res"] = None

        callback = partial(
            _updater,
            task_matcher=event.task_matcher,
            res_event_name=res_event_name,
            state=state,
        )
        self.subscribe_thread(callback)
        emitter.publish(event)
        while state["res"] is None:
            time.sleep(0.1)
        res = state["res"]
        assert res

        return res

def _updater(event: Event, _: EventBus, task_matcher: str, res_event_name: str, state: DictProxy) -> bool:
    if event.task_matcher == task_matcher and event.name == res_event_name:
        state["res"] = event
        return True
    return False

emitter = EventBus()


if __name__ == "__main__":
    user_arg = len(sys.argv) > 1
    now_str = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    if user_arg:
        print("publishing event")
        emitter.publish(Event(task_matcher="*", name="test:"+now_str, source_server_id="1", payload={"foo": user_arg}))
    else:
        print("subscribing to events")
        unsub = emitter.subscribe(lambda event, _: print(event))
