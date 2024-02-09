import dataclasses
import fcntl
import json
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from threading import Lock, Thread

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
    payload: dict = field(default_factory=dict)
    event_epoch: int = field(default_factory=lambda: int(time.time() * 1000))
    event_id: str = field(default_factory=lambda: "ev_" + str(uuid.uuid4()))

    @property
    def event_at(self) -> datetime:
        return datetime.fromtimestamp(self.event_epoch/1000, tz=UTC)

    def __repr__(self) -> str:
        return f"<Ev {self.name}, {self.task_matcher}, {self.event_at}, {self.payload} {self.source_server_id} {self.event_id}>"


class EventBus:
    Event = Event

    _subscribers: list[Callable[[Event, "EventBus"], None | bool]]
    def __init__(self) -> None:
        self._subscribers = []
        self.thread_lock = Lock()

    def subscribe(self, callback: Callable[[Event, "EventBus"], None | bool], *, kill:bool = False) -> None:
        started_at = int(time.time() * 1000)
        with self.thread_lock:
            self._subscribers.append(callback)

        for line in _tail_events():
            event = Event(**json.loads(line))
            if event.event_epoch <= started_at:
                continue
            unsub_prep: list[Callable] = []
            for subscriber in self._subscribers:
                res = subscriber(event, self)
                if not res:
                    continue
                unsub_prep.append(subscriber)

            if not unsub_prep:
                continue

            with self.thread_lock:
                for unsub in unsub_prep:
                    if unsub not in self._subscribers:
                        print("Subscriber already removed", unsub)
                        continue
                    self._subscribers.remove(unsub)

            if kill and unsub_prep:
                break

    def subscribe_thread(self, callback: Callable[[Event, "EventBus"], None | bool], *, kill: bool = True) -> Callable[[], None]:
        thread = Thread(
            target=self.subscribe,
            args=(callback, ),
            kwargs={"kill": kill},
        )
        thread.start()

    def publish(self, event: Event) -> None:
        with open(event_file, "a") as f:
            json_evet_str = json.dumps(dataclasses.asdict(event))
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(json_evet_str + "\n")
            fcntl.flock(f, fcntl.LOCK_UN)

    def request(self, event: Event) -> Event:
        res_event_name = f"{event.name}:res".replace(":req", "")
        state = {"res": None}

        callback = partial(
            _updater,
            task_matcher=event.task_matcher,
            res_event_name=res_event_name,
            state=state,
        )
        self.subscribe_thread(callback)
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


def _updater(event: Event, _: EventBus, task_matcher: str, res_event_name: str, state: dict) -> bool:
    if (event.task_matcher, event.name) == (task_matcher, res_event_name):
        state["res"] = event
        return True
    return False


emitter = EventBus()
