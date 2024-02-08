import dataclasses
from datetime import datetime, UTC
import fcntl
import json
import select
import subprocess
import sys
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from functools import partial
from pathlib import Path

new_entry = "foobar"
event_file = "/tmp/event_file.txt"
if not Path(event_file).exists():
    with open(event_file, "w") as f:
        f.write("")

def _tail_events() -> Generator[str, None, None]:
    f = subprocess.Popen(
        ["tail", "-F", event_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    p = select.poll()
    p.register(f.stdout)

    while True:
        if p.poll(1):
            print("yielding")
            data = f.stdout.readline()
            output = data.decode("utf-8").strip()
            print(output)
            yield output
        time.sleep(1)

@dataclass
class Event:
    task_matcher: str
    name: str
    source_server_id: str
    payload: dict | None = None


class EventBus:
    Event = Event

    _subscribers: list[Callable[[Event, "EventBus"], None]]
    def __init__(self) -> None:
        self._subscribers = []

    def subscribe(self, callback: Callable[[Event, "EventBus"], None]) -> Callable[[], None]:
        self._subscribers.append(callback)
        for line in _tail_events():
            event = Event(**json.loads(line))
            for subscriber in self._subscribers:
                subscriber(event, self)

        def _unsub() -> None:
            self.unsubscribe(callback)

        return _unsub

    def unsubscribe(self, callback: Callable[[Event, "EventBus"], None]) -> None:
        self._subscribers.remove(callback)

    def publish(self, event: Event) -> None:
        with open(event_file, "a") as f:
            json_evet_str = json.dumps(dataclasses.asdict(event))
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(json_evet_str + "\n")
            fcntl.flock(f, fcntl.LOCK_UN)

    def request(self, event: Event) -> Event:
        res_event_name = f"{event.name}:res".replace(":req", "")
        state = {"res": None}
        def updater(event: Event, _: EventBus, current_task: str, state: dict) -> None:
            if event.task_matcher == current_task and event.name == res_event_name:
                state["res"] = event

        unsub = emitter.subscribe(
            partial(updater, current_task=event.task_matcher, state=state),
        )
        emitter.publish(event)
        while state["res"] is None:
            time.sleep(0.1)
        res = state["res"]
        assert res

        unsub()
        return res


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
