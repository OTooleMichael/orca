import dataclasses
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from orca.models import EventName, EventType, State
from orca.py_event_server import Event, EventBus, emitter
from orca.utils import orca_id

RunableType = Callable[[], None]


@dataclass
class Task:
    name: str
    downstream_tasks: list[str] = field(default_factory=list)
    upstream_tasks: list[str] = field(default_factory=list)
    run: RunableType | None = None
    complete_check: Callable[[], bool] | None = None

    def __call__(self) -> None:
        if self.run:
            self.run()

    def is_complete(self) -> bool:
        if self.complete_check:
            return self.complete_check()

        return False


def task(
    func: RunableType | None = None,
    *,
    name: str | None = None,
    downstream_tasks: list[str] | None = None,
    upstream_tasks: list[str] | None = None,
    complete_check: Callable[[], bool] | None = None,
) -> Callable:
    if func and callable(func):
        return task()(func)

    def _decorator(func: RunableType) -> Task:
        _name = name or func.__name__
        return Task(
            name=_name,
            run=func,
            downstream_tasks=downstream_tasks or [],
            upstream_tasks=upstream_tasks or [],
            complete_check=complete_check,
        )

    return _decorator


@task(upstream_tasks=["task_d", "task_c"])
def task_b() -> None:
    print("Task A")


@task()
def task_d() -> None:
    print("Task D")


@task()
def task_c() -> None:
    print("Task C")


@task(upstream_tasks=["task_a", "task_c", "task_d"], complete_check=lambda: True)
def task_a() -> None:
    print("START  A")
    time.sleep(2)
    print("COMPLETe Task A")


@task(upstream_tasks=["task_2", "task_b", "task_d"])
def task_1() -> None:
    print("Task 1 start")
    time.sleep(2)
    print("Task 1 end")


@task(upstream_tasks=["task_a", "task_c"])
def task_2() -> None:
    print("Task 2")


@dataclasses.dataclass
class Server:
    name: str
    tasks: list[Task]
    emitter: EventBus = emitter
    _states: dict[str, str] = field(default_factory=dict)
    server_id: str = field(default_factory=lambda: orca_id("server"))

    def _handle_event(self, event: Event, _: EventBus) -> None:
        if event.source_server_id == self.name:
            return None

        match (event.name, event.event_type):
            case (EventName.run_task, EventType.request):
                return self.run_task(event.task_matcher)
            case (EventName.task_complete, EventType.request):
                self.check_task_complete(event.task_matcher)
                return None
            case (EventName.describe_server, EventType.request):
                self.describe()
                return None
            case _:
                return None

    def start(self) -> None:
        # subscribe self.handle_event to the emitter on a new thread
        self.emitter.subscribe_thread(self._handle_event)
        self.describe()
        self.emitter.publish(
            Event(
                task_matcher="",
                name="server_state",
                state=State.ready,
                source_server_id=self.name,
            ),
        )

    def describe(self) -> None:
        for task in self.tasks:
            self.emitter.publish(
                Event(
                    task_matcher=task.name,
                    name="describe_server",
                    event_type=EventType.response,
                    source_server_id=self.name,
                    payload={
                        "upstream_tasks": task.upstream_tasks,
                        "downstream_tasks": task.downstream_tasks,
                    },
                ),
            )

    def get_task(self, name: str) -> Task | None:
        return next((task for task in self.tasks if task.name == name), None)

    def run_task(self, name: str) -> None:
        task = self.get_task(name)
        if self._states.get(name) == "running" or not task:
            return

        self._states[name] = "running"
        self.emitter.publish(
            Event(
                task_matcher=name,
                name=EventName.task_state,
                state=State.started,
                source_server_id=self.name,
            ),
        )
        task()
        del self._states[name]
        self.emitter.publish(
            Event(
                task_matcher=name,
                name=EventName.task_state,
                state=State.completed,
                source_server_id=self.name,
            ),
        )

    def check_task_complete(self, name: str) -> bool:
        task = self.get_task(name)
        if task:
            complete = task.is_complete()
            self.emitter.publish(
                Event(
                    task_matcher=name,
                    name=EventName.task_complete,
                    event_type=EventType.response,
                    source_server_id=self.name,
                    payload={"complete": complete},
                ),
            )

        return False

server_a = Server(
    name="server_a",
    tasks=[
        task_a,
        task_b,
        task_c,
        task_d,
    ],
    emitter=emitter,
)
server_b = Server(
    name="server_b",
    tasks=[
        task_1,
        task_2,
    ],
    emitter=emitter,
)


def main() -> None:
    server_a.start()
    server_b.start()

if __name__ == "__main__":
    main()
