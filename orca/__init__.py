import dataclasses
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial


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

        def _unsub() -> None:
            self.unsubscribe(callback)

        return _unsub

    def unsubscribe(self, callback: Callable[[Event, "EventBus"], None]) -> None:
        self._subscribers.remove(callback)

    def publish(self, event: Event) -> None:
        print("Publishing", event)
        for subscriber in self._subscribers:
            subscriber(event, self)

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


@task()
def task_a() -> None:
    print("Task A")


@task(upstream_tasks=["task_a"], complete_check=lambda: True)
def task_b() -> None:
    print("Task B")


@task(upstream_tasks=["task_a"])
def task_1() -> None:
    print("Task 1 start")
    time.sleep(20)
    print("Task 1 end")


@task(upstream_tasks=["task_1", "task_b"])
def task_2() -> None:
    print("Task 2")


@dataclasses.dataclass
class Server:
    name: str
    tasks: list[Task]
    emitter: EventBus = emitter
    _states: dict[str, str] = field(default_factory=dict)

    def _handle_event(self, event: Event, _: EventBus) -> None:
        if event.source_server_id == self.name:
            return None
        if event.name == "task_run:req":
            return self.run_task(event.task_matcher)
        if event.name == "task_complete:req":
            self.check_task_complete(event.task_matcher)
            return None

        if event.name == "server_describe:req":
            self.describe()
            return None

        return None

    def start(self) -> None:
        self.emitter.subscribe(self._handle_event)
        self.describe()
        self.emitter.publish(
            Event(
                task_matcher="",
                name="server_state:ready",
                source_server_id=self.name,
            ),
        )

    def describe(self) -> None:
        for task in self.tasks:
            self.emitter.publish(
                Event(
                    task_matcher=task.name,
                    name="server_describe:res",
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
        if self._states.get(name) == "running":
            return

        if task:
            self._states[name] = "running"
            self.emitter.publish(
                Event(
                    task_matcher=name,
                    name="task_state:started",
                    source_server_id=self.name,
                ),
            )
            task()
            del self._states[name]
            self.emitter.publish(
                Event(
                    task_matcher=name,
                    name="task_state:complete",
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
                    name="task_complete:res",
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

class Orca:
    emitter: EventBus

    def __init__(self, emitter: EventBus) -> None:
        self.emitter = emitter
        self.tasks: dict[str, str]= {}
        self.graph: dict[str, list[str]]= {}
        self.emitter.subscribe(self._handle_server_describe)

    def start(self) -> None:
        self.emitter.publish(
            Event(
                task_matcher="",
                name="server_describe:req",
                source_server_id="orca",
            ),
        )

    def _add_task(self, task: Task, server_name: str) -> None:
        def _graph_edge(from_key: str, to_key: str) -> None:
            self.graph[from_key] = self.graph.get(from_key, [])
            self.graph[from_key].append(to_key)
        self.tasks[task.name] = server_name
        for downstream_task in task.downstream_tasks:
            _graph_edge(downstream_task, task.name)
        for upstream_task in task.upstream_tasks:
            _graph_edge(task.name, upstream_task)

    def _handle_server_describe(self, event: Event, _: EventBus) -> None:
        if event.source_server_id == "orca" or event.name != "server_describe:res":
            return
        payload = event.payload or {}
        task = Task(
            name=event.task_matcher,
            downstream_tasks=payload["downstream_tasks"],
            upstream_tasks=payload["upstream_tasks"],
        )
        self._add_task(task, event.source_server_id)
        return

    def create_run(self, task_name: str) -> Callable[[], None]:
        subgraph_items = []
        to_visit = [task_name]
        while to_visit:
            current = to_visit.pop(0)
            if current in subgraph_items:
                continue

            if not self.tasks.get(current):
                raise ValueError(f"Task {current} does not exist")

            subgraph_items.append(current)
            to_visit.extend(self.graph.get(current, []))


        search_list = [task_name]
        base_tasks = []

        while search_list:
            current_task = search_list.pop(0)
            complete_res = self.emitter.request(
                Event(
                    task_matcher=current_task,
                    name="task_complete:req",
                    source_server_id="orca",
                ),
            )
            if (complete_res.payload or {}).get("complete"):
                self.emitter.publish(
                    Event(
                        task_matcher=current_task,
                        name="task_state:already_complete",
                        source_server_id="orca",
                    ),
                )
                continue

            upstream_tasks = self.graph.get(current_task, [])
            if not upstream_tasks:
                base_tasks.append(current_task)
                continue

            self.emitter.subscribe(Waiter(current_task, upstream_tasks))
            search_list.extend(upstream_tasks)

        def execute_tree() -> None:
            for task_name in base_tasks:
                emitter.publish(
                    Event(
                        task_matcher=task_name,
                        name="task_run:req",
                        source_server_id="orca",
                    ),
                )
        return execute_tree


@dataclass
class Waiter:
    task_name: str
    upstream_tasks: list[str]

    def __call__(self, event: Event, emitter: EventBus) -> None:
        if event.name not in ("task_state:complete", "task_state:already_complete"):
            return
        if event.task_matcher not in self.upstream_tasks:
            return
        emitter.unsubscribe(self)
        remaining = [task for task in self.upstream_tasks if task != event.task_matcher]
        print("Remaining", remaining, self.task_name)
        if not remaining:
            emitter.publish(
                Event(
                    task_matcher=self.task_name,
                    name="task_run:req",
                    source_server_id="orca",
                ),
            )
            return
        emitter.subscribe(Waiter(self.task_name, remaining))


orca = Orca(
    emitter=emitter,
)

def main() -> None:
    server_a.start()
    server_b.start()
    orca.start()
    run = orca.create_run("task_2")
    run()

if __name__ == "__main__":
    main()
