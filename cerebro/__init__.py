import time
from threading import Lock
from dataclasses import dataclass, field
from collections.abc import Callable

from orca_tools.models import EventName, EventType, State, Task
from orca_tools.py_event_server import Event, EventBus, emitter
from orca_tools.utils import orca_id


@dataclass
class Waiter:
    task_name: str
    upstream_tasks: list[str]
    thread_lock: Lock
    waiter_id: str = field(default_factory=lambda: orca_id("waiter"))
    _is_dead: bool = False
    _original_upstream_tasks: list[str] = field(init=False, default_factory=list)

    def run(self, emitter: EventBus) -> None:
        emitter.publish(
            Event(
                task_matcher=self.task_name,
                name=EventName.run_task,
                event_type=EventType.request,
                source_server_id="orca",
            ),
        )

    def __call__(self, event: Event, emitter: EventBus) -> bool:
        if self._is_dead:
            return True

        if event.name != EventName.task_state or not event.state.is_terminal():
            return False

        if event.task_matcher not in self.upstream_tasks:
            return False

        with self.thread_lock:
            if not self._original_upstream_tasks:
                self._original_upstream_tasks = self.upstream_tasks
            self.upstream_tasks = [
                task for task in self.upstream_tasks if task != event.task_matcher
            ]

        if not self.upstream_tasks:
            self._is_dead = True
            self.run(emitter)
            return True
        return False


class Cerebro:
    emitter: EventBus
    server_name: str = "cerebro"

    def __init__(self, emitter: EventBus) -> None:
        self.emitter = emitter
        self.tasks: dict[str, str] = {}
        self.task_state: dict[str, Event] = {}
        self.thread_lock = Lock()
        self.graph: dict[str, list[str]] = {}

    def _clean_task_states(self) -> None:
        del_keys: list[str] = []
        for key, event in self.task_state.items():
            if event.state.is_terminal():
                del_keys.append(key)

        if not del_keys:
            return

        with self.thread_lock:
            for key in del_keys:
                del self.task_state[key]

    def start(self) -> None:
        self.emitter.subscribe_thread(self._handler, "testing_event_thread")
        time.sleep(1)
        self.emitter.publish(
            Event(
                task_matcher="",
                name=EventName.describe_server,
                event_type=EventType.request,
                source_server_id=self.server_name,
            ),
        )

    def _graph_edge(self, from_key: str, to_key: str) -> None:
        self.graph[from_key] = self.graph.get(from_key, [])
        self.graph[from_key].append(to_key)

    def _add_task(self, task: Task, server_name: str) -> None:
        self.tasks[task.name] = server_name
        for downstream_task in task.downstream_tasks:
            self._graph_edge(downstream_task, task.name)
        for upstream_task in task.upstream_tasks:
            self._graph_edge(task.name, upstream_task)

    def _handle_task_state(self, event: Event, _: EventBus) -> None:
        if event.state in (State.na,):
            return
        with self.thread_lock:
            self.task_state[event.task_matcher] = event
        self._clean_task_states()
        return

    def _handler(self, event: Event, _: EventBus) -> None:
        if event.source_server_id == self.server_name:
            return
        print("Cerebro", event.name, event.event_type)

        match (event.name, event.event_type):
            case (EventName.task_state, _):
                self._handle_task_state(event, _)
                return
            case (EventName.run_task, EventType.request):
                try:
                    run_fn = self.create_run(event.task_matcher)
                    run_fn()
                except Exception as e:
                    print(e)
                return
            case (EventName.describe_server, EventType.response):
                task = Task(
                    name=event.task_matcher,
                    downstream_tasks=event.payload["downstream_tasks"],
                    upstream_tasks=event.payload["upstream_tasks"],
                )
                self._add_task(task, event.source_server_id)
                return
            case _:
                return

    def get_task_state(self, task_name: str) -> State:
        with self.thread_lock:
            event = self.task_state.get(task_name)
        return event.state if event else State.pending

    def build_subgraph(self, task_name: str) -> list[tuple[str, str | None]]:
        connections: list[tuple[str, str | None]] = []
        subgraph_items = []
        to_visit = [task_name]
        while to_visit:
            current = to_visit.pop(0)
            if current in subgraph_items:
                continue

            if not self.tasks.get(current):
                raise ValueError(f"Task {current} does not exist")

            subgraph_items.append(current)
            children = self.graph.get(current, [])
            to_visit.extend(children)
            children = children or [None]
            connections.extend([(current, child) for child in children])
        return connections

    def create_run(self, task_name: str) -> Callable[[], None]:
        self._clean_task_states()
        self.build_subgraph(task_name)

        search_list = [task_name]
        visited = []
        base_tasks = []
        already_completed = []
        while search_list:
            current_task = search_list.pop(0)
            if current_task in visited:
                continue
            visited.append(current_task)
            task_state = self.get_task_state(current_task)
            is_completed = (
                task_state.is_terminal()
                or self.emitter.request(
                    Event(
                        task_matcher=current_task,
                        name=EventName.task_complete,
                        event_type=EventType.request,
                        source_server_id=self.server_name,
                    ),
                ).payload.get("complete")
                or False
            )
            if is_completed:
                already_completed.append(current_task)
                continue

            if task_state not in (State.pending,):
                continue

            upstream_tasks = self.graph.get(current_task, [])
            if not upstream_tasks:
                base_tasks.append(current_task)
                continue

            waiter = Waiter(current_task, upstream_tasks, self.thread_lock)
            self.emitter.subscribe_thread(waiter)
            search_list.extend(upstream_tasks)

        def execute_tree() -> None:
            for current_task in set(already_completed):
                self.emitter.publish(
                    Event(
                        task_matcher=current_task,
                        name=EventName.task_state,
                        state=State.already_complete,
                        source_server_id=self.server_name,
                    ),
                )
            for task_name in set(base_tasks):
                emitter.publish(
                    Event(
                        task_matcher=task_name,
                        name=EventName.run_task,
                        event_type=EventType.request,
                        source_server_id=self.server_name,
                    ),
                )

        return execute_tree


cerebro = Cerebro(
    emitter=emitter,
)


def main() -> None:
    cerebro.start()
    print("Cerebro Listening")


if __name__ == "__main__":
    main()
