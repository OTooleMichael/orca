from collections.abc import Callable, Generator
import inspect
import time
from threading import Lock
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import ContextManager

from orca_tools.models import EventName, EventType, State, Task
from orca_tools.py_event_server import Event, EventBus, emitter
from orca_tools.utils import orca_id


@dataclass
class DagRun:
    task_name: str
    cerebro: "Cerebro"
    edges: list[tuple[str, str | None]] = field(default_factory=list)
    to_visit: list[str] = field(default_factory=list)
    base_tasks: list[str] = field(default_factory=list)
    already_completed: list[str] = field(default_factory=list)
    _pending_completeness_check: list[str] = field(default_factory=list)
    _visited: list[str] = field(default_factory=list)
    _running: bool = False
    _has_lock: bool = False

    def visit(self, task_name: str) -> bool:
        if task_name in self._visited:
            return True
        self._visited.append(task_name)
        return False

    @contextmanager
    def lock(self) -> Generator[None, None, None]:
        if self._has_lock:
            yield
            return
        with self.cerebro.thread_lock():
            self._has_lock = True
            yield
            self._has_lock = False

    def handle_event(self, event: Event, emitter: EventBus) -> None:
        if event.task_matcher not in self._pending_completeness_check:
            return

        if (event.name, event.event_type) != (
            EventName.task_complete,
            EventType.response,
        ):
            return
        with self.lock():
            self._complete_event(event, emitter)

    def _complete_event(self, event: Event, emitter) -> None:
        task_name = event.task_matcher
        self._pending_completeness_check.remove(task_name)
        is_completed = event.payload.get("complete") or False
        if is_completed:
            self.already_completed.append(task_name)
            return self.build(emitter)
        self._extend_to_visit(task_name)
        return self.build(emitter)

    def _start_waiting(self, task_name: str, emitter: EventBus) -> None:
        with self.lock():
            self._pending_completeness_check.append(task_name)
        emitter.publish(
            Event(
                task_matcher=task_name,
                name=EventName.task_complete,
                event_type=EventType.request,
                source_server_id="",
            ),
        )

    def build(self, emitter: EventBus) -> None:
        while self.to_visit:
            current_task = self.to_visit.pop(0)
            if self.visit(current_task):
                continue
            task_state = self.cerebro.get_task_state(
                current_task, has_lock=self._has_lock
            )
            if task_state == State.waiting:
                continue

            is_completed = task_state.is_terminal()
            if is_completed:
                self.already_completed.append(current_task)
                continue

            if task_state not in (State.pending,):
                continue
            self._start_waiting(current_task, emitter)

    @property
    def is_ready(self) -> bool:
        return not self.to_visit and not self._pending_completeness_check

    def _extend_to_visit(self, task_name: str) -> None:
        upstream_tasks = self.cerebro.graph.get(task_name, [])
        if not upstream_tasks:
            self.base_tasks.append(task_name)
            return

        if not self.cerebro.waiters.get(task_name):
            waiter = Waiter(task_name, upstream_tasks, self.cerebro.thread_lock)
            self.cerebro.waiters[task_name] = waiter
        self.to_visit.extend(upstream_tasks)

    def execute_tree(self, emitter: EventBus) -> None:
        if not self.is_ready:
            raise ValueError("DagRun is not ready")
        if self._running:
            print("Already running", self.task_name)
            return
        self._running = True
        for current_task in set(self.already_completed):
            emitter.publish(
                Event(
                    task_matcher=current_task,
                    name=EventName.task_state,
                    state=State.already_complete,
                    source_server_id="",
                ),
            )
        for task_name in set(self.base_tasks):
            emitter.publish(
                Event(
                    task_matcher=task_name,
                    name=EventName.run_task,
                    event_type=EventType.request,
                    source_server_id="",
                ),
            )


@dataclass
class Waiter:
    task_name: str
    upstream_tasks: list[str]
    thread_lock: Callable[[], ContextManager[None]]
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

    def handle(self, event: Event, emitter: EventBus) -> bool:
        if self._is_dead:
            return True

        if event.name != EventName.task_state or not event.state.is_terminal():
            return False

        if event.task_matcher not in self.upstream_tasks:
            return False

        with self.thread_lock():
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
    dag_runs: dict[str, DagRun] = {}
    waiters: dict[str, Waiter] = {}
    _thread_lock: Lock

    def __init__(self, emitter: EventBus) -> None:
        self.emitter = emitter
        self.dag_runs = {}
        self.waiters = {}
        self.tasks: dict[str, str] = {}
        self.task_state: dict[str, Event] = {}
        self._thread_lock = Lock()
        self.graph: dict[str, list[str]] = {}

    @contextmanager
    def thread_lock(self) -> Generator[None, None, None]:
        # print the calling function server_name
        caller = [el[3] for el in inspect.stack()[1:5]]
        print("GETTING LOCK", caller)
        with self._thread_lock:
            yield
        print("RELEASING LOCK", caller)

    def publish(self, event: Event) -> None:
        event.source_server_id = self.server_name
        self.emitter.publish(event)

    def _clean_task_states(self) -> None:
        del_keys: list[str] = []
        for key, event in self.task_state.items():
            if event.state.is_terminal():
                del_keys.append(key)

        if not del_keys:
            return

        with self.thread_lock():
            for key in del_keys:
                del self.task_state[key]

    def start(self) -> None:
        self.emitter.subscribe_thread(self._handler)
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
        for task_name, waiter in list(self.waiters.items()):
            if waiter.handle(event, emitter):
                self.waiters.pop(task_name, None)

        with self.thread_lock():
            self.task_state[event.task_matcher] = event
        self._clean_task_states()
        return

    def _handler(self, event: Event, emitter: EventBus) -> None:
        if event.source_server_id == self.server_name:
            return

        match (event.name, event.event_type):
            case (EventName.task_state, _):
                self._handle_task_state(event, emitter)
                return
            case (EventName.task_complete, EventType.response):
                for key, run in list(self.dag_runs.items()):
                    run.handle_event(event, emitter)
                    if run.is_ready:
                        print("RUNNING TREE", run.task_name)
                        run.execute_tree(emitter)
                        self.dag_runs.pop(key, None)

                return
            case (EventName.user_run_task, EventType.request):
                if dag := self.create_run(event.task_matcher):
                    dag.build(emitter)
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

    def get_task_state(self, task_name: str, has_lock: bool = False) -> State:
        if not has_lock:
            with self.thread_lock():
                return self.get_task_state(task_name, has_lock=True)
        if task_name in self.waiters:
            return State.waiting
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

    def create_run(self, task_name: str) -> DagRun | None:
        if task_name in self.dag_runs:
            print("Already running", task_name)
            return None
        self._clean_task_states()
        connections = self.build_subgraph(task_name)
        self.dag_runs[task_name] = DagRun(
            task_name=task_name,
            cerebro=self,
            edges=connections,
            to_visit=[task_name],
        )
        return self.dag_runs[task_name]


cerebro = Cerebro(
    emitter=emitter,
)


def main() -> None:
    cerebro.start()
    print("Cerebro Listening")


if __name__ == "__main__":
    main()
