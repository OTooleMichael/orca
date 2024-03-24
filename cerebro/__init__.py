from collections.abc import Callable, Generator

# import inspect
import time
from threading import Lock
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import ContextManager

from orca_tools.models import Task
from orca_tools.py_event_server import EventBus, MemoryBus, emitter as _emitter
from orca_tools.utils import orca_id
from orca_tools.protos import Event, is_terminal_state
from generated_grpc import orca_pb2 as pb2, orca_enums


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

    def handle_event(self, event: pb2.TaskCompleteRes, emitter: EventBus) -> None:
        task_name = event.event.task_name
        if task_name not in self._pending_completeness_check:
            return

        with self.lock():
            self._complete_event(event, emitter)

    def _complete_event(self, event: pb2.TaskCompleteRes, emitter) -> None:
        task_name = event.event.task_name
        self._pending_completeness_check.remove(task_name)
        is_completed = event.state == orca_enums.TaskState.COMPLETED
        if is_completed:
            self.already_completed.append(task_name)
            return self.build(emitter)
        self._extend_to_visit(task_name)
        return self.build(emitter)

    def _start_waiting(self, task_name: str, emitter: EventBus) -> None:
        with self.lock():
            self._pending_completeness_check.append(task_name)
        emitter.publish(
            pb2.TaskCompleteReq(
                task_name=task_name,
                event=pb2.EventCore(
                    event_id=orca_id("event"),
                    source_server_id="cerebro",
                    task_name=task_name,
                ),
            )
        )

    def build(self, emitter: EventBus) -> None:
        while self.to_visit:
            current_task = self.to_visit.pop(0)
            if self.visit(current_task):
                continue
            task_state = self.cerebro.get_task_state(
                current_task, has_lock=self._has_lock
            )
            if task_state == orca_enums.TaskState.WAITING:
                continue

            is_completed = is_terminal_state(task_state)
            if is_completed:
                self.already_completed.append(current_task)
                continue

            if task_state not in (orca_enums.TaskState.IDLE,):
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
            waiter = Waiter(task_name, set(upstream_tasks), self.cerebro.thread_lock)
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
            ac_event = pb2.TaskStateEvent(
                event=pb2.EventCore(
                    event_id=orca_id("event"),
                    source_server_id="cerebro",
                    task_name=current_task,
                    state=pb2.ALREADY_COMPLETED,
                )
            )
            emitter.publish(ac_event)
            self.cerebro._handler(ac_event, emitter)
        for task_name in set(self.base_tasks):
            emitter.publish(
                pb2.RunTaskEvent(
                    task_name=task_name,
                    event=pb2.EventCore(
                        event_id=orca_id("event"),
                        source_server_id="cerebro",
                        task_name=task_name,
                    ),
                ),
            )


@dataclass
class Waiter:
    task_name: str
    upstream_tasks: set[str]
    thread_lock: Callable[[], ContextManager[None]]
    waiter_id: str = field(default_factory=lambda: orca_id("waiter"))
    _is_dead: bool = False
    _is_failed: bool = False
    _original_upstream_tasks: set[str] = field(init=False, default_factory=set)

    def run(self, emitter: EventBus) -> None:
        if self._is_failed:
            emitter.publish(
                pb2.TaskStateEvent(
                    event=pb2.EventCore(
                        event_id=orca_id("event"),
                        source_server_id="cerebro",
                        task_name=self.task_name,
                        state=pb2.FAILED_UPSTREAM,
                    ),
                ),
            )
            return
        emitter.publish(
            pb2.RunTaskEvent(
                task_name=self.task_name,
                event=pb2.EventCore(
                    event_id=orca_id("event"),
                    source_server_id="cerebro",
                    task_name=self.task_name,
                ),
            ),
        )

    def handler(self, event: pb2.TaskStateEvent, emitter: EventBus) -> bool:
        if self._is_dead:
            return True

        if not is_terminal_state(event.event.state):
            return False

        task_name = event.event.task_name
        if task_name not in self.upstream_tasks:
            return False

        if orca_enums.TaskState(event.event.state) in (
            orca_enums.TaskState.FAILED,
            orca_enums.TaskState.FAILED_UPSTREAM,
        ):
            self._is_failed = True

        with self.thread_lock():
            if not self._original_upstream_tasks:
                self._original_upstream_tasks = self.upstream_tasks
            self.upstream_tasks = {
                task for task in self.upstream_tasks if task != task_name
            }

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
        self.task_state: dict[str, pb2.TaskStateEvent] = {}
        self._thread_lock = Lock()
        self.graph: dict[str, list[str]] = {}

    @contextmanager
    def thread_lock(self) -> Generator[None, None, None]:
        # print the calling function server_name
        # caller = [el[3] for el in inspect.stack()[1:5]]
        with self._thread_lock:
            yield

    def publish(self, event: Event) -> None:
        event.event.source_server_id = self.server_name
        self.emitter.publish(event)

    def _clean_task_states(self) -> None:
        del_keys: list[str] = []
        for key, event in self.task_state.items():
            if is_terminal_state(event.event.state):
                del_keys.append(key)

        if not del_keys:
            return

        with self.thread_lock():
            for key in del_keys:
                del self.task_state[key]

    def close(self) -> None:
        if not self._listener:
            return
        try:
            self._listener.join(timeout=1)
        except Exception:
            pass

    def start(self) -> None:
        print("Starting")
        self._listener = self.emitter.subscribe_thread(self._handler, "crebro")
        self.publish(
            pb2.DescribeServerReq(
                event=pb2.EventCore(
                    event_id=orca_id("event"),
                    task_name="",
                )
            ),
        )
        print("Cerebro started")

    def _graph_edge(self, from_key: str, to_key: str) -> None:
        self.graph[from_key] = self.graph.get(from_key, [])
        self.graph[from_key].append(to_key)

    def _add_task(self, task: Task, server_name: str) -> None:
        self.tasks[task.name] = server_name
        for downstream_task in task.downstream_tasks:
            self._graph_edge(downstream_task, task.name)
        for upstream_task in task.upstream_tasks:
            self._graph_edge(task.name, upstream_task)

    def _handle_task_state(self, event: pb2.TaskStateEvent, emitter: EventBus) -> None:
        if orca_enums.TaskState.from_grpc(event.event.state) in (
            orca_enums.TaskState.NA,
        ):
            return
        for task_name, waiter in list(self.waiters.items()):
            if waiter.handler(event, emitter):
                self.waiters.pop(task_name, None)

        with self.thread_lock():
            self.task_state[event.event.task_name] = event
        self._clean_task_states()
        return

    def _handler(self, event: Event, emitter: EventBus) -> None:
        if isinstance(event, pb2.DescribeServerRes):
            self._add_task(
                Task.from_pb(event.task),
                event.event.source_server_id,
            )
            return
        if isinstance(event, pb2.TaskStateEvent):
            self._handle_task_state(event, emitter)
            return
        if isinstance(event, pb2.TaskCompleteRes):
            for key, run in list(self.dag_runs.items()):
                run.handle_event(event, emitter)
                if run.is_ready:
                    print("RUNNING TREE", run.task_name)
                    run.execute_tree(emitter)
                    self.dag_runs.pop(key, None)
            return
        if isinstance(event, pb2.UserRunTaskEvent):
            task_name = event.event.task_name
            if dag := self.create_run(task_name):
                dag.build(emitter)
            return
        if event.event.source_server_id == self.server_name:
            return
        print("UNKNOWN EVENT", event.DESCRIPTOR.name, event)
        return

    def get_task_state(
        self, task_name: str, has_lock: bool = False
    ) -> orca_enums.TaskState:
        if not has_lock:
            with self.thread_lock():
                return self.get_task_state(task_name, has_lock=True)
        if task_name in self.waiters:
            return orca_enums.TaskState.WAITING
        event = self.task_state.get(task_name)
        return (
            orca_enums.TaskState.from_grpc(event.event.state)
            if event
            else orca_enums.TaskState.IDLE
        )

    def build_subgraph(self, task_name: str) -> list[tuple[str, str | None]]:
        connections: list[tuple[str, str | None]] = []
        subgraph_items = []
        to_visit = [task_name]
        while to_visit:
            current = to_visit.pop(0)
            if current in subgraph_items:
                continue

            if not self.tasks.get(current):
                event = pb2.TaskStateEvent(
                    event=pb2.EventCore(
                        event_id=orca_id("event"),
                        source_server_id="cerebro",
                        task_name=current,
                        state=pb2.NOT_EXISTING,
                    )
                )
                self.emitter.publish(event)
                event2 = pb2.TaskStateEvent(
                    event=pb2.EventCore(
                        event_id=orca_id("event"),
                        source_server_id="cerebro",
                        task_name=task_name,
                        state=pb2.FAILED_UPSTREAM,  # possible additional state: NOT_EXISTING_TASK_UPSTREAM
                    )
                )
                self.emitter.publish(event2)
                return []

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
        if not connections:
            return None
        self.dag_runs[task_name] = DagRun(
            task_name=task_name,
            cerebro=self,
            edges=connections,
            to_visit=[task_name],
        )
        return self.dag_runs[task_name]


cerebro = Cerebro(
    emitter=_emitter,
)


def main() -> None:
    cerebro.start()
    print("Cerebro Listening")


if __name__ == "__main__":
    main()
