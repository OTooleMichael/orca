import time
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing import Process

from orca.py_event_server import Event, EventBus, emitter

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


class Orca:
    emitter: EventBus

    def __init__(self, emitter: EventBus) -> None:
        self.emitter = emitter
        self.tasks: dict[str, str]= {}
        self.graph: dict[str, list[str]]= {}

    def start(self) -> None:
        Process(
            target=self.emitter.subscribe,
            args=(self._handle_server_describe,),
        ).start()
        time.sleep(1)
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
        if event.source_server_id == "orca" or event.name not in ("server_describe:res", "user_run_task:req"):
            return

        print(event)
        if event.name == "user_run_task:req":
            try:
                run = self.create_run(event.task_matcher)
                run()
            except Exception as e:
                print(e)
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

            waiter = Waiter(current_task, upstream_tasks)
            self.emitter.subscribe_thread(waiter)
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

    def __call__(self, event: Event, emitter: EventBus) -> bool:
        if event.name not in ("task_state:complete", "task_state:already_complete"):
            return False
        if event.task_matcher not in self.upstream_tasks:
            return False

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
            return True
        emitter.subscribe_thread(Waiter(self.task_name, remaining))
        return True


orca = Orca(
    emitter=emitter,
)

def main() -> None:
    orca.start()
    print("Orca Listening")

if __name__ == "__main__":
    main()
