from contextlib import closing, contextmanager
import time
from typing import Generator, Protocol
import pytest
from dataclasses import dataclass, field

from orca_tools.py_event_server import emitter as _emitter
from orca_tools.protos import Event
from orca_tools.py_event_server import MemoryBus, EventBus
from cerebro import Cerebro
from orca_tools.task_server_utils import Server
from task_servers import task_server_letters, task_server_nums
from generated_grpc import orca_pb2 as pb2
from generated_grpc import orca_enums as en


class PatternFn(Protocol):
    def __call__(self, event: Event) -> bool: ...


@dataclass
class TaskStateMatcher(PatternFn):
    task_name: str
    state: en.TaskState

    def __call__(self, event: Event) -> bool:
        if not isinstance(event, pb2.TaskStateEvent):
            return False
        return (
            event.event.task_name == self.task_name and event.event.state == self.state
        )


def message_to_string(message: Event) -> str:
    return "\n".join([message.DESCRIPTOR.name, str(message)])


Pattern = list[list[PatternFn]]


@dataclass
class WaitConsumer:
    targeted: PatternFn
    pattern: Pattern
    matched: list[Event] = field(default_factory=list)
    seen: list[Event] = field(default_factory=list)
    error: str = ""
    debug: bool = False

    def __repr__(self) -> str:
        matched = "\n".join([message_to_string(message) for message in self.matched])
        seen = "\n".join([message_to_string(message) for message in self.seen])
        return f"{self.error} {self.empty=}\nMatched:\n{matched},\nSeen:\n{seen}"

    @property
    def empty(self) -> bool:
        return not self.pattern and not self.error

    def consume(self, event: Event) -> bool:
        """Expects events to arrive in an order.
        Closes thread when all patterns are matched,
        or an unknown event is received.
        """
        return self._consume(event)

    def handle(self, event: Event, emitter: EventBus) -> bool:
        return self._consume(event)

    def _consume(self, event: Event) -> bool:
        if not self.targeted(event):
            return False

        if self.debug:
            print(message_to_string(event))

        self.seen.append(event)
        current_layer = self.pattern.pop(0) if self.pattern else None
        if not current_layer:
            print("No more patterns to match")
            return True

        new_layer = list(filter(lambda pattern: not pattern(event), current_layer))
        if len(new_layer) == len(current_layer):
            self.error = f"No pattern matched for {event!s}"
            print(self.error)
            return True

        self.matched.append(event)
        if not new_layer:
            return self.empty

        self.pattern = [new_layer] + self.pattern
        return False


def _run_task(
    task_name: str,
    consumer: WaitConsumer,
    timeout: int = 5,
    emitter: EventBus = _emitter,
) -> None:
    """Run a given task and wait according to the pattern of our consumer."""
    event = pb2.UserRunTaskEvent(
        event=pb2.EventCore(task_name=task_name, source_server_id="cli"),
    )
    thread = emitter.subscribe_thread(consumer.handle, "consumer")
    emitter.publish(event)
    try:
        thread.join(timeout=timeout)
    except TimeoutError:
        assert not consumer.empty, "Patterns matched but failed to exit thread"
        raise AssertionError("Unmatched patterns")
    assert not consumer.error, consumer
    assert consumer.empty, consumer


def _is_state_event(event: Event) -> bool:
    return isinstance(event, pb2.TaskStateEvent)


@dataclass
class ServersStarted:
    servers: list[str]

    def __call__(self, event: Event, emitter: EventBus) -> bool:
        if not isinstance(event, pb2.ServerStateEvent):
            return False
        if event.event.state != en.ServerState.READY:
            return False
        self.servers = [
            server for server in self.servers if server != event.event.source_server_id
        ]
        return not self.servers


@contextmanager
def _create_server() -> Generator[MemoryBus, None, None]:
    emitter = MemoryBus()
    with closing(emitter):
        cer = Cerebro(emitter=emitter)
        tasks = task_server_letters.tasks
        server = task_server_letters.Server(name="a", tasks=tasks, emitter=emitter)
        ready_check = ServersStarted(
            servers=[
                cer.server_name,
                server.name,
            ]
        )
        thread = emitter.subscribe_thread(ready_check, "ready_check")
        server.start()
        cer.start()
        thread.join(timeout=1)
        yield emitter


def test_passing() -> None:
    with _create_server() as emitter:
        _run_task(
            "task_d",
            WaitConsumer(
                debug=True,
                pattern=[
                    [TaskStateMatcher("task_d", en.TaskState.STARTED)],
                    [TaskStateMatcher("task_d", en.TaskState.COMPLETED)],
                ],
                targeted=_is_state_event,
            ),
            emitter=emitter,
        )


def test_bad_pattern() -> None:
    pattern: Pattern = [
        [TaskStateMatcher("task_d", en.TaskState.COMPLETED)],
        [TaskStateMatcher("task_d", en.TaskState.STARTED)],
    ]
    with _create_server() as emitter, pytest.raises(AssertionError):
        _run_task(
            "task_d",
            WaitConsumer(
                pattern=pattern,
                targeted=_is_state_event,
            ),
            emitter=emitter,
        )


@dataclass
class NameMatcher:
    task_name: str

    def __call__(self, event: Event) -> bool:
        if not isinstance(event, pb2.TaskStateEvent):
            return False
        return event.event.task_name == self.task_name


def test_task_a() -> None:
    with _create_server() as emitter:
        pattern: Pattern = [
            [
                NameMatcher("task_b"),
                NameMatcher("task_d"),
            ],
            [
                NameMatcher("task_a"),
            ],
        ]
        _run_task(
            "task_a",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent)
                and event.event.state == en.TaskState.COMPLETED,
                debug=True,
            ),
            emitter=emitter,
        )


def test_two_servers() -> None:
    pattern: Pattern = [
        [
            NameMatcher("task_b"),
            NameMatcher("task_d"),
        ],
        [NameMatcher("task_a")],
        [NameMatcher("task_2")],
    ]
    with _create_server() as emitter:
        tasks = task_server_nums.server.tasks
        server = Server(name="b", tasks=tasks, emitter=emitter)
        server.start()
        time.sleep(1)
        _run_task(
            "task_2",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent)
                and event.event.state == en.TaskState.COMPLETED,
                debug=True,
            ),
            emitter=emitter,
        )

def test_unknown_task() -> None:
    """Should emit and error when an unknown task is run."""
    pattern = _tuples_to_pattern([
        [
                ("task_unknown", orca_enums.TaskState.NOT_EXISTING),
        ]
    ])
    #with pytest.raises(AssertionError):
    _run_task(
        "task_unknown",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent),
            debug=False,
        ),
    )

def test_requires_unknown_task() -> None:
    """Should emit and error when required task is unknown."""
    pattern = _tuples_to_pattern([
        [
            ("task_non_existing", orca_enums.TaskState.NOT_EXISTING),
            ("task_requires_non_extisting_task", orca_enums.TaskState.FAILED_UPSTREAM)
        ]
    ])
    _run_task(
        "task_requires_non_extisting_task",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent),
            debug=False,
        ),
    )





@pytest.mark.skip
def test_broken_graph_deps() -> None:
    """Should emit and error when the runnable dag has a broken dependency."""


@pytest.mark.skip
def two_dags_that_overlap_should_not_interfer() -> None:
    """Dags that overlap should run, but share the shared tasks."""


@pytest.mark.skip
def test_shared_dags_should_have_dag_markers() -> None:
    """If a task is shared between two dags, it should have a marker/id for each dag."""


"""
LifeCycle
- acknolwedge
- start
- pre_run_test_start
- pre_run_start
- post_run_start
- post_run_test_start
- pre_commit_start
- post_commit_start
- complete
"""


@pytest.mark.skip
def test_pretest_hook() -> None:
    """Should run a hook before the task is run."""


@pytest.mark.skip
def test_posttest_hook() -> None:
    """Should run a hook after the task is run."""


@pytest.mark.skip
def test_precommit_hook() -> None:
    """Should run a hook before the task is committed."""


@pytest.mark.skip
def test_postcommit_hook() -> None:
    """Should run a hook after the task is committed."""


def test_failing_upstream() -> None:
    pattern: Pattern = [
        [
            TaskStateMatcher("task_d", en.TaskState.STARTED),
            TaskStateMatcher("task_d", en.TaskState.COMPLETED),
            TaskStateMatcher("task_failing_root", en.TaskState.STARTED),
            TaskStateMatcher("task_failing_root", en.TaskState.FAILED),
        ],
        [TaskStateMatcher("task_requires_failing", en.TaskState.FAILED_UPSTREAM)],
    ]

    with _create_server() as emitter:
        _run_task(
            "task_requires_failing",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent),
                debug=False,
            ),
            emitter=emitter,
        )
