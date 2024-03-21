from collections.abc import Callable
import functools
import pytest
import time
from dataclasses import dataclass, field

from orca_tools.py_event_server import emitter
from orca_tools.protos import Event
from generated_grpc import orca_pb2 as pb2
from generated_grpc import orca_enums

PatternFn = Callable[[Event], bool]


def message_to_string(message: Event) -> str:
    return "\n".join([message.DESCRIPTOR.name, str(message)])


@dataclass
class WaitConsumer:
    targeted: PatternFn
    pattern: list[list[PatternFn]]
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

    def _consume(self, event: Event) -> bool:
        if not self.targeted(event):
            return False
        if self.debug:
            print(message_to_string(event))
        self.seen.append(event)
        current_layer = self.pattern.pop(0) if self.pattern else None
        if not current_layer:
            return True

        new_layer = list(filter(lambda pattern: not pattern(event), current_layer))
        if new_layer == current_layer:
            self.error = f"No pattern matched for {event!s}"
            print(self)
            return True

        self.matched.append(event)
        if not new_layer:
            return self.empty
        self.pattern = [new_layer] + self.pattern
        return False


def _run_task(task_name: str, consumer: WaitConsumer, timeout: int = 5) -> None:
    """Run a given task and wait according to the pattern of our consumer."""
    time.sleep(1)
    event = pb2.UserRunTaskEvent(
        event=pb2.EventCore(task_name=task_name, source_server_id="cli"),
    )
    thread = emitter.subscribe_thread(lambda e, em: consumer.consume(e))
    emitter.publish(event)
    try:
        thread.join(timeout=timeout)
    except TimeoutError:
        assert not consumer.empty, "Patterns matched but failed to exit thread"
        raise AssertionError("Unmatched patterns")
    assert not consumer.error, consumer
    assert consumer.empty, consumer


def _layer_el_matcher(event: Event, el: tuple[str, orca_enums.TaskState]) -> bool:
    if not isinstance(event, pb2.TaskStateEvent):
        return False
    return event.event.task_name == el[0] and event.event.state == el[1]


def _tuples_to_pattern(
    pattern: list[list[tuple[str, orca_enums.TaskState]]],
) -> list[list[PatternFn]]:
    return [
        ([functools.partial(_layer_el_matcher, el=el) for el in layer])
        for layer in pattern
    ]


def test_passing() -> None:
    pattern = _tuples_to_pattern(
        [
            [("task_d", orca_enums.TaskState.STARTED)],
            [("task_d", orca_enums.TaskState.COMPLETED)],
        ]
    )
    _run_task(
        "task_d",
        WaitConsumer(
            pattern=pattern, targeted=lambda e: isinstance(e, pb2.TaskStateEvent)
        ),
    )


def test_bad_pattern() -> None:
    pattern = _tuples_to_pattern(
        [
            [("task_d", orca_enums.TaskState.COMPLETED)],
            [("task_d", orca_enums.TaskState.STARTED)],
        ]
    )
    with pytest.raises(AssertionError):
        _run_task(
            "task_d",
            WaitConsumer(
                pattern=pattern, targeted=lambda e: isinstance(e, pb2.TaskStateEvent)
            ),
        )
    # The cactual events can overlap with the next test
    time.sleep(2)


def test_task_a() -> None:
    pattern = _tuples_to_pattern(
        [
            # [("task_c", orca_enums.TaskState.COMPLETED)],
            [
                ("task_b", orca_enums.TaskState.COMPLETED),
                ("task_d", orca_enums.TaskState.COMPLETED),
            ],
            [("task_a", orca_enums.TaskState.COMPLETED)],
        ]
    )
    _run_task(
        "task_a",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent)
            and e.event.state == orca_enums.TaskState.COMPLETED,
            debug=True,
        ),
    )


def test_two_servers() -> None:
    pattern = _tuples_to_pattern(
        [
            [
                ("task_b", orca_enums.TaskState.COMPLETED),
                ("task_d", orca_enums.TaskState.COMPLETED),
            ],
            [("task_a", orca_enums.TaskState.COMPLETED)],
            [("task_2", orca_enums.TaskState.COMPLETED)],
        ]
    )
    _run_task(
        "task_2",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent)
            and e.event.state == orca_enums.TaskState.COMPLETED,
            debug=False,
        ),
    )


"""def test_missing_upstream_task() -> None:
    pattern = _tuples_to_pattern(
        [
            # [("task_c", orca_enums.TaskState.COMPLETED)], is complete
            [
                ("task_b", orca_enums.TaskState.COMPLETED),
                ("task_d", orca_enums.TaskState.COMPLETED),
            ],
            [("task_a", orca_enums.TaskState.COMPLETED)],
            [("task_2", orca_enums.TaskState.COMPLETED)],
            ]
    )
    _run_task(
        "task_",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent)
            ,
            debug=False,
        ),
    )"""


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
    pattern = _tuples_to_pattern(
        [
            [
                ("task_3", orca_enums.TaskState.STARTED),
                ("task_failing_root", orca_enums.TaskState.STARTED),
                ("task_3", orca_enums.TaskState.COMPLETED),
                ("task_failing_root", orca_enums.TaskState.FAILED),
            ],
            [("try to complete", orca_enums.TaskState.FAILED_UPSTREAM)],
        ]
    )

    _run_task(
        "task_requires_failing",
        WaitConsumer(
            pattern=pattern,
            targeted=lambda e: isinstance(e, pb2.TaskStateEvent),
            debug=False,
        ),
    )
