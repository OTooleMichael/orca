import time
import pytest

from orca_tools.protos import Event
from orca_tools.task_server_utils import Server
from task_servers import task_server_nums
from generated_grpc import orca_pb2 as pb2
from generated_grpc import orca_enums as en
from tests.utils import (
    Pattern,
    TaskStateMatcher,
    run_task,
    WaitConsumer,
    create_server,
    NameMatcher,
)


def _is_state_event(event: Event) -> bool:
    return isinstance(event, pb2.TaskStateEvent)


def test_passing() -> None:
    with create_server() as emitter:
        run_task(
            "task_d",
            WaitConsumer(
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
    with create_server() as emitter, pytest.raises(AssertionError):
        run_task(
            "task_d",
            WaitConsumer(
                pattern=pattern,
                targeted=_is_state_event,
            ),
            emitter=emitter,
        )


def test_task_a() -> None:
    with create_server() as emitter:
        pattern: Pattern = [
            [
                NameMatcher("task_b"),
                NameMatcher("task_d"),
            ],
            [
                NameMatcher("task_a"),
            ],
        ]
        run_task(
            "task_a",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent)
                and event.event.state == en.TaskState.COMPLETED,
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
    with create_server() as emitter:
        tasks = task_server_nums.server.tasks
        server = Server(name="b", tasks=tasks, emitter=emitter)
        server.start()
        time.sleep(0.4)
        run_task(
            "task_2",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent)
                and event.event.state == en.TaskState.COMPLETED,
            ),
            emitter=emitter,
        )


def test_unknown_task() -> None:
    """Should emit and error when an unknown task is run."""
    pattern: Pattern = [[TaskStateMatcher("task_unknown", en.TaskState.NOT_EXISTING)]]
    with create_server() as emitter:
        run_task(
            "task_unknown",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent),
            ),
            emitter=emitter,
        )


def test_requires_unknown_task() -> None:
    """Should emit and error when required task is unknown."""
    pattern: Pattern = [
        [
            TaskStateMatcher("task_non_existing", en.TaskState.NOT_EXISTING),
            TaskStateMatcher(
                "task_requires_non_extisting_task", en.TaskState.NOT_EXISTING_UPSTREAM
            ),
        ]
    ]
    with create_server() as emitter:
        run_task(
            "task_requires_non_extisting_task",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent),
            ),
            emitter=emitter,
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

    with create_server() as emitter:
        run_task(
            "task_requires_failing",
            WaitConsumer(
                pattern=pattern,
                targeted=lambda event: isinstance(event, pb2.TaskStateEvent),
            ),
            emitter=emitter,
        )
