from dataclasses import dataclass, field
from collections.abc import Callable

from result import Result, Err, Ok
import generated_grpc.orca_pb2 as pb2


STATES = [
    "pending",
    "started",
    "completed",
    "failed",
    "already_complete",
    "failed_upstream",
]

RunableType = Callable[[], None]


@dataclass
class Task:
    """Task class, callable in the case of a Task Server."""

    name: str
    downstream_tasks: list[str] = field(default_factory=list)
    upstream_tasks: list[str] = field(default_factory=list)
    run: RunableType | None = None
    complete_check: Callable[[], bool] | None = None
    _result: Result[None, Exception] | None = None

    @classmethod
    def from_pb(cls, pb: pb2.Task) -> "Task":
        return cls(
            name=pb.name,
            downstream_tasks=list(pb.downstream_tasks),
            upstream_tasks=list(pb.upstream_tasks),
        )

    def to_pb(self) -> pb2.Task:
        return pb2.Task(
            name=self.name,
            downstream_tasks=self.downstream_tasks,
            upstream_tasks=self.upstream_tasks,
        )

    @property
    def result(self) -> Result[None, Exception]:
        if self._result is None:
            return Err(RuntimeError("Task has not been run"))
        return self._result

    def __call__(self) -> None:
        if self.run:
            self.run()

    def safe_run(self) -> Result[None, Exception]:
        try:
            self.__call__()
            self._result = Ok(None)
            return self._result
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self._result = Err(e)
            return self._result

    def is_complete(self) -> bool:
        if self.complete_check:
            return self.complete_check()

        return False


def task(
    *,
    name: str | None = None,
    downstream_tasks: list[str] | None = None,
    upstream_tasks: list[str] | None = None,
    complete_check: Callable[[], bool] | None = None,
) -> Callable[[RunableType], Task]:
    """Decorator to create a Task from a function."""

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
