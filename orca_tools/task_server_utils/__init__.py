from dataclasses import dataclass, field
import traceback
from result import Err, Result, Ok
import copy
from orca_tools.models import Task
from orca_tools.py_event_server import EventBus, emitter
from orca_tools.protos import Event
from orca_tools.utils import orca_id
from generated_grpc import orca_enums
import generated_grpc.orca_pb2 as pb2
from threading import Thread
from logging import getLogger

logger = getLogger(__name__)


@dataclass
class Server:
    name: str
    tasks: list[Task]
    emitter: EventBus = emitter
    _states: dict[str, str] = field(default_factory=dict)
    server_id: str = field(default_factory=lambda: orca_id("server"))

    def _handle_event(self, event: Event, emitter: EventBus) -> None:
        if event.event.source_server_id == self.name:
            return None
        if isinstance(event, pb2.DescribeServerReq):
            self.describe()
            return None
        if isinstance(event, pb2.RunTaskEvent):
            self.run_task(event.task_name)
            return None
        if isinstance(event, pb2.TaskCompleteReq):
            self.check_task_complete(event.task_name)
            return None
        return None

    def _publish(self, event: Event) -> None:
        event.MergeFrom(event.__class__(event=event.event or pb2.EventCore()))  # type: ignore
        event.event.MergeFrom(
            pb2.EventCore(
                event_id=event.event.event_id or orca_id("evt"),
                source_server_id=":".join([self.name, self.server_id]),
            )
        )
        self.emitter.publish(event)

    def close(self) -> None:
        if not self._listener:
            return
        try:
            self._listener.join(timeout=1)
        except Exception:
            pass

    def start(self) -> None:
        self._listener = self.emitter.subscribe_thread(self._handle_event, "server")
        self.describe()
        self._publish(
            pb2.ServerStateEvent(
                event=pb2.EventCore(),
                state=orca_enums.ServerState.READY.to_grpc(),
            )
        )
        print(f"Server {self.name} is ready")

    def describe(self) -> None:
        for task in self.tasks:
            self._publish(
                pb2.DescribeServerRes(task=task.to_pb(), event=pb2.EventCore())
            )

    def get_task(self, name: str) -> Task | None:
        return next((task for task in self.tasks if task.name == name), None)

    def _safe_run(self, task: Task) -> Result[None, pb2.Error | RuntimeError]:
        # Create a thread to run the function
        task_clone = copy.copy(task)
        try:
            thread = Thread(target=task_clone.safe_run)
            thread.start()
            thread.join()
            return task_clone.result
        except KeyboardInterrupt:
            raise
        except Exception as e:
            return Err(RuntimeError(f"Critical error in running task {task.name} {e}"))

    def run_task(self, name: str) -> None:
        task = self.get_task(name)
        if self._states.get(name) == "running" or not task:
            return

        self._states[name] = "running"
        print(f"Running task {name}")
        self._publish(
            pb2.TaskStateEvent(
                event=pb2.EventCore(
                    task_name=name,
                    state=orca_enums.TaskState.pb().STARTED,
                )
            )
        )
        res = self._safe_run(task)
        print(f"finished task {name} {res.is_err()=}")
        if isinstance(res, Ok):
            del self._states[name]
            self._publish(
                pb2.TaskStateEvent(
                    event=pb2.EventCore(
                        task_name=name,
                        state=orca_enums.TaskState.pb().COMPLETED,
                    )
                )
            )
            return

        del self._states[name]
        error = res.err_value
        if isinstance(error, RuntimeError):
            # A more serious error
            logger.exception(error)
            error = pb2.Error(
                task_name=name,
                text=str(error),
                stack_trace=traceback.format_exc(),
            )

        self._publish(
            pb2.TaskStateEvent(
                error=error,
                event=pb2.EventCore(
                    task_name=name,
                    state=orca_enums.TaskState.pb().FAILED,
                ),
            )
        )

    def check_task_complete(self, name: str) -> bool:
        task = self.get_task(name)
        if not task:
            return False
        complete = task.is_complete()
        self._publish(
            pb2.TaskCompleteRes(
                task_name=name,
                event=pb2.EventCore(
                    task_name=name,
                ),
                state=(
                    orca_enums.TaskState.COMPLETED
                    if complete
                    else orca_enums.TaskState.IDLE
                ).to_pb(),
            )
        )
        return False
