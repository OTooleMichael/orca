from contextlib import closing, contextmanager
from typing import Generator, Protocol
from dataclasses import dataclass, field

from orca_tools.py_event_server import emitter as _emitter
from orca_tools.protos import Event
from orca_tools.py_event_server import MemoryBus, EventBus
from cerebro import Cerebro
from task_servers import task_server_letters
import generated_grpc.orca_pb2 as pb2
from generated_grpc import orca_enums as en


class PatternFn(Protocol):
    def __call__(self, event: Event) -> bool: ...


Pattern = list[list[PatternFn]]


@dataclass
class TaskStateMatcher(PatternFn):
    task_name: str
    state: en.TaskState

    def _pair(self) -> tuple[str, en.TaskState]:
        return self.task_name, self.state

    def __call__(self, event: Event) -> bool:
        if not isinstance(event, pb2.TaskStateEvent):
            return False
        return self._pair() == (event.event.task_name, event.event.state)


@dataclass
class NameMatcher:
    task_name: str

    def __call__(self, event: Event) -> bool:
        if not isinstance(event, pb2.TaskStateEvent):
            return False
        return event.event.task_name == self.task_name


def message_to_string(message: Event) -> str:
    return "\n".join([message.DESCRIPTOR.name, str(message)])


@dataclass
class WaitConsumer:
    """Expects events to arrive in an order.
    Closes thread when all patterns are matched,
    or an unknown event is received.
    """

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


def run_task(
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
def create_server() -> Generator[MemoryBus, None, None]:
    """Create a A server and a Cerebro server and wait for them to start."""
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
