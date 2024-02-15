import time
from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, Field

from orca_tools.utils import orca_id

STATES = [
    "pending",
    "started",
    "completed",
    "failed",
    "already_complete",
    "failed_upstream",
]

class EventType(Enum):
    event = "event"
    request = "request"
    response = "response"

class State(Enum):
    ready = "ready"
    pending = "pending"
    started = "started"
    completed = "completed"
    failed = "failed"
    already_complete = "already_complete"
    failed_upstream = "failed_upstream"
    na = "na"

    def is_terminal(self) -> bool:
        return self in (State.completed, State.failed, State.already_complete, State.failed_upstream)

class EventName(Enum):
    user_run_task = "user_run_task"
    task_state = "task_state"
    task_complete = "task_complete"
    run_task = "run_task"
    server_state = "server_state"
    describe_server = "describe_server"

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, str):
            return self.value == __value
        return super().__eq__(__value)


class Event(BaseModel):
    task_matcher: str
    name: EventName
    event_type: EventType = EventType.event
    source_server_id: str
    state: State = State.na
    payload: dict = Field(default_factory=dict)
    event_epoch: int = Field(default_factory=lambda: int(time.time() * 1000))
    event_id: str = Field(default_factory=lambda : orca_id("ev"))

    @property
    def event_at(self) -> datetime:
        return datetime.fromtimestamp(self.event_epoch/1000, tz=UTC)

    def __repr__(self) -> str:
        return f"<Ev {self.name}, {self.state}, {self.task_matcher}>"
