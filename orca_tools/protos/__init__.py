from datetime import UTC, datetime
from typing import Union
from google.protobuf.timestamp_pb2 import Timestamp as _Timestamp
from generated_grpc import orca_pb2 as pb2
from generated_grpc import orca_enums
from google.protobuf import message as _message

from orca_tools.utils import orca_id


_NULL_BYTE = b"\x00"

Event = (
    pb2.TaskStateEvent
    | pb2.RunTaskEvent
    | pb2.TaskCompleteReq
    | pb2.TaskCompleteRes
    | pb2.ServerStateEvent
    | pb2.UserRunTaskEvent
    | pb2.DescribeServerReq
    | pb2.DescribeServerRes
)


def is_terminal_state(state: Union[pb2.TaskState, orca_enums.TaskState]) -> bool:
    state = (
        state
        if isinstance(state, orca_enums.TaskState)
        else orca_enums.TaskState.from_grpc(state)
    )
    return state in (
        orca_enums.TaskState.COMPLETED,
        orca_enums.TaskState.FAILED,
        orca_enums.TaskState.ALREADY_COMPLETED,
        orca_enums.TaskState.FAILED_UPSTREAM,
    )


def encode_message(message: _message.Message) -> bytes:
    event_str = message.SerializeToString()
    name = message.DESCRIPTOR.name.encode("utf-8")
    return name + _NULL_BYTE + event_str


def decode_message(data: bytes) -> _message.Message:
    first_null = data.index(_NULL_BYTE)
    message_type = data[:first_null]
    message = getattr(pb2, message_type.decode("utf-8")).FromString(
        data[first_null + 1 :]
    )
    return message


def encode_event(event: Event) -> bytes:
    event.event.MergeFrom(
        pb2.EventCore(
            event_at=Timestamp(),
            state=event.event.state or pb2.TaskState.Value("NA"),  # type: ignore
            event_id=event.event.event_id or orca_id("evt"),
        )
    )
    return encode_message(event)


def decode_event(data: bytes) -> Event:
    return decode_message(data)  # type: ignore


def Timestamp(dt: None | datetime = None) -> _Timestamp:
    dt = dt or datetime.now(tz=UTC)
    timestamp = _Timestamp()
    timestamp.FromDatetime(dt=dt)
    return timestamp
