import sys
from functools import partial

from orca_tools.py_event_server import EventBus, emitter
from orca_tools.protos import Event
from generated_grpc import orca_pb2 as pb2


def _waiter(event: Event, _: EventBus, target_task: str) -> bool:
    should_indent = isinstance(event, pb2.TaskStateEvent)
    print("   " if should_indent else "", event)
    if not isinstance(event, pb2.TaskStateEvent):
        return False

    is_finised = event.event.task_name == target_task and event.event.state in (
        pb2.FAILED,
        pb2.COMPLETED,
        pb2.ALREADY_COMPLETED,
        pb2.FAILED_UPSTREAM,
    )

    if is_finised:
        print("Finished!!")
    return is_finised


def main() -> None:
    task_name = sys.argv[1]
    print("start", task_name)
    event = pb2.UserRunTaskEvent(
        event=pb2.EventCore(task_name=task_name, source_server_id="cli"),
    )
    print(event)
    emitter.subscribe_thread(partial(_waiter, target_task=task_name))
    print("subscribed")
    emitter.publish(event)
    print("published")


if __name__ == "__main__":
    main()
