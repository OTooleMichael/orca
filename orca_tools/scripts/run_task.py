import sys
from functools import partial

from orca_tools.models import EventName, Event, EventType
from orca_tools.py_event_server import EventBus, emitter


def _waiter(event: Event, _: EventBus, target_task: str) -> bool:
    should_indent = event.name != EventName.task_state
    print("   " if should_indent else "", event)

    is_finised = event.state.is_terminal() and event.task_matcher == target_task

    if is_finised:
        print("Finished!!")
    return is_finised


if __name__ == "__main__":
    task_name = sys.argv[1]
    emitter.publish(
        Event(
            task_matcher=task_name,
            name=EventName.user_run_task,
            event_type=EventType.request,
            source_server_id=":user",
        ),
    )
    emitter.subscribe_thread(partial(_waiter, target_task=task_name))
