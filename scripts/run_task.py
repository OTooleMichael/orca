import sys
from functools import partial

from orca.py_event_server import Event, EventBus, emitter


def _waiter(event: Event, _: EventBus, target_task: str) -> bool:
    if event.name.startswith("task_state:"):
        print(event)

    is_finised = event.name in ("task_state:complete", "task_state:already_complete") and event.task_matcher == target_task
    if is_finised:
        print("Finished!!")
    return is_finised

if __name__ == "__main__":
    task_name = sys.argv[1]
    emitter.publish(
        Event(
            task_matcher=task_name,
            name="user_run_task:req",
            source_server_id=":user",
        ),
    )
    emitter.subscribe_thread(partial(_waiter, target_task=task_name))



