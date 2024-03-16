import time

from orca_tools.task_server_utils import Server, emitter
from orca_tools.models import task, Task


@task(upstream_tasks=["task_c", "task_b", "task_d"])
def task_a() -> None:
    print("Task A")


@task(upstream_tasks=["task_c"])
def task_b() -> None:
    print("Task B")


@task(complete_check=lambda: True)
def task_c() -> None:
    print("Task C")


@task()
def task_d() -> None:
    print("Task D")
    time.sleep(2)
    print("Task D done")


server = Server(
    name="server_a",
    tasks=[task for task in globals().values() if isinstance(task, Task)],
    emitter=emitter,
)


def main() -> None:
    server.start()


if __name__ == "__main__":
    main()