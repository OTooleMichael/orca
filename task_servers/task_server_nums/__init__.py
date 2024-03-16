import time
from orca_tools.task_server_utils import Server, emitter
from orca_tools.models import task, Task


@task(upstream_tasks=["task_2", "task_b", "task_d"])
def task_1() -> None:
    print("Task 1 start")
    time.sleep(2)
    print("Task 1 end")


@task(upstream_tasks=["task_a", "task_c"])
def task_2() -> None:
    print("Task 2")


@task()
def task_3() -> None:
    print("Task 3")


@task(upstream_tasks=["task_failing_root", "task_3"])
def task_requires_failing() -> None:
    print("task_child")


@task()
def task_failing_root() -> None:
    print("Failing Task")
    raise ValueError("Permanent fail")


server = Server(
    name="server_b",
    tasks=[task for task in globals().values() if isinstance(task, Task)],
    emitter=emitter,
)


def main() -> None:
    server.start()


if __name__ == "__main__":
    main()
