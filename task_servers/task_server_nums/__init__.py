from orca_tools.task_server_utils import Server, emitter
from orca_tools.models import task, Task


@task(upstream_tasks=["task_2", "task_b", "task_d"])
def task_1() -> None:
    print("Task 1 start")


@task(upstream_tasks=["task_a", "task_c"])
def task_2() -> None:
    print("Task 2")


@task()
def task_3() -> None:
    print("Task 3")


server = Server(
    name="server_b",
    tasks=[task for task in globals().values() if isinstance(task, Task)],
    emitter=emitter,
)


def main() -> None:
    server.start()


if __name__ == "__main__":
    main()
