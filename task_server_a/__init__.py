import time

from orca_tools.task_server_utils import Server, task, emitter


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
    time.sleep(5)
    print("Task D done")


server = Server(
    name="server_a",
    tasks=[
        task_a,
        task_b,
        task_c,
        task_d,
    ],
    emitter=emitter,
)


def main() -> None:
    server.start()


if __name__ == "__main__":
    main()
