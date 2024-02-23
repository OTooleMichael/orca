from orca_tools.task_server_utils import Server, task, emitter


@task(upstream_tasks=["task_a", "task_c", "task_d"])
def task_a() -> None:
    print("Task A")


@task(upstream_tasks=["task_a"])
def task_b() -> None:
    print("Task B")


@task()
def task_c() -> None:
    print("Task C")


@task()
def task_d(upstream_tasks=["task_a", "task_b", "task_c"]) -> None:
    print("Task D")


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
