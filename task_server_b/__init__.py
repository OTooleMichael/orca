from orca_tools.task_server_utils import Server, task, emitter


@task(upstream_tasks=["task_2", "task_b", "task_d"])
def task_1() -> None:
    print("Task 1 start")
    time.sleep(2)
    print("Task 1 end")


@task(upstream_tasks=["task_a", "task_c"])
def task_2() -> None:
    print("Task 2")


server = Server(
    name="server_b",
    tasks=[
        task_1,
        task_2,
    ],
    emitter=emitter,
)


def main() -> None:
    server.start()

if __name__ == "__main__":
    main()
