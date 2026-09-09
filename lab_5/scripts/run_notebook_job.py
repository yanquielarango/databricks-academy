import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


POLL_INTERVAL_SECONDS = 10


def wait_for_run(
    client: WorkspaceClient,
    run_id: int,
) -> None:
    while True:
        run = client.jobs.get_run(run_id=run_id)

        lifecycle_state = (
            run.state.life_cycle_state.value
            if run.state and run.state.life_cycle_state
            else None
        )

        result_state = (
            run.state.result_state.value
            if run.state and run.state.result_state
            else None
        )

        print(
            f"Run ID: {run_id} | "
            f"Lifecycle: {lifecycle_state} | "
            f"Result: {result_state}"
        )

        if lifecycle_state == "TERMINATED":
            if result_state == "SUCCESS":
                print("Notebook job completed successfully.")
                return

            raise RuntimeError(
                f"Notebook job failed. "
                f"Run ID: {run_id} | "
                f"Result: {result_state}"
            )

        if lifecycle_state in {
            "SKIPPED",
            "INTERNAL_ERROR",
        }:
            raise RuntimeError(
                f"Notebook job failed. "
                f"Run ID: {run_id} | "
                f"Lifecycle: {lifecycle_state}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    client = WorkspaceClient()

    current_user = client.current_user.me().user_name

    if not current_user:
        raise RuntimeError(
            "Databricks did not return the current user"
        )

    notebook_path = (
        f"/Workspace/Users/{current_user}"
        f"/.bundle/lab_5/dev/files/notebooks/platform_check.py"
    )

    print(f"Current Databricks identity: {current_user}")
    print("Submitting notebook with Databricks job compute...")
    print(f"Notebook: {notebook_path}")

    run_waiter = client.jobs.submit(
        run_name="lab-9-notebook-automation",
        tasks=[
            jobs.SubmitTask(
                task_key="run_platform_notebook",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                ),
            )
        ],
    )

    run_id = run_waiter.run_id

    if run_id is None:
        raise RuntimeError(
            "Databricks did not return a run ID"
        )

    print(
        f"Notebook job submitted successfully. "
        f"Run ID: {run_id}"
    )

    wait_for_run(
        client=client,
        run_id=run_id,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"\nPlatform automation failed: {exc}"
        )
        sys.exit(1)