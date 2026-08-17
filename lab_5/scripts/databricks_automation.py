import os
import time

from databricks.sdk import WorkspaceClient


TERMINAL_STATES = {
    "TERMINATED",
    "SKIPPED",
    "INTERNAL_ERROR",
}


def main() -> None:
    client = WorkspaceClient()

    job_id = os.getenv("DATABRICKS_JOB_ID")

    if not job_id:
        raise RuntimeError("DATABRICKS_JOB_ID is not set")

    print(f"Triggering Databricks Job: {job_id}")

    run = client.jobs.run_now(job_id=int(job_id))

    if run.run_id is None:
        raise RuntimeError("Databricks did not return a run ID")

    run_id = run.run_id

    print(f"Job started. Run ID: {run_id}")

    while True:
        run = client.jobs.get_run(run_id)

      
        lifecycle_state = run.state.life_cycle_state.value
       
        result_state = run.state.result_state.value if run.state.result_state else None

        print(
            f"Run ID: {run_id} | "
            f"Lifecycle: {lifecycle_state} | "
            f"Result: {result_state}"
        )

        if lifecycle_state in TERMINAL_STATES:
            if result_state == "SUCCESS":
                print("Databricks Job completed successfully.")
                return

            raise RuntimeError(
                f"Databricks Job failed. "
                f"Run ID: {run_id} | "
                f"Result: {result_state}"
            )

        time.sleep(10)


if __name__ == "__main__":
    main()