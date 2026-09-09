import os
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

    notebook_path = os.getenv("DATABRICKS_NOTEBOOK_PATH")

    if not notebook_path:
        raise RuntimeError(
            "DATABRICKS_NOTEBOOK_PATH is not set"
        )

    print("Getting Databricks Runtime version...")

    spark_version = client.clusters.select_spark_version(
        latest=True,
        long_term_support=True,
    )

    print(f"Spark version: {spark_version}")

    print("Selecting an available node type...")

    node_type_id = client.clusters.select_node_type(
        local_disk=True,
    )

    print(f"Node type: {node_type_id}")

    cluster_id = None

    try:
        print("\nCreating temporary Databricks cluster...")

        cluster = client.clusters.create_and_wait(
            cluster_name="lab-9-platform-automation",
            spark_version=spark_version,
            node_type_id=node_type_id,
            num_workers=1,
            autotermination_minutes=15,
        )

        cluster_id = cluster.cluster_id

        if not cluster_id:
            raise RuntimeError(
                "Databricks did not return a cluster ID"
            )

        print(
            f"Cluster created successfully. "
            f"Cluster ID: {cluster_id}"
        )

        print(
            f"\nSubmitting notebook: "
            f"{notebook_path}"
        )

        run_waiter = client.jobs.submit(
            run_name="lab-9-notebook-automation",
            tasks=[
                jobs.SubmitTask(
                    task_key="run_platform_notebook",
                    existing_cluster_id=cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=notebook_path,
                    ),
                )
            ],
        )

        run = run_waiter.result()

        if run.run_id is None:
            raise RuntimeError(
                "Databricks did not return a run ID"
            )

        run_id = run.run_id

        print(
            f"Notebook job submitted. "
            f"Run ID: {run_id}"
        )

        wait_for_run(
            client=client,
            run_id=run_id,
        )

    finally:
        if cluster_id:
            print(
                f"\nDeleting temporary cluster: "
                f"{cluster_id}"
            )

            try:
                client.clusters.permanent_delete(
                    cluster_id=cluster_id
                )

                print(
                    "Temporary cluster deleted."
                )

            except Exception as cleanup_error:
                print(
                    "WARNING: Failed to delete "
                    f"temporary cluster: {cleanup_error}"
                )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"\nPlatform automation failed: {exc}"
        )
        sys.exit(1)