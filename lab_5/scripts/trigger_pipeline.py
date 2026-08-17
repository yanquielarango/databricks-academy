import os
import time

from databricks.sdk import WorkspaceClient



TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELED",
}

FAILURE_STATES = {
    "FAILED",
    "CANCELED",
}


def main() -> None:
    client = WorkspaceClient()

    pipeline_id = os.getenv("DATABRICKS_PIPELINE_ID")
    full_refresh = os.getenv("PIPELINE_FULL_REFRESH", "false").lower() == "true"

    if not pipeline_id:
        raise RuntimeError("DATABRICKS_PIPELINE_ID is not set")

    print(f"Triggering Databricks Pipeline: {pipeline_id} (full_refresh={full_refresh})")

    update = client.pipelines.start_update(
        pipeline_id=pipeline_id,
        full_refresh=full_refresh,
    )

    if update.update_id is None:
        raise RuntimeError("Databricks did not return an update ID")

    update_id = update.update_id

    print(f"Pipeline update started. Update ID: {update_id}")

    while True:
        result = client.pipelines.get_update(
            pipeline_id=pipeline_id,
            update_id=update_id,
        )

      
        state = result.update.state.value

        print(f"Update ID: {update_id} | State: {state}")

        if state in TERMINAL_STATES:
            if state in FAILURE_STATES:
                raise RuntimeError(
                    f"Pipeline update failed. "
                    f"Update ID: {update_id} | State: {state}"
                )

            print("Pipeline update completed successfully.")
            return

        time.sleep(10)


if __name__ == "__main__":
    main()