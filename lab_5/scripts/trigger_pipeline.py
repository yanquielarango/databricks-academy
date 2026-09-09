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


def get_pipeline_id(
    client: WorkspaceClient,
    pipeline_name: str,
) -> str:
    matches = []

    for pipeline in client.pipelines.list_pipelines():
        if pipeline.name == pipeline_name:
            matches.append(pipeline)

    if not matches:
        raise RuntimeError(
            f"Pipeline '{pipeline_name}' was not found"
        )

    if len(matches) > 1:
        raise RuntimeError(
            f"More than one pipeline named "
            f"'{pipeline_name}' was found"
        )

    pipeline_id = matches[0].pipeline_id

    if not pipeline_id:
        raise RuntimeError(
            f"Pipeline '{pipeline_name}' has no pipeline ID"
        )

    return pipeline_id


def main() -> None:
    client = WorkspaceClient()

    pipeline_name = os.getenv(
        "DATABRICKS_PIPELINE_NAME",
        "lab_5_etl",
    )

    full_refresh = (
        os.getenv(
            "PIPELINE_FULL_REFRESH",
            "false",
        ).lower()
        == "true"
    )

    print(
        f"Looking for Databricks pipeline: "
        f"{pipeline_name}"
    )

    pipeline_id = get_pipeline_id(
        client=client,
        pipeline_name=pipeline_name,
    )

    print(
        f"Pipeline found. "
        f"Pipeline ID: {pipeline_id}"
    )

    print(
        f"Triggering pipeline "
        f"(full_refresh={full_refresh})..."
    )

    update = client.pipelines.start_update(
        pipeline_id=pipeline_id,
        full_refresh=full_refresh,
    )

    if update.update_id is None:
        raise RuntimeError(
            "Databricks did not return an update ID"
        )

    update_id = update.update_id

    print(
        f"Pipeline update started. "
        f"Update ID: {update_id}"
    )

    while True:
        result = client.pipelines.get_update(
            pipeline_id=pipeline_id,
            update_id=update_id,
        )

        if (
            not result.update
            or not result.update.state
        ):
            raise RuntimeError(
                "Databricks did not return "
                "the pipeline update state"
            )

        state = result.update.state.value

        print(
            f"Update ID: {update_id} | "
            f"State: {state}"
        )

        if state in TERMINAL_STATES:
            if state in FAILURE_STATES:
                raise RuntimeError(
                    f"Pipeline update failed. "
                    f"Update ID: {update_id} | "
                    f"State: {state}"
                )

            print(
                "Pipeline update completed successfully."
            )
            return

        time.sleep(10)


if __name__ == "__main__":
    main()