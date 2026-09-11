import os
from pathlib import Path

import pandas as pd
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

ORDERS_SOURCE = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "order_details.csv"
)

TABLE_NAME = "dbr_dev.yanquiel_bronze.orders_bronze"


def build_stream():
    server_endpoint = os.environ["ZEROBUS_ENDPOINT"]
    workspace_url = os.environ["DATABRICKS_HOST"]
    client_id = os.environ["ZEROBUS_CLIENT_ID"]
    client_secret = os.environ["ZEROBUS_CLIENT_SECRET"]

    sdk = ZerobusSdk(
        server_endpoint,
        workspace_url,
    )

    table_properties = TableProperties(
        TABLE_NAME
    )

    return sdk.create_stream(
        client_id,
        client_secret,
        table_properties,
    )


def load_orders():
    return (
        pd.read_csv(ORDERS_SOURCE)
        .dropna(subset=["item_id"])
    )


def build_event(
    order_row,
    id_offset=0,
    discount_code=None,
):
    event = {
        "order_details_id": (
            int(order_row["order_details_id"])
            + id_offset
        ),
        "order_id": (
            int(order_row["order_id"])
            + id_offset
        ),
        "order_date": str(
            order_row["order_date"]
        ),
        "order_time": str(
            order_row["order_time"]
        ),
        "item_id": int(
            order_row["item_id"]
        ),
        "event_timestamp": (
            pd.Timestamp.utcnow().isoformat()
        ),
    }

    if discount_code is not None:
        event["discount_code"] = discount_code

    return event


def send_batch(
    stream,
    orders_batch,
    id_offset=0,
    discount_code=None,
):
    last_offset = None

    for _, row in orders_batch.iterrows():
        event = build_event(
            row,
            id_offset=id_offset,
            discount_code=discount_code,
        )

        last_offset = stream.ingest_record_offset(
            event
        )

    if last_offset is not None:
        stream.wait_for_offset(last_offset)