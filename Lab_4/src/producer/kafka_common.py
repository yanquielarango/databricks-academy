import base64
import json
from pathlib import Path
import pandas as pd
from databricks.sdk import WorkspaceClient
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
TOPIC_NAME = "orders_event"


ORDERS_SOURCE = (Path(__file__).resolve().parent.parent.parent/ "data"/ "order_details.csv")


def get_confluent_credentials():
    w = WorkspaceClient(profile="lab_4")

    api_key_b64 = w.secrets.get_secret(scope="confluent-scope", key="api-key").value
    api_secret_b64 = w.secrets.get_secret(scope="confluent-scope", key="api-secret").value

    api_key = base64.b64decode(api_key_b64).decode("utf-8")
    api_secret = base64.b64decode(api_secret_b64).decode("utf-8")

    return api_key, api_secret


def build_producer():
    api_key, api_secret = get_confluent_credentials()
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    })


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Error: {err}")


def load_orders():
    return pd.read_csv(ORDERS_SOURCE).dropna(subset=["item_id"])


def build_event(order_row, id_offset=0, discount_code=None):
    event = {
        "order_id": int(order_row["order_id"]) + id_offset,
        "item_id": int(order_row["item_id"]),
        "event_timestamp": pd.Timestamp.utcnow().isoformat(),
    }
    if discount_code:
        event["discount_code"] = discount_code
    return event


def send_batch(producer, orders_sample, id_offset=0, discount_code=None):
    for _, row in orders_sample.iterrows():
        event = build_event(row, id_offset, discount_code)
        producer.produce(
            TOPIC_NAME,
            key=str(event["order_id"]).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
    producer.flush()