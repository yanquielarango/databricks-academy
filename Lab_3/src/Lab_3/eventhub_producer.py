import json
import time
from datetime import datetime, timezone

import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData


def get_eventhub_connection_string():
    return dbutils.secrets.get(
        scope="azure-secrets",
        key="eventhub-connection-string"
    )


EVENTHUB_NAME = "orders-eventhub"

ORDERS_SOURCE = "/Volumes/dbr_dev/yanquiel/staging/data/order_details.csv"
MENU_SOURCE = "/Volumes/dbr_dev/yanquiel/staging/data/menu_items.csv"

EVENTS_PER_BATCH = 10
SECONDS_BETWEEN_BATCHES = 5
NUM_BATCHES = 5


def build_event(order_row, menu_lookup):
    item = menu_lookup.get(order_row["item_id"])
    return {
        "order_id": int(order_row["order_id"]),
        "item_id": int(order_row["item_id"]),
        "item_name": item["item_name"] if item else None,
        "price": float(item["price"]) if item else None,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    orders = pd.read_csv(ORDERS_SOURCE)
    menu = pd.read_csv(MENU_SOURCE).set_index("menu_item_id").to_dict(orient="index")

    client = EventHubProducerClient.from_connection_string(
        conn_str=get_eventhub_connection_string(),
        eventhub_name=EVENTHUB_NAME,
    )

    with client:
        for batch_num in range(1, NUM_BATCHES + 1):
            batch = client.create_batch()
            sample = orders.sample(EVENTS_PER_BATCH)

            for _, row in sample.iterrows():
                batch.add(EventData(json.dumps(build_event(row, menu))))

            client.send_batch(batch)
            print(f"Batch {batch_num}: {len(sample)} eventos enviados")
            time.sleep(SECONDS_BETWEEN_BATCHES)

    print("Producer terminado")


if __name__ == "__main__":
    main()