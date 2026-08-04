import time
from utils.kafka_common import build_producer, load_orders, send_batch

EVENTS_PER_BATCH = 100
NUM_BATCHES = 10
SECONDS_BETWEEN_BATCHES = 5


def main():
    orders = load_orders()
    producer = build_producer()
    for batch_num in range(1, NUM_BATCHES + 1):
        sample = orders.sample(EVENTS_PER_BATCH)
        send_batch(producer, sample)
        print(f"Batch {batch_num}: {len(sample)} eventos enviados")
        time.sleep(SECONDS_BETWEEN_BATCHES)
    print("Producer terminado")


if __name__ == "__main__":
    main()