import time

from zerobus_common import (
    build_stream,
    load_orders,
    send_batch,
)


EVENTS_PER_BATCH = 100
NUM_BATCHES = 10
SECONDS_BETWEEN_BATCHES = 5


def main():
    orders = load_orders()
    stream = build_stream()

    try:
        for batch_num in range(NUM_BATCHES):
            start = batch_num * EVENTS_PER_BATCH
            end = start + EVENTS_PER_BATCH

            batch = orders.iloc[start:end]

            if batch.empty:
                print("No hay más eventos para enviar.")
                break

            send_batch(
                stream,
                batch,
            )

            print(
                f"Batch {batch_num + 1}: "
                f"{len(batch)} eventos enviados"
            )

            time.sleep(SECONDS_BETWEEN_BATCHES)

    finally:
        stream.close()

    print("Producer terminado")


if __name__ == "__main__":
    main()