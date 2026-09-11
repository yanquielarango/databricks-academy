import argparse

from zerobus_common import (
    build_stream,
    load_orders,
    send_batch,
)


def main(scenario):
    orders = load_orders()
    stream = build_stream()

    try:
        if scenario == "reload":
            batch = orders.iloc[:20]

            send_batch(
                stream,
                batch,
                id_offset=100000,
            )

        elif scenario == "schema_change":
            batch = orders.iloc[20:30]

            send_batch(
                stream,
                batch,
                id_offset=200000,
                discount_code="PROMO10",
            )

        print(
            f"Scenario '{scenario}' sent"
        )

    finally:
        stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--scenario",
        choices=[
            "reload",
            "schema_change",
        ],
        required=True,
    )

    args = parser.parse_args()

    main(args.scenario)