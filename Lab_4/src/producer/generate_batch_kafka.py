import argparse
from kafka_common import build_producer, load_orders, send_batch


def main(scenario):
    orders = load_orders()
    producer = build_producer()
    if scenario == "reload":
        sample = orders.sample(20)
        send_batch(producer, sample, id_offset=100000)
    elif scenario == "schema_change":
        sample = orders.sample(10)
        send_batch(producer, sample, id_offset=200000, discount_code="PROMO10")
    print(f"Escenario '{scenario}' enviado")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["reload", "schema_change"], required=True)
    args = parser.parse_args()
    main(args.scenario) 