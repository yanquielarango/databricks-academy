# src/Lab/generate_batches.py

import argparse
from pathlib import Path

import pandas as pd


SOURCE = "/Volumes/dbr_dev/yanquiel_dabs/staging/data/order_details.csv"

LANDING_PATH = Path(
    "/Volumes/dbr_dev/yanquiel_dabs/landing/orders"
)


def generate_and_upload_batch(
    batch_folder,
    sample_size,
    id_offset,
    discount_code=None
):

    df = pd.read_csv(SOURCE)

    sample = df.sample(
        sample_size
    ).copy()

    sample["order_details_id"] += id_offset


    if discount_code:
        sample["discount_code"] = discount_code


    destination = LANDING_PATH / batch_folder

    destination.mkdir(
        parents=True,
        exist_ok=True
    )


    output_file = (
        destination /
        "order_details.csv"
    )


    sample.to_csv(
        output_file,
        index=False
    )


    print(
        f"Generated: {output_file} "
        f"({sample.shape[0]} rows)"
    )



if __name__ == "__main__":

    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--scenario",
        choices=[
            "reload",
            "schema_change"
        ],
        required=True
    )


    args = parser.parse_args()


    if args.scenario == "reload":

        generate_and_upload_batch(
            batch_folder="batch_012",
            sample_size=20,
            id_offset=100000
        )


    elif args.scenario == "schema_change":

        generate_and_upload_batch(
            batch_folder="batch_012",
            sample_size=10,
            id_offset=200000,
            discount_code="PROMO10"
        )