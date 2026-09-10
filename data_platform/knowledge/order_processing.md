# Order Processing

## Overview

The data platform processes order events through a Medallion Architecture composed of Bronze, Silver, and Gold layers.

Order events are produced by a Python application and ingested into Databricks using Zerobus.

The processing flow is:

Local Python Producer → Zerobus → Bronze → Silver → Gold

## Order Ingestion

Order events are sent by the local Python producer through Databricks Zerobus.

Zerobus writes the events directly into:

`dbr_dev.yanquiel_bronze.orders_bronze`

The Bronze table stores the incoming order events before transformation and data quality processing.

Each order event can contain:

- `order_details_id`
- `order_id`
- `order_date`
- `order_time`
- `item_id`
- `event_timestamp`
- `discount_code`

The Bronze layer preserves the ingested events, including duplicate events.

## Order Parsing

The Lakeflow pipeline reads the Bronze order table as a streaming source.

The first transformation parses and standardizes the incoming fields.

The following conversions are performed:

- `order_details_id` → LONG
- `order_id` → LONG
- `order_date` → DATE
- `item_id` → LONG
- `event_timestamp` → TIMESTAMP

An `ingest_datetime` column is also added using the current processing timestamp.

## Order Validation

Before an order reaches the Silver table, the pipeline validates the required fields.

A valid order must have:

- a non-null `order_details_id`
- a non-null `order_id`
- a non-null `item_id`
- a valid, non-null `event_timestamp`

Orders that fail these basic validation rules are sent to:

`dbr_dev.yanquiel_silver.orders_quarantine`

The quarantine table contains a `dq_reason` column describing why the record was rejected.

Possible reasons include:

- `missing_order_details_id`
- `missing_order_id`
- `missing_item_id`
- `missing_or_invalid_event_timestamp`

## Duplicate Handling

Valid order events are deduplicated before they are stored in the Silver orders table.

The pipeline uses:

`order_details_id`

as the deduplication key.

A one-day event-time watermark is applied using `event_timestamp`.

Duplicate events can remain in the Bronze layer because Bronze represents the ingested source events.

The Silver layer contains the cleaned and deduplicated representation of those orders.

## Silver Orders

Validated and deduplicated orders are stored in:

`dbr_dev.yanquiel_silver.orders_silver`

This table is the trusted order dataset used by downstream analytics processing.

## Gold Fact Orders

Silver orders are transformed into the Gold fact table:

`dbr_dev.yanquiel_gold.fact_orders`

The Gold transformation derives a `date_key` from `order_date` and assigns a quantity of `1` to each order line.

Orders are enriched using the Gold menu and date dimensions.

The resulting fact table contains fields including:

- `order_id`
- `item_id`
- `date_key`
- `order_date`
- `order_time`
- `event_timestamp`
- `discount_code`
- `price`
- `quantity`

## Data Quality

Additional data quality checks are executed using Databricks Labs DQX.

Order checks include:

- required order identifiers must not be null
- `order_details_id` must be unique
- `event_timestamp` must not be in the future
- `ingest_datetime` must not be earlier than `event_timestamp`
- menu references must be valid

The CI/CD process fails when critical data quality checks fail.

## Reconciliation

After the pipeline completes, reconciliation checks compare the Silver orders with the Gold fact table.

The platform verifies:

1. The number of Silver orders matches the number of Gold fact records.
2. The sum of `quantity` in Gold matches the number of Silver order rows.

If either comparison fails, reconciliation fails and the CI/CD workflow stops.

## Processing Behavior

The number of order events processed by the platform can vary between producer executions.

Bronze stores the incoming events received through Zerobus, including duplicate events.

Silver contains the valid and deduplicated representation of those events. Therefore, the number of records in Silver can be lower than the number of events stored in Bronze.

Orders that fail the required-field validation rules are routed to the quarantine table instead of the Silver orders table.

Gold is built from the cleaned Silver dataset. As part of the reconciliation process, the number of Gold fact records is expected to match the number of valid and deduplicated Silver orders.