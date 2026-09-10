# Menu and Pricing

## Overview

The data platform ingests menu data from CSV files and processes it through the Bronze, Silver, and Gold layers.

The menu dataset contains information about menu items, categories, and prices.

The processing flow is:

CSV Files → Auto Loader → Bronze → Silver SCD Type 2 → Gold Dimension

## Menu Source

Menu files are read from:

`/Volumes/<catalog>/<bronze_schema>/landing/menu/`

In the development environment, the Bronze schema is:

`dbr_dev.yanquiel_bronze`

The menu source uses CSV files with headers.

## Source Schema

The expected menu fields are:

- `menu_item_id`
- `item_name`
- `category`
- `price`

The ingestion process also includes a `_corrupt_record` field used to capture malformed CSV rows.

The source schema uses the following data types:

- `menu_item_id` → INTEGER
- `item_name` → STRING
- `category` → STRING
- `price` → DOUBLE

## Bronze Ingestion

Menu data is ingested using Databricks Auto Loader with streaming file ingestion.

The Bronze table is:

`dbr_dev.yanquiel_bronze.menu_bronze`

Auto Loader reads CSV files from the landing volume and processes newly available files incrementally.

The ingestion configuration uses permissive parsing.

Malformed CSV content can be captured in:

`_corrupt_record`

## Bronze Metadata

The Bronze transformation adds metadata columns to each ingested menu record.

These include:

- `file_name`
- `ingest_datetime`

`file_name` identifies the source file from which the record was ingested.

`ingest_datetime` records when the data was processed by the pipeline.

## Bronze Table Properties

The Bronze menu table uses Delta Lake.

Change Data Feed is enabled on the table.

The table also uses automatic write optimization and compaction settings.

## Silver Validation

Before menu records are written to Silver, they pass through a cleaned streaming view named:

`menu_silver_clean`

The pipeline applies data quality expectations before SCD processing.

A menu record must satisfy:

- `price > 0`
- `menu_item_id IS NOT NULL`

Records that fail these expectations are dropped from the cleaned stream.

## Menu Transformation

The menu transformation selects the relevant business and metadata fields.

The transformed output contains:

- `menu_item_id`
- `item_name`
- `menu_category`
- `price`
- `file_name`
- `ingest_datetime`

The original source field:

`category`

is renamed to:

`menu_category`

## Silver SCD Type 2 Processing

Validated menu records are written to:

`dbr_dev.yanquiel_silver.menu_silver`

The Silver table uses Databricks Auto CDC with Slowly Changing Dimension Type 2 behavior.

The business key is:

`menu_item_id`

Changes are sequenced using:

`ingest_datetime`

This allows the platform to preserve historical versions of a menu item when its attributes change over time.

For example, if the price of a menu item changes, the previous version can remain available in Silver while a new version becomes the current one.

The SCD Type 2 table includes Databricks-managed tracking columns such as:

- `__START_AT`
- `__END_AT`

A record with a null `__END_AT` represents the currently active version.

## Pricing History

Because the Silver menu table uses SCD Type 2, changes to the `price` field can be preserved historically.

This means the platform can keep previous menu prices instead of overwriting them.

The same historical behavior applies when other tracked menu attributes change, such as:

- `item_name`
- `menu_category`
- `price`

## Gold Menu Dimension

The Gold menu dimension is:

`dbr_dev.yanquiel_gold.dim_menu_item`

This dimension contains only the latest active version of each menu item.

The Gold transformation filters Silver records using:

`__END_AT IS NULL`

This selects the current version of each menu item.

The Gold dimension contains:

- `menu_item_id`
- `item_name`
- `menu_category`
- `price`

## Current vs Historical Data

Silver preserves historical menu versions using SCD Type 2.

Gold exposes only the current active version of each menu item.

Therefore:

- Silver is used when historical changes are required.
- Gold is used when analytics need the latest menu information.

## Processing Behavior

New menu files can be added to the landing location over time.

Auto Loader discovers and processes new files incrementally.

Valid menu records are transformed and processed through Auto CDC.

If an existing `menu_item_id` arrives with updated attributes, a new historical version can be created in Silver.

The Gold menu dimension always keeps the currently active version available for downstream analytics.