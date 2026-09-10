# Data Platform Architecture

## Overview

The project implements a data platform on Databricks using a Medallion Architecture.

The platform combines batch file ingestion and streaming event ingestion and processes data through Bronze, Silver, and Gold layers.

The main architecture is:

Batch CSV Files → Auto Loader ─┐
                               ├→ Bronze → Silver → Gold
Order Producer → Zerobus ──────┘

The platform also includes automated data quality checks, reconciliation, governance, and CI/CD.

## Development Environment

The development environment uses the following Unity Catalog catalog:

`dbr_dev`

The main schemas are:

- `dbr_dev.yanquiel_bronze`
- `dbr_dev.yanquiel_silver`
- `dbr_dev.yanquiel_gold`

Each schema represents a different stage of data processing.

## Bronze Layer

The Bronze layer contains data as it enters the platform.

Its purpose is to preserve source data before downstream transformations.

The project currently uses two ingestion patterns.

### Streaming Order Ingestion

Order events are produced by a local Python application and sent to Databricks using Zerobus.

Zerobus writes directly to:

`dbr_dev.yanquiel_bronze.orders_bronze`

The Bronze order table can contain duplicate events because it represents the incoming event stream.

### Batch File Ingestion

Menu data is delivered as CSV files to a Databricks Volume.

Databricks Auto Loader incrementally discovers and processes the files.

The resulting Bronze table is:

`dbr_dev.yanquiel_bronze.menu_bronze`

Ingestion metadata such as source file path and ingestion timestamp is also captured.

## Silver Layer

The Silver layer contains cleaned and validated data.

Its purpose is to provide trusted datasets for downstream processing.

### Orders

Orders are parsed, validated, and deduplicated before being stored in:

`dbr_dev.yanquiel_silver.orders_silver`

Duplicate order events are removed using `order_details_id`.

Records with missing required order fields are routed to:

`dbr_dev.yanquiel_silver.orders_quarantine`

### Menu

Menu records are validated before Silver processing.

The Silver menu table is:

`dbr_dev.yanquiel_silver.menu_silver`

It uses Auto CDC with Slowly Changing Dimension Type 2 behavior.

This allows historical versions of menu items to be preserved when their attributes change.

## Gold Layer

The Gold layer provides business-ready datasets for analytics and downstream consumption.

### Fact Orders

The order fact table is:

`dbr_dev.yanquiel_gold.fact_orders`

It is built from cleaned Silver orders and enriched using Gold dimensions.

Each order line receives a quantity of `1`.

### Menu Dimension

The current menu dimension is:

`dbr_dev.yanquiel_gold.dim_menu_item`

Silver preserves historical menu versions, while this Gold dimension exposes only the currently active version of each menu item.

### Date Dimension

The platform also contains:

`dbr_dev.yanquiel_gold.dim_date`

The date dimension is used to enrich order data through the `date_key`.

## Lakeflow Declarative Pipelines

Data transformations are orchestrated using Databricks Lakeflow Declarative Pipelines.

Pipeline definitions describe the relationships between Bronze, Silver, and Gold datasets.

The pipeline uses streaming processing where appropriate and materialized views for Gold analytical datasets.

The main pipeline is:

`data_platform_etl`

## Data Quality

The platform applies data quality controls at multiple stages.

Lakeflow expectations are used during pipeline processing to reject invalid records.

Additional post-pipeline data quality checks are executed using Databricks Labs DQX.

Examples of DQX checks include:

- required identifiers must not be null
- order detail identifiers must be unique
- timestamps must be valid
- ingestion timestamps must be consistent with event timestamps
- menu prices must be valid
- menu references used by orders must exist

Critical DQX failures cause the automated workflow to fail.

## Reconciliation

After data processing and quality validation, reconciliation checks verify consistency between Silver and Gold.

For orders, the platform checks that:

1. The Silver order count matches the Gold fact order count.
2. The sum of `quantity` in Gold matches the number of Silver order records.

A reconciliation failure causes the automated workflow to fail.

## Governance

The Gold layer includes access-control logic for analytical data.

A row filter is applied to the Gold order fact dataset.

The filter uses the Databricks session identity to determine which records a caller is allowed to access.

The CI service principal is authorized to access the complete dataset so that automated reconciliation can validate Silver and Gold consistently.

## Databricks Asset Bundles

The project infrastructure and Databricks resources are managed using Databricks Asset Bundles.

The bundle is named:

`data_platform`

Bundle configuration defines resources such as:

- Lakeflow pipelines
- schemas
- volumes
- jobs
- permissions
- environment-specific configuration

This allows the Databricks platform configuration to be stored and versioned together with the project code.

## CI/CD

GitHub Actions is used for CI/CD in the development environment.

Authentication between GitHub Actions and Azure Databricks uses a service principal and Azure OIDC authentication.

The DEV workflow performs automated validation and deployment.

The main workflow stages are:

Unit Tests → Bundle Validation → DEV Deployment → Post-deployment Automation

Post-deployment automation includes:

- checking whether the Bronze streaming table requires initial setup
- triggering the Lakeflow pipeline
- monitoring pipeline execution
- running DQX checks
- running Silver-to-Gold reconciliation
- executing platform automation

A failure in an important validation stage causes the workflow to fail.

## Streaming Resource Setup

The Zerobus Bronze order table must exist before the local producer can send events.

The project contains a setup job responsible for preparing the required streaming resources and permissions.

The CI/CD workflow checks whether the Bronze order table already exists.

The setup process is executed only when the required table does not exist.

This makes the setup operation safe for repeated deployments.

## Testing

Transformation logic is covered by unit tests.

The tests use Databricks Connect to execute Spark transformations against Databricks serverless compute.

Tests cover important transformation behavior for:

- orders
- menu data
- Gold fact orders

Unit tests run before bundle validation and deployment.

A unit test failure prevents the DEV deployment from continuing.

## Platform Reliability

The platform uses several controls to improve reliability:

- Bronze preserves source events.
- Silver validates and cleans data.
- duplicate order events are removed before analytics.
- invalid required-field records can be quarantined.
- menu history is preserved using SCD Type 2.
- DQX performs additional data quality validation.
- reconciliation verifies consistency between Silver and Gold.
- CI/CD prevents deployment workflows from silently succeeding when critical checks fail.

Together, these controls provide traceability from ingestion through business-ready analytical data.