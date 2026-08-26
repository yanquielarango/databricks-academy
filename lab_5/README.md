# Data Engineering Pipeline with Lakeflow Declarative Pipelines

This project contains a Bronze → Silver → Gold data pipeline built with Azure Databricks and Unity Catalog.

The pipeline uses Lakeflow Declarative Pipelines for data processing and Databricks Asset Bundles for deployment. GitHub Actions is used for CI/CD.

The data comes from two sources:

- Order events from Confluent Cloud using Kafka
- Menu data from CSV files

The project was developed across Labs 5, 6 and 7. Each lab extends the same pipeline instead of creating a separate project.

- **Lab 5** implements the Bronze and Silver layers with streaming ingestion and SCD Type 2 processing
- **Lab 6** adds the Gold layer, AI/BI dashboard, alerting, governance and CI/CD
- **Lab 7** adds unit testing and data quality testing with pytest, Databricks Connect, DQX, quarantine handling and reconciliation checks

> Labs 5, 6 and 7 are kept inside the same `lab_5/` project. Each lab builds on the previous one. Keeping everything in the same project avoids duplicating pipeline logic and keeps Bronze, Silver and Gold as one continuous data pipeline.

---

## Architecture

```text
Kafka (Confluent)
       |
       v
orders_bronze
       |
       v
orders_parsed
     /     \
    v       v
orders_silver   orders_quarantine
    |
    v
fact_orders
    |
    v
AI/BI Dashboard


CSV Menu
   |
   v
menu_bronze
   |
   v
menu_silver_clean
   |
   v
menu_silver (SCD2)
   |
   v
dim_menu_item
   |
   +----------> fact_orders

dim_date ------> fact_orders
```

Bronze and Silver are processed incrementally.

Menu data is ingested with Auto Loader. Order events are read from Kafka using Structured Streaming.

---

## Repository structure

```text
lab_5/
├── databricks.yml
├── pyproject.toml
├── uv.lock
│
├── dq/
│   └── silver/
│       ├── menu.yml
│       └── orders.yml
│
├── queries/
│
├── resources/
│
├── scripts/
│   ├── databricks_automation.py
│   ├── run_dqx.py
│   ├── run_reconciliation.py
│   └── trigger_pipeline.py
│
├── src/
│   ├── dashboard/
│   ├── producer/
│   │
│   ├── transformation_functions/
│   │   ├── fact_orders.py
│   │   ├── menu.py
│   │   └── orders.py
│   │
│   └── transformations/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
└── tests/
    ├── conftest.py
    └── unit/
        ├── test_fact_orders.py
        ├── test_menu.py
        └── test_orders.py
```

---

## Tech stack

| Area | Technology |
|---|---|
| Data platform | Azure Databricks |
| Pipelines | Lakeflow Declarative Pipelines |
| Storage and governance | Delta Lake and Unity Catalog |
| Streaming | Confluent Cloud Kafka |
| File ingestion | Auto Loader |
| Deployment | Databricks Asset Bundles |
| CI/CD | GitHub Actions |
| Unit testing | pytest |
| Spark testing | Databricks Connect |
| Data quality | Databricks Labs DQX |
| Python dependencies | uv |
| Dashboard | Databricks AI/BI |

---

# Lab 5

Lab 5 contains the main data ingestion and transformation pipeline. It implements the Bronze and Silver layers.

## Bronze layer

The Bronze layer keeps the source data with only the transformations needed for ingestion.

### Orders

Orders are read from the `orders_event` Kafka topic using Structured Streaming.

The Bronze table stores the raw JSON together with Kafka metadata such as:

- `topic`
- `partition`
- `offset`
- timestamp
- ingestion timestamp

The combination of `topic`, `partition` and `offset` can be used as the technical identity of a Kafka event.

### Menu

Menu data is loaded from CSV files using Auto Loader.

The schema also contains `_corrupt_record` so malformed CSV rows can be captured instead of silently ignored.

---

## Silver layer

The Silver layer parses and cleans the source data before it is used by Gold.

### Orders

Order events are parsed from JSON and converted to the expected data types.

The Silver table contains fields such as:

```text
order_id
item_id
event_timestamp
discount_code
topic
partition
offset
ingest_datetime
```

Lakeflow expectations are used to validate required fields.

### Menu

Menu data is cleaned before being stored in Silver.

The Silver menu table is maintained as SCD Type 2 using Lakeflow Auto CDC.

```python
dp.create_auto_cdc_flow(
    stored_as_scd_type=2
)
```

Lakeflow maintains the SCD2 columns:

```text
__START_AT
__END_AT
```

This keeps the history when menu information changes.

---

# Lab 6

Lab 6 extends the pipeline with the Gold layer and the components needed for analytics, governance and deployment.

## Gold layer

The Gold layer contains the star schema used for analytics.

| Table | Type | Description |
|---|---|---|
| `dim_date` | Dimension | Calendar dimension |
| `dim_menu_item` | Dimension | Current menu item snapshot |
| `fact_orders` | Fact | Orders enriched with menu and date information |

`fact_orders` joins Silver orders with the menu and date dimensions.

A `date_key` is derived from `event_timestamp` and each fact record gets a `quantity` value of `1`.

---

## AI/BI dashboard

The Databricks AI/BI dashboard uses `fact_orders` as its main source.

It contains:

- order count
- revenue
- units sold
- average order value
- revenue trend
- category breakdown
- menu item breakdown
- filters for date, category and menu item

The dashboard uses individual data permissions so Unity Catalog security rules are evaluated for each user.

---

## Alerting

A SQL alert checks order activity in `orders_silver`.

It checks a rolling 24 hour window and sends an email notification if no orders are found.

This helps detect ingestion problems before they affect the Gold layer.

---

## Governance

Row level and column level security are applied to `fact_orders`.

A row filter controls which `item_id` values a user can see.

A column mask protects `discount_code`.

The policies are created and applied by the `gold_governance_job`.

Because `fact_orders` is a materialized view the security rules are applied using `ALTER MATERIALIZED VIEW`.

---

## CI/CD

GitHub Actions is used for validation and deployment to Dev and Prod.

Deployment and pipeline execution are kept separate.

A push to the Lab 6 feature branch performs:

```text
Unit tests
    |
    v
Bundle validation
    |
    v
Deploy to Dev
    |
    v
STOP
```

The pipeline is not executed automatically after deployment.

The main jobs are:

| Job | Trigger | Behavior |
|---|---|---|
| `unit-tests` | PR or push | Runs the Lab 7 pytest tests |
| `validate` | PR or push | Validates the Databricks Asset Bundle |
| `deploy-dev` | Push to `feature/lab-6-gold` | Deploys the bundle to Dev |
| `deploy-prod` | Tag `lab6-v*` | Validates and deploys the bundle to Prod |
| `run-dev-automation` | Manual | Runs the Dev pipeline, DQX, reconciliation and governance |
| `run-prod-automation` | Manual | Runs the Prod pipeline and governance |

Production deployment is triggered with a Git tag.

```bash
git tag lab6-vX.Y.Z
git push origin lab6-vX.Y.Z
```

This deploys the bundle to Prod but does not execute the production pipeline.

Pipeline execution is started manually from GitHub Actions.

---

# Lab 7

Lab 7 adds testing and data quality controls to the existing pipeline.

The goal is to test both the transformation code and the data moving through the medallion architecture.

Unit tests check the transformation logic.

Lakeflow expectations handle invalid records during pipeline processing.

DQX checks the data stored in Databricks.

Reconciliation checks compare data between Silver and Gold.

---

## Transformation functions

Transformation logic was moved from the Lakeflow files into importable Python functions.

They are stored in:

```text
src/transformation_functions/
├── fact_orders.py
├── menu.py
└── orders.py
```

The Lakeflow pipeline still defines the tables and views but calls these functions for the transformation logic.

For example the orders pipeline calls:

```python
transform_orders(df)
```

This makes the transformation logic easier to test without testing the Lakeflow decorators themselves.

---

## Unit tests

Unit tests are written with pytest.

Databricks Connect is used so tests can be started from the local IDE while Spark operations run against Databricks compute.

The tests are located in:

```text
tests/
├── conftest.py
└── unit/
    ├── test_fact_orders.py
    ├── test_menu.py
    └── test_orders.py
```

The current tests cover the main transformation logic for orders, menu and `fact_orders`.

### Orders

The tests check:

- JSON parsing
- integer casting
- timestamp parsing
- Kafka metadata
- output schema

### Menu

The tests check:

- column selection
- renaming `category` to `menu_category`
- expected output values

### Fact orders

The tests check:

- `date_key` creation
- `quantity` creation
- joins with the dimensions
- left join behavior

The unit tests can be executed with:

```bash
uv run pytest -m unit_test -v
```

Current result:

```text
4 passed
```

The tests are also integrated into GitHub Actions and run before bundle validation and deployment.

---

## Data quality with DQX

Databricks Labs DQX is used to check the data stored in Databricks.

The DQX rules are defined in YAML files.

```text
dq/
└── silver/
    ├── menu.yml
    └── orders.yml
```

The checks cover the main data quality dimensions used in the lab.

| Dimension | Example |
|---|---|
| Completeness | IDs and timestamps must not be null |
| Uniqueness | Kafka events must be unique |
| Validity | Values must follow the expected rules |
| Consistency | Order items must exist in the menu dimension |
| Timeliness | Event and ingestion timestamps must be consistent |

For orders the Kafka metadata is used for event uniqueness:

```text
topic + partition + offset
```

`order_id + item_id` is not used because the same item can legitimately appear more than once in the order event data.

For the SCD2 menu data the historical versions are checked using:

```text
menu_item_id + __START_AT
```

There is also a check to make sure each menu item has no more than one active version where:

```text
__END_AT IS NULL
```

DQX can be executed with:

```bash
uv run python scripts/run_dqx.py
```

A successful execution looks like:

```text
Running DQX checks on: dbr_dev.yanquiel_silver.orders_silver

Total rows:   1050
Valid rows:   1050
Invalid rows: 0
Missing menu references: 0

Running DQX checks on: dbr_dev.yanquiel_silver.menu_silver

Total rows:   32
Valid rows:   32
Invalid rows: 0
Menu items with multiple active versions: 0

DQX PASSED: all data quality checks passed.
```

If a critical data quality check fails the script returns a non zero exit code.

This allows DQX to be used as a quality gate when the pipeline is executed through GitHub Actions.

---

## Quarantine

Lab 7 also adds a quarantine path for invalid order records.

The orders flow is:

```text
orders_bronze
      |
      v
orders_parsed
    /       \
   v         v
orders_silver   orders_quarantine
```

Lakeflow expectations validate the required fields:

```text
order_id IS NOT NULL
item_id IS NOT NULL
event_timestamp IS NOT NULL
```

Valid records continue to `orders_silver`.

Invalid records are written to:

```text
orders_quarantine
```

The quarantine table includes a `dq_reason` column that explains why the record was rejected.

The quarantine flow was tested with an invalid Kafka event:

```json
{
  "order_id": null,
  "item_id": 105,
  "event_timestamp": "2026-08-26T20:05:00Z"
}
```

The record was stored in quarantine with:

```text
order_id: NULL
item_id: 105
dq_reason: missing_order_id
```

A check against `orders_silver` confirmed that no record with a null `order_id` was written there.

Lakeflow handles the routing of invalid records. DQX is kept separate and checks the resulting data.

---

## Reconciliation tests

Lab 7 also includes reconciliation checks between Silver and Gold.

The current reconciliation compares:

```text
orders_silver
```

with:

```text
fact_orders
```

The first check compares the number of rows.

```text
Silver orders count = Gold fact_orders count
```

The second check compares the Silver row count with:

```text
SUM(fact_orders.quantity)
```

Each fact record has:

```text
quantity = 1
```

so the values should match.

The reconciliation checks can be executed with:

```bash
uv run python scripts/run_reconciliation.py
```

Example result:

```text
Running reconciliation checks...

Silver: dbr_dev.yanquiel_silver.orders_silver
Gold:   dbr_dev.yanquiel_gold.fact_orders

Row-count reconciliation
Silver orders: 1050
Gold orders:   1050

Aggregate reconciliation
Silver rows:        1050
Gold SUM(quantity): 1050

RECONCILIATION PASSED: Silver and Gold are consistent.
```

The script returns a non zero exit code if reconciliation fails.

---

## Lab 7 in CI/CD

The Lab 7 tests are integrated into the existing GitHub Actions workflow.

Unit tests run before the Databricks Asset Bundle is validated and deployed.

```text
Push
  |
  v
pytest
  |
  v
Bundle validation
  |
  v
Deploy to Dev
```

The Lakeflow pipeline itself is not started automatically.

For Dev the pipeline and data quality checks can be started manually from GitHub Actions.

The manual flow is:

```text
Lakeflow Pipeline
       |
       v
      DQX
       |
       v
Reconciliation
       |
       v
Governance
```

The pipeline trigger waits until the Lakeflow update finishes before DQX starts.

If the pipeline fails the workflow stops.

If DQX finds a critical data quality problem the workflow stops.

If reconciliation fails the workflow also stops.

This keeps deployment separate from data processing while still allowing the Lab 7 checks to work as gates when the pipeline is executed.

---

## Testing and data quality

The project uses different checks for different problems.

| Tool | Purpose |
|---|---|
| pytest | Tests transformation logic |
| Databricks Connect | Runs Spark tests against Databricks compute |
| Lakeflow expectations | Validate records during pipeline processing |
| Quarantine table | Keeps rejected records for inspection |
| DQX | Checks the quality of stored data |
| Reconciliation | Checks data between Silver and Gold |

This gives the project checks at both code and data level.

---

