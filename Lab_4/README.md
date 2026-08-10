# Lab 4 — Silver Layer, Data Quality & Schema Evolution

Goal for this lab: build a Silver layer that is reliable. It needs to be deduplicated, schema checked and able to handle changes in both the data and the schema without breaking.

I did not take screenshots while working. Instead this README shows the real code and the real output or errors I got for each task.

## Setup

Two environments in one bundle. `dev` runs on Databricks Free Edition using serverless. `prod` runs on Azure through SoftServe's workspace with a fixed cluster set by `existing_cluster_id`. Bronze has two tables. `brz_menu_items` comes from a batch CSV. `brz_order_details` comes from Confluent Kafka as raw JSON through streaming. Silver has `slv_menu_items` (SCD2) and `slv_order_details` (parsed from Bronze, also used to demo schema enforcement).

## Silver layer design

For menu I dedupe Bronze down to the latest `_ingestion_timestamp` per `menu_item_id` before comparing anything against Silver:

```python
df_source = (
    df_bronze_menu
    .groupBy("menu_item_id")
    .agg(F.max("_ingestion_timestamp").alias("_ingestion_timestamp"))
    .join(df_bronze_menu, on=["menu_item_id", "_ingestion_timestamp"])
    .select("menu_item_id", "item_name", "menu_category", "price", "_source_file", "_ingestion_timestamp")
)
```

The schema for menu has the usual SCD2 columns (`effective_date`, `end_date`, `is_current`) plus lineage columns (`_source_file`, `_ingestion_timestamp`). Orders skips SCD tracking since an order event does not get updated, it just happens once. It keeps `topic`, `partition` and `offset` as its lineage instead of `_source_file` since that is what actually identifies where a row came from in Kafka.

## MERGE pipeline and SCD Type 2

Menu prices change and I wanted to keep the history instead of overwriting it, so this is SCD2. Change detection compares whatever is new in Bronze against whatever is currently live in Silver:

```python
df_changes = (
    df_source.alias("src")
    .join(df_silver_current.alias("cur"), on="menu_item_id", how="left")
    .filter(
        F.col("cur.menu_item_id").isNull()
        | (F.col("src.item_name") != F.col("cur.item_name"))
        | (F.col("src.menu_category") != F.col("cur.menu_category"))
        | (F.col("src.price") != F.col("cur.price"))
    )
    .select("src.*")
)
```

The write itself has two steps. A MERGE closes the row that was current then a plain append opens the new one:

```python
silver_table.alias("tgt").merge(
    df_changes.alias("src"),
    "tgt.menu_item_id = src.menu_item_id AND tgt.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "end_date": F.current_timestamp()
}).execute()

df_new_versions = (
    df_changes
    .withColumn("effective_date", F.current_timestamp())
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)
df_new_versions.write.format("delta").mode("append").saveAsTable(SILVER_MENU_FULL_TABLE)
```

I tested it by changing the price on item 132 (Eggplant Parmesan) three times in a row. The history came out clean:

| price | is_current | end_date |
|---|---|---|
| 16.95 | False | 2026-08-07 10:37:58 |
| 25.00 | False | 2026-08-07 10:47:24 |
| 50.00 | True | none |

There is exactly one current row at any time and the old ones stay with a real `end_date` instead of getting overwritten.

## Orders: from a fixed schema to a flexible one

My first version of `silver_orders.ipynb` parsed Bronze's `raw_json` with a fixed `StructType` called `ORDER_EVENT_SCHEMA`. That works but it means every time the producer adds a field the notebook needs a schema update to pick it up. Not great for something meant to run unattended.

So I switched the production path to parse with a generic `MapType` instead:

```python
df_parsed = df_bronze_orders.withColumn(
    "event",
    F.from_json(F.col("raw_json"), MapType(StringType(), StringType()))
)

df_silver_orders = (
    df_parsed
    .select(
        F.col("event")["order_id"].cast("int").alias("order_id"),
        F.col("event")["item_id"].cast("int").alias("item_id"),
        F.col("event")["event_timestamp"].cast("timestamp").alias("event_timestamp"),
        F.col("event")["discount_code"].alias("discount_code"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
)
```

Fields I do not ask for simply do not show up. Fields I do ask for come back as `NULL` if they are missing from a given event instead of throwing an error. Adding a new field later is a one line change in the `.select()`, not a schema migration.

The write is idempotent now too. It uses a MERGE keyed on `topic`, `partition` and `offset` since that combination uniquely identifies a Kafka message:

```python
(
    silver_orders_table.alias("tgt")
    .merge(
        df_silver_orders.alias("src"),
        "tgt.topic = src.topic AND tgt.`partition` = src.`partition` AND tgt.`offset` = src.`offset`"
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

Confirmed with a duplicate check after running the notebook a few times:

```python
spark.table(SILVER_ORDER_FULL_TABLE).groupBy("topic", "partition", "offset").count().filter("count > 1").count()
# -> 0
```

## Schema enforcement and evolution

I kept this as a separate demo further down in the same notebook. This part uses a strict `StructType` on purpose instead of the flexible `MapType` above so I could show a real rejection and a real fix.

I ran a producer scenario (`generate_batch_kafka.py --scenario schema_change`) that adds a `discount_code` field the table did not have yet, then tried writing it against the old narrower schema (`ORDER_EVENT_SCHEMA`, no `discount_code`). The write got rejected as expected:

```
[DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table.
To enable schema migration using DataFrameWriter or DataStreamWriter, please set: '.option("mergeSchema", "true")'.

Table schema: order_id, item_id, event_timestamp, topic, partition, offset, _ingestion_timestamp
Data schema:  order_id, item_id, event_timestamp, discount_code, topic, partition, offset, _ingestion_timestamp

Table ACLs are enabled in this cluster, so automatic schema migration is not allowed.
Please use the ALTER TABLE command for changing the schema.
```

Same write with `mergeSchema=true` added goes through and widens the table on its own. Old rows get backfilled with `discount_code = NULL` and new ones carry the actual value. Both attempts live in the same section of the notebook. The failing one is wrapped in a try and except block so it does not block the rest of the notebook from running.

One thing worth mentioning honestly. Since the production `MapType` path already picks up `discount_code` on its own, running this enforcement demo after the production path has already processed the same Bronze rows will append those same 10 rows a second time. This section still uses a plain append on purpose to keep the demo simple. I am keeping it this way since the point here is to show the schema rejection and the fix, not to be part of the regular pipeline. Just do not put this section into a scheduled job.

## Column mapping

Enabled it on `slv_menu_items`:

```sql
ALTER TABLE slv_menu_items
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
)
```

Then renamed a column just to see it work:

```sql
ALTER TABLE slv_menu_items RENAME COLUMN category TO menu_category
```

No rewrite and it happened instantly. Querying the table right after confirmed the rename with all the history still there. `DROP COLUMN` follows the same idea. I left it commented out in the notebook instead of actually dropping something. No reason to lose real data just to prove a point.

## On data contracts

With `mergeSchema` on, the table just adapts to whatever shows up. That is convenient but it also means the schema is effectively whatever the producer decides to send that day. Nobody reviews it, it just lands. A data contract flips that around. The schema gets agreed on and versioned up front, usually enforced through something like Confluent's schema registry which is already available for this topic. The producer cannot publish something that breaks the contract.

|  | mergeSchema | data contract |
|---|---|---|
| speed | fast, no coordination needed | slower, needs both sides to agree |
| risk downstream | higher since changes just flow through | lower since changes get reviewed first |
| when you find out | after the fact | before it ever lands |

The `discount_code` situation above is a small example of exactly what a contract is meant to prevent. The `MapType` approach I used for the production path is a kind of middle ground. It does not enforce a contract but it also does not break when the shape changes, it just quietly tolerates it. A real contract would still be the better answer for an actual production system with real stakes.

## Re-runs and reliability

Where things stand now:

`bronze_menu.ipynb` is fine. It uses overwrite plus mergeSchema so rerunning it just re-reads the CSV.

`create_schema_and_tables.ipynb` is fine. Everything uses `IF NOT EXISTS`.

`silver_menu_items.ipynb` is fine. The MERGE only acts on real changes.

`bronze_ingestion_orders_kafka.ipynb` is fine under normal conditions. The streaming checkpoint tracks what has already been consumed.

`silver_orders.ipynb` is fine now for the production path. The MERGE on topic, partition and offset was confirmed to produce no duplicates after several reruns. The enforcement and evolution demo at the bottom is a known exception on purpose, explained above. Do not schedule that part.

## Pipeline jobs

I restructured the bronze only jobs into two task pipelines with explicit dependencies, so Silver cannot run against a half updated Bronze.

`menu_pipeline_job` runs `bronze_menu` then `silver_menu` using `depends_on`.

`orders_pipeline_job` runs `ingest_orders_kafka` then `silver_orders` using `depends_on`.

Both show up in the Databricks Jobs UI as a two step flow instead of two separate jobs I would have to remember to run in the right order myself. `setup_job` stays on its own since it is infrastructure, not a recurring data pipeline.

## Scheduling

`orders_pipeline_job` and `menu_pipeline_job` have a schedule defined in `databricks.yml` for `prod`. Orders runs every 15 minutes and menu runs once a day using the `Europe/Warsaw` timezone. Both are set to `pause_status: PAUSED`. The schedule is visible in the Jobs UI and ready to go but nothing runs automatically until it gets unpaused. I did not want it starting on its own before the pipeline gets tested in prod.

## Running the jobs

The first time this runs in any environment `setup_job` needs to run before anything else. It creates the schemas, the volume and the tables that the other jobs depend on.

Via CLI:
```bash
databricks bundle deploy -t dev --profile lab_4
databricks bundle run -t dev setup_job --profile lab_4
```

For `prod` the variable `existing_cluster_id` is required since there is no serverless there. It needs to be passed explicitly:
```bash
databricks bundle deploy -t prod --var="cluster_id=<your-cluster-id>" --profile lab_4_prod
databricks bundle run -t prod setup_job --var="cluster_id=<your-cluster-id>" --profile lab_4_prod
```

Once the tables exist the two pipeline jobs can run the same way:
```bash
databricks bundle run -t dev menu_pipeline_job --profile lab_4
databricks bundle run -t dev orders_pipeline_job --profile lab_4
```

Via the UI after deploying, go to Workflows, find the job by name (`Lab_4-setup`, `Lab_4-menu-pipeline`, `Lab_4-orders-pipeline`) and click **Run now**. Same result without touching the CLI, probably easier for anyone reviewing this who prefers clicking over typing commands.

To turn on the `prod` schedules once ready, edit the job in the UI under Schedule and toggle it on. Or set `pause_status` to `UNPAUSED` in `databricks.yml` and redeploy.

## OPTIMIZE, VACUUM, and Liquid Clustering vs ZORDER

OPTIMIZE on `brz_order_details` compacted ten small streaming files down to one:

```
numFilesRemoved: 10, numFilesAdded: 1
totalSize: 37,760 bytes to 15,152 bytes
```

VACUUM in dry run mode with the default 7 day retention on `slv_menu_items` came back empty. Nothing was outside the retention window yet, which just confirms the safety guard is doing its job.

Liquid Clustering, which is Databricks' current recommendation over ZORDER, is enabled on `slv_order_details`, clustered by `order_id` since that is the most likely filter and join column for this table:

```sql
ALTER TABLE slv_order_details CLUSTER BY (order_id)
```

Followed by OPTIMIZE, which compacted two files into one. The run metrics show `clusteringStrategy` populated so it was actually clustering aware, not just a generic file merge.

Quick comparison for the record. ZORDER still works but needs to be rerun manually and does not mix well with static partitioning. Liquid Clustering can change its key later without a full rewrite and does not need partitioning at all. That is why I went with it here.

This all lives in its own notebook, `maintenance_and_quality.ipynb`, run manually rather than as part of any job. Column mapping, VACUUM and constraint changes are things you do deliberately, not on a schedule.

## Data quality rules

Two CHECK constraints on `slv_menu_items`:

```sql
ALTER TABLE slv_menu_items ADD CONSTRAINT positive_price CHECK (price > 0)
ALTER TABLE slv_menu_items ADD CONSTRAINT valid_menu_item_id CHECK (menu_item_id IS NOT NULL)
```

I tried writing a row with `price = -5.0` to confirm it actually works:

```
Data quality constraint rejected the write as expected:
[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint positive_price (price > 0) violated by row with values:
 price : -5.0.
```

Worth noting these constraints only check one row at a time. They will not catch something like two rows both marked `is_current = true` for the same item. That kind of cross row rule is really enforced by the MERGE logic itself, not by a constraint.

## CI/CD

The bundle deploys to two targets. `dev` uses Free Edition, serverless and a PAT. `prod` uses Azure through SoftServe, a fixed cluster and a PAT. There are three GitHub Actions workflows sitting at the repo root since Actions will not pick them up otherwise in a monorepo with multiple labs.

`deploy-dev.yml` deploys to dev on push to `main`, scoped to `Lab_4/**`. Verified working several times.

`validate-pr.yml` runs `bundle validate` on pull requests into `main`. Verified working.

`deploy-prod.yml` deploys to prod on version tags. Not exercised yet.

Both targets use a Personal Access Token instead of a Service Principal. Not the ideal setup. Free Edition does not support the account level infrastructure Service Principals need and I do not have admin rights on the SoftServe workspace to set one up there either. In a real production environment this is where I would push for Service Principals with proper federation instead.

I also ran into two practical CI/CD issues along the way worth mentioning. The workflows only got picked up once `.github/workflows/` moved to the repo root, since GitHub Actions does not look inside subfolders in a monorepo. And `databricks bundle deploy` failed in CI until I removed the `profile` field from `databricks.yml`. With `profile` set, the CLI insists on reading `~/.databrickscfg`, which does not exist on a GitHub hosted runner, even with `DATABRICKS_HOST` and `DATABRICKS_TOKEN` set as environment variables.

## Still open

Waiting for approval to deploy this to prod. Schedules are set up but paused.