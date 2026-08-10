# Lab 4 — Silver Layer, Data Quality & Schema Evolution

Goal for this lab: get a Silver layer that's actually reliable  deduplicated, schema checked and able to handle changes to both the data and the schema without falling over.

I didn't take screenshots as I went so this README walks through each task with the actual code and the actual output/errors I got which should cover the same ground.

## Setup

Two environments, one bundle: `dev` runs on Databricks Free Edition (serverless), `prod` runs on Azure through SoftServe's workspace (fixed cluster set via `existing_cluster_id`). Bronze has two tables  `brz_menu_items` from a batch CSV `brz_order_details` streamed in from Confluent Kafka as raw JSON. Silver has `slv_menu_items` (SCD2) and `slv_order_details` (parsed from Bronze, used to demo schema enforcement).

## Silver layer design

For menu I dedupe Bronze down to the latest `_ingestion_timestamp` per `menu_item_id` before comparing anything against Silver:

```python
df_source = (
    df_bronze_menu
    .groupBy("menu_item_id")
    .agg(F.max("_ingestion_timestamp").alias("_ingestion_timestamp"))
    .join(df_bronze_menu, on=["menu_item_id", "_ingestion_timestamp"])
    .select("menu_item_id", "item_name", "category", "price", "_source_file", "_ingestion_timestamp")
)
```

Schema-wise menu carries the usual SCD2 columns (`effective_date`, `end_date`, `is_current`) plus lineage (`_source_file`, `_ingestion_timestamp`). Orders skips SCD tracking (an order event doesn't get "updated" it just happens) but keeps `topic`/`partition`/`offset` as its lineage instead of `_source_file` since that's what actually identifies where a row came from in Kafka.

## MERGE pipeline + SCD Type 2

Menu prices change and I wanted to keep the history instead of just overwriting it so this is SCD2. Change detection compares whatever's new in Bronze against whatever's currently live in Silver:

```python
df_changes = (
    df_source.alias("src")
    .join(df_silver_current.alias("cur"), on="menu_item_id", how="left")
    .filter(
        F.col("cur.menu_item_id").isNull()
        | (F.col("src.item_name") != F.col("cur.item_name"))
        | (F.col("src.category") != F.col("cur.category"))
        | (F.col("src.price") != F.col("cur.price"))
    )
    .select("src.*")
)
```

The write itself is two steps  a MERGE to close out whichever row was current then a plain append to open the new one:

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

Tested it by bumping the price on item 132 (Eggplant Parmesan) three times in a row. History came out clean:

| price | is_current | end_date |
|---|---|---|
| 16.95 | False | 2026-08-07 10:37:58 |
| 25.00 | False | 2026-08-07 10:47:24 |
| 50.00 | True | — |

Exactly one current row at any point and the old ones stick around with a real `end_date` instead of getting overwritten.

## Schema enforcement & evolution

I ran a producer scenario (`generate_batch_kafka.py --scenario schema_change`) that adds a `discount_code` field the orders table never had. First write no `mergeSchema` blows up as expected:

```
[DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table.
To enable schema migration using DataFrameWriter or DataStreamWriter please set: '.option("mergeSchema", "true")'.

Table schema: order_id, item_id, event_timestamp, topic, partition, offset, _ingestion_timestamp
Data schema:  order_id, item_id, event_timestamp, discount_code, topic, partition, offset, _ingestion_timestamp

- Table ACLs are enabled in this cluster, so automatic schema migration is not allowed.
  Please use the ALTER TABLE command for changing the schema.
```

Same write, `mergeSchema=true` added goes through and widens the table on its own:

```python
df_silver_orders_v2.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(SILVER_ORDER_FULL_TABLE)
```

Old rows got backfilled with `discount_code = NULL` new ones carry the actual value  nothing lost nothing broke downstream. Both attempts live in the same notebook the failing one wrapped in a `try/except` so the whole thing still runs end to end as a job instead of dying halfway through.

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

Then renamed a column just to see it happen:

```sql
ALTER TABLE slv_menu_items RENAME COLUMN category TO menu_category
```

No rewrite, instant and querying the table right after confirmed the rename with all the history intact. `DROP COLUMN` follows the same idea I left it commented out in the notebook rather than actually dropping something, no reason to lose data just to prove a point.

## On data contracts

With `mergeSchema` on the table just adapts to whatever shows up. Convenient but it also means the schema is effectively whatever the producer decided to send that day  nobody reviews it it just lands. A data contract flips that around: schema gets agreed on and versioned up front usually enforced through something like Confluent's schema registry (which is already sitting right there for this topic) and the producer can't publish something that breaks it.

|  | mergeSchema | data contract |
|---|---|---|
| speed | fast no coordination needed | slower needs both sides to agree |
| risk downstream | higher changes just flow through | lower  reviewed before they land |
| when you find out | after the fact | before it ever lands |

The `discount_code` situation above is basically a small scale example of exactly what a contract is meant to prevent  the producer changed shape without warning anyone and the first sign of it was a failed write two layers downstream. A contract would've caught that at the source.

## Re-runs and reliability

Where things stand:

- `bronze_menu.ipynb` — fine, uses overwrite + mergeSchema, rerunning just re reads the CSV
- `create_objects.ipynb` — fine, everything's `IF NOT EXISTS`
- `silver_menu_items.ipynb` — fine, the MERGE only acts on real changes
- `bronze_ingestion_orders_kafka.ipynb` — fine under normal conditions the streaming checkpoint tracks what's already been consumed
- `silver_orders.ipynb` — **not fine**. It's a plain append with no dedup so running it twice over the same Bronze range duplicates rows. Fix would be a MERGE keyed on `topic + partition + offset`, same idea as the SCD2 merge but with `whenNotMatchedInsertAll()` instead of an update branch. I know about it I'm leaving it for now  noting it here rather than pretending it's solved.

## Scheduling

None of the jobs have a schedule attached yet, on purpose. Still iterating on things, and given the append issue above an automatic schedule right now would just mean duplicate rows piling up unattended. Scheduling is the obvious next step once that's fixed.

## OPTIMIZE, VACUUM, and Liquid Clustering vs ZORDER

OPTIMIZE on `brz_order_details` — ten small streaming files compacted down to one:

```
numFilesRemoved: 10 → numFilesAdded: 1
totalSize: 37,760 bytes → 15,152 bytes
```

VACUUM (dry run, default 7-day retention) on `slv_menu_items` came back empty  nothing outside the retention window yet which is really just confirmation that the safety guard is doing its job rather than a failure of any kind.

Liquid Clustering (Databricks' current recommendation over ZORDER) enabled on `slv_order_details` clustered by `order_id` since that's the most likely filter/join column for this table:

```sql
ALTER TABLE slv_order_details CLUSTER BY (order_id)
```

Followed by OPTIMIZE which compacted two files into one  the run metrics show `clusteringStrategy` populated so it was actually clustering aware not just a generic file merge.

Quick comparison for the record: ZORDER still works but needs to be re run manually and doesn't mix well with static partitioning. Liquid Clustering can change its key later without a full rewrite and doesn't need partitioning at all  which is why I went with it here.

## Data quality rules

Two CHECK constraints on `slv_menu_items`:

```sql
ALTER TABLE slv_menu_items ADD CONSTRAINT positive_price CHECK (price > 0)
ALTER TABLE slv_menu_items ADD CONSTRAINT valid_menu_item_id CHECK (menu_item_id IS NOT NULL)
```

Tried writing a row with `price = -5.0` to confirm it actually bites:

```
Data quality constraint rejected the write as expected:
[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint positive_price (price > 0) violated by row with values:
 - price : -5.0.
```

Worth noting these constraints only check one row at a time they won't catch something like two rows both marked `is_current = true` for the same item. That kind of cross row rule is really enforced by the MERGE logic itself  not by a constraint.

## CI/CD

Bundle deploys to two targets  `dev` (Free Edition, serverless, PAT) and `prod` (Azure/SoftServe fixed cluster, PAT). Three GitHub Actions workflows sitting at the repo root since Actions won't pick them up otherwise in a monorepo with multiple labs:

- `deploy-dev.yml` — deploys to dev on push to `main` scoped to `Lab_4/**`. This one's actually been run and confirmed working.
- `validate-pr.yml` — runs `bundle validate` on PRs into `main`.
- `deploy-prod.yml` — deploys to prod on version tags.

Both targets use a Personal Access Token rather than a Service Principal. Not the ideal setup  Free Edition doesn't support the account level infrastructure Service Principals need  and I don't have admin rights on the SoftServe workspace to set one up there either. In a real production environment this is where I'd push for Service Principals with proper federation instead.

