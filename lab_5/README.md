# Lab 5 — Lakeflow Declarative Pipelines vs Classic Spark

Goal for this lab: build the same kind of pipeline as Lab 4 but using Lakeflow Declarative Pipelines (formerly Delta Live Tables) and compare it against the classic PySpark approach.

I reused the same data sources as Lab 4 (the menu CSV and the Confluent Kafka orders topic) so the comparison would be based on real working code from both sides instead of a made up example.

## Setup

Scaffolded with `databricks pipelines init` then reorganized to match my own structure. The project has `src/transformations/bronze` and `src/transformations/silver` plus `src/producer` reused from Lab 4 for generating test events.

Two targets same environments as Lab 4. `dev` runs on Free Edition. `prod` runs on Azure through SoftServe. Both use `resources.schemas` and `resources.volumes` as bundle managed resources so schemas and the landing volume get created as part of the deploy itself instead of needing a separate setup job, unlike the `setup_job` approach in Lab 4.

## Bronze: CSV and streaming sources

Menu started out as a `@dp.materialized_view` doing a full CSV reread every run. Switched it to a `@dp.table` using Auto Loader (`cloudFiles`) instead so it only picks up new files incrementally rather than reprocessing the whole CSV each time. Same incremental pattern I used for Bronze menu in Lab 4:

```python
CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/menu/"

MENU_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True),
])

@dp.table(
    name=f"{BRONZE_SCHEMA}.menu_bronze",
    comment="Menu raw data processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
    },
)
def menu_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(MENU_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )
    return (
        df.withColumn("file_name", F.col("_metadata.file_path"))
          .withColumn("ingest_datetime", F.current_timestamp())
    )
```

Orders comes from Kafka so it is a `@dp.table` (a real streaming table) too:

```python
@dp.table(
    name="orders_bronze",
    schema="${var.bronze_schema}",
    comment="Orders raw data from Confluent Kafka",
)
def orders_bronze():
    api_key = dbutils.secrets.get(scope="confluent-scope", key="api-key")
    api_secret = dbutils.secrets.get(scope="confluent-scope", key="api-secret")
    kafka_options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
        "subscribe": TOPIC_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{api_key}" password="{api_secret}";'
        ),
        "startingOffsets": "earliest",
    }
    df_raw = spark.readStream.format("kafka").options(**kafka_options).load()
    return (
        df_raw
        .withColumn("raw_json", F.col("value").cast("string"))
        .withColumn("ingest_datetime", F.current_timestamp())
        .select("raw_json", "topic", "partition", "offset", "timestamp", "ingest_datetime")
    )
```

The decorator you choose (`materialized_view` vs `table`) still matters for anything downstream reading incrementally. I first wrote `menu_silver_clean` as a `@dp.table` reading `menu_bronze` with `read_stream` back when `menu_bronze` was still a materialized view that got fully recomputed on every run. That failed with `DELTA_SOURCE_TABLE_IGNORE_CHANGES` because streaming reads cannot handle a source that overwrites itself. Once I switched `menu_bronze` to Auto Loader that source became append-only which is why `menu_silver_clean` could in principle go back to being a streaming table too. I kept it as a materialized view for now since it already works but it's worth revisiting.

Also switched from passing the schema as a decorator argument (`schema="${var.bronze_schema}"`) to resolving it inside the function body with `spark.conf.get("bronze_schema")` and building the fully qualified name as an f-string (`f"{BRONZE_SCHEMA}.menu_bronze"`). Both work. This version reads the schema from the pipeline's `configuration` block which needs `bronze_schema` and `silver_schema` defined there.

## Expectations

Instead of `ALTER TABLE ADD CONSTRAINT` like in Lab 4 expectations are declared right on the function:

```python
@dp.materialized_view(
    name="menu_silver_clean",
    schema="${var.silver_schema}",
    comment="Cleaned menu events, ready for SCD2 processing",
)
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_drop("valid_menu_item_id", "menu_item_id IS NOT NULL")
def menu_silver_clean():
    ...
```

Same idea for orders:

```python
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
def orders_silver():
    ...
```
Big behavior difference from Lab 4 worth calling out. A `CHECK` constraint in Delta rejects the entire write if even one row violates it. `expect_or_drop` just quietly drops the bad rows and keeps the rest. Neither is better on its own, it depends on whether you want a hard stop or a tolerant pipeline that just filters out garbage.

## SCD Type 2

This is where the difference is biggest. In Lab 4 the SCD2 logic was a two step MERGE, close the current row then insert a new one, roughly 40 lines of code. In Lakeflow it is:

```python
dp.create_streaming_table(
    name="menu_silver",
    schema="${var.silver_schema}",
)

dp.create_auto_cdc_flow(
    target="menu_silver",
    source="menu_silver_clean",
    keys=["menu_item_id"],
    sequence_by="ingest_datetime",
    stored_as_scd_type=2,
)
```

That is the whole thing. No manual MERGE and no manually managing `is_current` or `end_date`, Lakeflow handles all of it internally.

## Lineage

This is fully automatic and needs no code at all. After running the pipeline the UI shows the whole dependency graph on its own:

```
orders_bronze (streaming table) -> orders_silver (streaming table)
menu_bronze (materialized view) -> menu_silver_clean (materialized view) -> menu_silver (streaming table)
```

Every node showed green after a run: `orders_bronze`, `orders_silver`, `menu_bronze`, `menu_silver_clean` (32 output records, 2 expectations met) and `menu_silver`. In Lab 4 there was no equivalent of this. You had to read through the notebooks yourself to figure out what depended on what.

## Reload safely

Lakeflow has a Full Refresh option from the UI or with `databricks pipelines run --full-refresh`. I ran a full refresh specifically on `menu_silver` to see how it behaves with the SCD2 history since that felt like the riskiest table to reload.

In Lab 4 reloading safely meant deleting the target table and the streaming checkpoint by hand exactly what caused the `OffsetOutOfRangeException` incident once the checkpoint pointed to offsets Kafka had already expired. Full refresh in Lakeflow is a single controlled action instead of a manual multi step process which removes a lot of the room for that kind of mistake.

## Declarative vs classic, side by side

| | Lab 4 (classic) | Lab 5 (declarative) |
|---|---|---|
| Defining a table | Manual `CREATE TABLE` plus write logic | `@dp.materialized_view` / `@dp.table`, schema inferred |
| Execution order | You control notebook/cell order yourself | Lakeflow resolves the dependency graph automatically |
| SCD Type 2 | About 40 lines of manual two step MERGE | 6 lines with `create_auto_cdc_flow` |
| Data quality | `CHECK` constraint, rejects the whole write | `expect_or_drop`, drops only the bad rows |
| Streaming checkpoints | Manual, and I hit a real checkpoint bug | Managed automatically by the framework |
| Lineage | Not visible, had to read the code | Automatic, visible in the UI with zero extra code |
| Reloading data | Manual delete of table and checkpoint | Full refresh, one controlled action |
| Environment config | Interactive widgets at runtime | Bundle variables, resolved at deploy time |

Operational simplicity clearly favors declarative here. Less code, fewer places to introduce a bug, and things like lineage and reload come for free. The trade off is flexibility. In Lab 4 I could write literally any PySpark logic I wanted inside a cell. In Lakeflow you are working inside the shape the framework expects (materialized view vs streaming table, `create_auto_cdc_flow`'s specific parameters) which is great until you need something the framework does not directly support.

On cost, serverless Lakeflow pipelines bill differently from a job running on a fixed cluster. For something that runs briefly and infrequently like this menu/orders pipeline that is probably cheaper. For a workload running nonstop it would need real comparison against a properly sized cluster, which I have not done here.

## A DABs gotcha that cost me some time

Small tip that took me a while to figure out. I spent a good amount of time searching through the docs, trying different configurations and debugging an issue with the schema names generated by Databricks even when you explicitly define the schema you want, for example `<catalog>.<bronze_schema>`.

If you're using `mode: development` Databricks can create a development prefixed schema like `<catalog>.dev_<username>_<bronze_schema>`.

At first I thought there was something wrong with my schema configuration but it turns out it's related to how DABs handles development mode.

If you don't want the `[dev username]` prefix on resources and also want to avoid the development prefixed schema you can remove `mode: development` from the dev target and use:

```yaml
dev:
  default: true

  workspace:
    host: https://<your-workspace>

  presets:
    name_prefix: ''
    pipelines_development: true
    trigger_pause_status: "PAUSED"
```

Then the resources and schemas keep the names you actually defined instead of having the development prefix added.

The important part is that `name_prefix: ''` by itself doesn't solve the issue if `mode: development` is enabled. This took me a while to figure out so hopefully it saves someone else some debugging time.

## CI/CD

Same pattern as Lab 4, reusing the existing GitHub Secrets since they point at the same two workspaces. Three workflows: `deploy-lab5-dev.yml` (push to main, scoped to `lab_5/**`), `validate-lab5-pr.yml` (PRs into main) and `deploy-lab5-prod.yml` (tags matching `lab5-v*`). Dev deploy and PR validation are both confirmed working. Prod deploy is not exercised yet.

