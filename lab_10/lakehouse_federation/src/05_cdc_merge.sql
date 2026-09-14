USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);


CREATE TABLE IF NOT EXISTS cdc_checkpoint
(
    pipeline_name STRING,
    last_processed_version BIGINT
)
USING DELTA;


INSERT INTO cdc_checkpoint
SELECT
    'customer_cdc',
    -1
WHERE NOT EXISTS (
    SELECT 1
    FROM cdc_checkpoint
    WHERE pipeline_name = 'customer_cdc'
);


MERGE INTO customer_current AS target

USING (

    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        active,
        _change_type,
        _commit_version,
        _commit_timestamp

    FROM (

        SELECT
            customer_id,
            first_name,
            last_name,
            email,
            active,
            _change_type,
            _commit_version,
            _commit_timestamp,

            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY
                    _commit_version DESC,
                    _commit_timestamp DESC
            ) AS rn

        FROM table_changes(
            'customer_cdc_source',
            0
        )

        WHERE _commit_version > (

            SELECT last_processed_version
            FROM cdc_checkpoint
            WHERE pipeline_name = 'customer_cdc'
        )

        AND _change_type IN (
            'insert',
            'update_postimage',
            'delete'
        )
    )

    WHERE rn = 1

) AS source

ON target.customer_id = source.customer_id

WHEN MATCHED
    AND source._change_type = 'delete'
THEN DELETE

WHEN MATCHED
    AND source._change_type IN ('insert', 'update_postimage')
THEN UPDATE SET
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.email = source.email,
    target.active = source.active

WHEN NOT MATCHED
    AND source._change_type IN ('insert', 'update_postimage')
THEN INSERT
(
    customer_id,
    first_name,
    last_name,
    email,
    active
)
VALUES
(
    source.customer_id,
    source.first_name,
    source.last_name,
    source.email,
    source.active
);



UPDATE cdc_checkpoint
SET last_processed_version = (

    SELECT MAX(_commit_version)

    FROM table_changes(
        'customer_cdc_source',
        0
    )
)

WHERE pipeline_name = 'customer_cdc';