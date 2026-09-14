USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);



CREATE TABLE IF NOT EXISTS customer_history
(
    customer_id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    active INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    source_commit_version BIGINT
)
USING DELTA;


INSERT INTO cdc_checkpoint
SELECT
    'customer_history',
    0
WHERE NOT EXISTS (
    SELECT 1
    FROM cdc_checkpoint
    WHERE pipeline_name = 'customer_history'
);



INSERT INTO customer_history
(
    customer_id,
    first_name,
    last_name,
    email,
    active,
    valid_from,
    valid_to,
    is_current,
    source_commit_version
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    active,
    _commit_timestamp,
    NULL,
    TRUE,
    _commit_version

FROM table_changes(
    'customer_cdc_source',
    0,
    0
)

WHERE _change_type = 'insert'

AND NOT EXISTS (
    SELECT 1
    FROM customer_history
);


MERGE INTO customer_history AS target

USING (

    SELECT
        customer_id,
        _change_type,
        _commit_version,
        _commit_timestamp

    FROM table_changes(
        'customer_cdc_source',
        1
    )

    WHERE _commit_version > (

        SELECT last_processed_version
        FROM cdc_checkpoint
        WHERE pipeline_name = 'customer_history'
    )

    AND _change_type IN (
        'update_postimage',
        'delete'
    )

) AS source

ON target.customer_id = source.customer_id
AND target.is_current = TRUE

WHEN MATCHED THEN UPDATE SET
    target.valid_to = source._commit_timestamp,
    target.is_current = FALSE;

INSERT INTO customer_history
(
    customer_id,
    first_name,
    last_name,
    email,
    active,
    valid_from,
    valid_to,
    is_current,
    source_commit_version
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    active,
    _commit_timestamp,
    NULL,
    TRUE,
    _commit_version

FROM table_changes(
    'customer_cdc_source',
    1
)

WHERE _commit_version > (

    SELECT last_processed_version
    FROM cdc_checkpoint
    WHERE pipeline_name = 'customer_history'
)

AND _change_type IN (
    'insert',
    'update_postimage'
);



UPDATE cdc_checkpoint

SET last_processed_version = (

    SELECT MAX(_commit_version)
    FROM table_changes(
        'customer_cdc_source',
        0
    )
)

WHERE pipeline_name = 'customer_history';