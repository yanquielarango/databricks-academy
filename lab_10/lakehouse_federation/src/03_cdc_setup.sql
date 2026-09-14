CREATE TABLE IF NOT EXISTS
IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
)
AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    active
FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_delta');


CREATE TABLE IF NOT EXISTS
IDENTIFIER(:catalog || '.' || :schema || '.customer_current')
USING DELTA
AS
SELECT *
FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')
WHERE 1 = 0;