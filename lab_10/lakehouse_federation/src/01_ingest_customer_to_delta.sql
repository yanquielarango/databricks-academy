CREATE SCHEMA IF NOT EXISTS
IDENTIFIER(:catalog || '.' || :schema);


CREATE OR REPLACE TABLE
IDENTIFIER(:catalog || '.' || :schema || '.customer_delta')
USING DELTA
AS
SELECT *
FROM IDENTIFIER(:foreign_catalog || '.public.customer');