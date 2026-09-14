INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')
(
    customer_id,
    first_name,
    last_name,
    email,
    active
)

SELECT
    1002,
    'Emma',
    'Wilson',
    'emma.wilson@example.com',
    1

WHERE NOT EXISTS (
    SELECT 1
    FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')
    WHERE customer_id = 1002
);


UPDATE IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')

SET email = 'linda.williams.updated@example.com'

WHERE customer_id = 3
  AND email <> 'linda.williams.updated@example.com';



DELETE FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')

WHERE customer_id = 4;