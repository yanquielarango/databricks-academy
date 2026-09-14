INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')
(
    customer_id,
    first_name,
    last_name,
    email,
    active
)

SELECT
    1001,
    'Daniel',
    'Miller',
    'daniel.miller@example.com',
    1

WHERE NOT EXISTS (

    SELECT 1

    FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')

    WHERE customer_id = 1001
);



UPDATE IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')

SET
    email = 'mary.smith.updated@example.com'

WHERE customer_id = 1

  AND email <> 'mary.smith.updated@example.com';


DELETE FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_cdc_source')

WHERE customer_id = 2;