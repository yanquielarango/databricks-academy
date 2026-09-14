SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(p.payment_id) AS payment_count,
    ROUND(SUM(p.amount), 2) AS total_spent
FROM IDENTIFIER(:catalog || '.' || :schema || '.customer_delta') AS c

JOIN IDENTIFIER(:foreign_catalog || '.public.payment') AS p
    ON c.customer_id = p.customer_id

GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name

ORDER BY total_spent DESC

LIMIT 20;