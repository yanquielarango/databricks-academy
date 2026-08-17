ALTER TABLE ${catalog}.yanquiel_gold.fact_orders
SET ROW FILTER ${catalog}.yanquiel_gold.filter_orders ON (item_id);