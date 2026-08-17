ALTER MATERIALIZED VIEW dbr_dev.yanquiel_gold.fact_orders
SET ROW FILTER dbr_dev.yanquiel_gold.filter_orders ON (item_id);