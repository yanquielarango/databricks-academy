ALTER TABLE ${catalog}.yanquiel_gold.fact_orders
ALTER COLUMN discount_code
SET MASK ${catalog}.yanquiel_gold.mask_discount_code;