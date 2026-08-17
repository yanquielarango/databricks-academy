CREATE OR REPLACE FUNCTION ${catalog}.yanquiel_gold.filter_orders(item_id INT)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN current_user() = 'yanquiel@softserve.academy'
      THEN TRUE
    WHEN current_user() = 'lbiel@softserve.academy'
      THEN item_id <= 110
    ELSE FALSE
  END;