CREATE OR REPLACE FUNCTION ${catalog}.yanquiel_gold.mask_discount_code(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN current_user() = 'yanquiel@softserve.academy'
      THEN value
    ELSE
      '****'
  END;