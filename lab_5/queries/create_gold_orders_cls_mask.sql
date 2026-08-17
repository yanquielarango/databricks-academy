CREATE OR REPLACE FUNCTION dbr_dev.yanquiel_gold.mask_discount_code(value STRING)
RETURNS STRING
RETURN
  CASE
    WHEN current_user() = 'yanquiel@softserve.academy'
      THEN value
    ELSE
      '****'
  END;