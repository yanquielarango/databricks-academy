CREATE OR REPLACE FUNCTION dbr_dev.yanquiel_gold.filter_orders(item_id INT)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN session_user() IN (
      'yanquiel@yagdata.com',
      'yanquiel.arango@gmail.com',
      'yanquiel@softserve.academy',
      '9dc17781-cd90-4512-aaaa-dc45aa687261'
    )
      THEN TRUE

    WHEN session_user() = 'lbiel@softserve.academy'
      THEN item_id <= 110

    ELSE FALSE
  END;