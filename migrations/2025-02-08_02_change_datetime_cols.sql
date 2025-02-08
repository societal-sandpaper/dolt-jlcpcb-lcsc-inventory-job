ALTER TABLE components
ADD COLUMN last_on_stock_dt DATETIME,
ADD COLUMN last_update_dt DATETIME;

UPDATE components
SET last_on_stock_dt = FROM_UNIXTIME(last_on_stock),
    last_update_dt = FROM_UNIXTIME(last_update);

ALTER TABLE components
DROP COLUMN last_on_stock,
DROP COLUMN last_update;

ALTER TABLE components
CHANGE COLUMN last_on_stock_dt last_on_stock DATETIME,
CHANGE COLUMN last_update_dt last_update DATETIME;
