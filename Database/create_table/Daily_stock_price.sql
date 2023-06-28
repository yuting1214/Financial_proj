Create TABLE Daily_stock_price (
  Date DATE NOT NULL,
  Stock_id VARCHAR(6),
  Volume BIGINT CHECK (Volume >= 0),
  Value BIGINT CHECK (Value >= 0),
  Open NUMERIC(7, 2),
  Max NUMERIC(7, 2),
  Min NUMERIC(7, 2),
  Close NUMERIC(7, 2),
  Spread NUMERIC(7, 2),
  Turnover BIGINT CHECK (Turnover >= 0)
)
