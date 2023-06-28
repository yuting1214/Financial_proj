CREATE TABLE Daily_total_trade(
  Date DATE NOT NULL,
  Taiex_volume BIGINT CHECK (Taiex_volume >= 0),
  Taiex_value BIGINT CHECK (Taiex_value >= 0),
  Taiex_turnover BIGINT CHECK (Taiex_turnover >= 0),
  Taiex_index NUMERIC(7, 2),
  Taiex_spread NUMERIC(7, 2),
  TPEx_volume BIGINT CHECK (TPEx_volume >= 0),
  TPEx_value BIGINT CHECK (TPEx_value >= 0),
  TPEx_turnover BIGINT CHECK (TPEx_turnover >= 0),
  TPEx_index NUMERIC(7, 2),
  TPEx_spread NUMERIC(7, 2)
);
