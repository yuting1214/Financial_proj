CREATE TABLE Daily_institution_trade (
  Datetime DATE NOT NULL,
  Stock_id SMALLINT PRIMARY KEY,
  Foreigner_buy BIGINT CHECK (Foreigner_buy >= 0),
  Foreigner_sell BIGINT CHECK (Foreigner_sell >= 0),
  Foreigner_balance BIGINT,
  Foreign_Invester_buy BIGINT CHECK (Foreign_Invester_buy >= 0),
  Foreign_Invester_sell BIGINT CHECK (Foreign_Invester_sell >= 0),
  Foreign_Invester_balance BIGINT,
  Foreign_Dealer_Self_buy BIGINT CHECK (Foreign_Dealer_Self_buy >= 0),
  Foreign_Dealer_Self_sell BIGINT CHECK (Foreign_Dealer_Self_sell >= 0),
  Foreign_Dealer_Self_balance BIGINT,
  Dealer_buy BIGINT CHECK (Dealer_buy >= 0),
  Dealer_sell BIGINT CHECK (Dealer_sell >= 0),
  Dealer_balance BIGINT,
  Dealer_self_buy BIGINT CHECK (Dealer_self_buy >= 0),
  Dealer_self_sell BIGINT CHECK (Dealer_self_sell >= 0),
  Dealer_self_balance BIGINT,
  Dealer_Hedging_buy BIGINT CHECK (Dealer_Hedging_buy >= 0),
  Dealer_Hedging_sell BIGINT CHECK (Dealer_Hedging_sell >= 0),
  Dealer_Hedging_balance BIGINT,
  Investment_Trust_buy BIGINT CHECK (Investment_Trust_buy >= 0),
  Investment_Trust_sell BIGINT CHECK (Investment_Trust_sell >= 0),
  Investment_Trust_balance BIGINT
  );
  
  