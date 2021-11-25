CREATE TABLE Daily_institution_total_trade (
  Date DATE NOT NULL,
  Foreigner_buy_total BIGINT CHECK (Foreigner_buy_total >= 0)
  Foreigner_sell_total BIGINT CHECK (Foreigner_sell_total >= 0),
  Foreigner_total_balance BIGINT,
  Foreign_Invester_total_buy BIGINT CHECK (Foreign_Invester_total_buy >= 0),
  Foreign_Invester_total_sell BIGINT CHECK (Foreign_Invester_total_sell >= 0),
  Foreign_Invester_total_balance BIGINT,
  Foreign_Dealer_Self_total_buy BIGINT CHECK (Foreign_Dealer_Self_total_buy >= 0),
  Foreign_Dealer_Self_total_sell BIGINT CHECK (Foreign_Dealer_Self_total_sell >= 0),
  Foreign_Dealer_Self_total_balance BIGINT,
  Dealer_total_buy BIGINT CHECK (Dealer_total_buy >= 0),
  Dealer_total_sell BIGINT CHECK (Dealer_total_sell >= 0),
  Dealer_total_balance BIGINT,
  Dealer_self_total_buy BIGINT CHECK (Dealer_self_total_buy >= 0),
  Dealer_self_total_sell BIGINT CHECK (Dealer_self_total_sell >= 0),
  Dealer_self_total_balance BIGINT,
  Dealer_Hedging_total_buy BIGINT CHECK (Dealer_Hedging_total_buy >= 0),
  Dealer_Hedging_total_sell BIGINT CHECK (Dealer_Hedging_total_sell >= 0),
  Dealer_Hedging_total_balance BIGINT,
  Investment_Trust_total_buy BIGINT CHECK (Investment_Trust_total_buy >= 0),
  Investment_Trust_total_sell BIGINT CHECK (Investment_Trust_total_sell >= 0),
  Investment_Trust_total_balance BIGINT
  );
