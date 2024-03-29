CREATE TABLE Daily_institution_total_trade (
  Date DATE NOT NULL,
  Taiex_Foreigner_total_buy BIGINT CHECK (Taiex_Foreigner_total_buy >= 0),
  Taiex_Foreigner_total_sell BIGINT CHECK (Taiex_Foreigner_total_sell >= 0),
  Taiex_Foreigner_total_balance BIGINT,
  Taiex_Foreign_Invester_total_buy BIGINT CHECK (Taiex_Foreign_Invester_total_buy >= 0),
  Taiex_Foreign_Invester_total_sell BIGINT CHECK (Taiex_Foreign_Invester_total_sell >= 0),
  Taiex_Foreign_Invester_total_balance BIGINT,
  Taiex_Foreign_Dealer_Self_total_buy BIGINT CHECK (Taiex_Foreign_Dealer_Self_total_buy >= 0),
  Taiex_Foreign_Dealer_Self_total_sell BIGINT CHECK (Taiex_Foreign_Dealer_Self_total_sell >= 0),
  Taiex_Foreign_Dealer_Self_total_balance BIGINT,
  Taiex_Dealer_total_buy BIGINT CHECK (Taiex_Dealer_total_buy >= 0),
  Taiex_Dealer_total_sell BIGINT CHECK (Taiex_Dealer_total_sell >= 0),
  Taiex_Dealer_total_balance BIGINT,
  Taiex_Dealer_self_total_buy BIGINT CHECK (Taiex_Dealer_self_total_buy >= 0),
  Taiex_Dealer_self_total_sell BIGINT CHECK (Taiex_Dealer_self_total_sell >= 0),
  Taiex_Dealer_self_total_balance BIGINT,
  Taiex_Dealer_Hedging_total_buy BIGINT CHECK (Taiex_Dealer_Hedging_total_buy >= 0),
  Taiex_Dealer_Hedging_total_sell BIGINT CHECK (Taiex_Dealer_Hedging_total_sell >= 0),
  Taiex_Dealer_Hedging_total_balance BIGINT,
  Taiex_Investment_Trust_total_buy BIGINT CHECK (Taiex_Investment_Trust_total_buy >= 0),
  Taiex_Investment_Trust_total_sell BIGINT CHECK (Taiex_Investment_Trust_total_sell >= 0),
  Taiex_Investment_Trust_total_balance BIGINT,
    TPEx_Foreigner_total_buy BIGINT CHECK (TPEx_Foreigner_total_buy >= 0),
  TPEx_Foreigner_total_sell BIGINT CHECK (TPEx_Foreigner_total_sell >= 0),
  TPEx_Foreigner_total_balance BIGINT,
  TPEx_Foreign_Invester_total_buy BIGINT CHECK (TPEx_Foreign_Invester_total_buy >= 0),
  TPEx_Foreign_Invester_total_sell BIGINT CHECK (TPEx_Foreign_Invester_total_sell >= 0),
  TPEx_Foreign_Invester_total_balance BIGINT,
  TPEx_Foreign_Dealer_Self_total_buy BIGINT CHECK (TPEx_Foreign_Dealer_Self_total_buy >= 0),
  TPEx_Foreign_Dealer_Self_total_sell BIGINT CHECK (TPEx_Foreign_Dealer_Self_total_sell >= 0),
  TPEx_Foreign_Dealer_Self_total_balance BIGINT,
  TPEx_Dealer_total_buy BIGINT CHECK (TPEx_Dealer_total_buy >= 0),
  TPEx_Dealer_total_sell BIGINT CHECK (TPEx_Dealer_total_sell >= 0),
  TPEx_Dealer_total_balance BIGINT,
  TPEx_Dealer_self_total_buy BIGINT CHECK (TPEx_Dealer_self_total_buy >= 0),
  TPEx_Dealer_self_total_sell BIGINT CHECK (TPEx_Dealer_self_total_sell >= 0),
  TPEx_Dealer_self_total_balance BIGINT,
  TPEx_Dealer_Hedging_total_buy BIGINT CHECK (TPEx_Dealer_Hedging_total_buy >= 0),
  TPEx_Dealer_Hedging_total_sell BIGINT CHECK (TPEx_Dealer_Hedging_total_sell >= 0),
  TPEx_Dealer_Hedging_total_balance BIGINT,
  TPEx_Investment_Trust_total_buy BIGINT CHECK (TPEx_Investment_Trust_total_buy >= 0),
  TPEx_Investment_Trust_total_sell BIGINT CHECK (TPEx_Investment_Trust_total_sell >= 0),
  TPEx_Investment_Trust_total_balance BIGINT
);
