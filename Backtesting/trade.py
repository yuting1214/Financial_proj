def all_meet_same_rule_weight_new(indicator_dict, price_df, total_cash, commission):
    # (1) Sub-function: buy stock with weight
    def Buy_stock_evenly_quant(target_df, total_cash, current_cash, commission, buy_criterion, cash_ratio = 0.2):
        unit = 1
        stock_unit = 1000
        # if current_cash ratio, change with current_cash
        cash_threshold = total_cash * cash_ratio
        original_price_series = target_df[target_df.columns[target_df.columns != 'stock_id'][0]]
        # When there is no target stock_id
        if len(original_price_series) == 0:
            remain_cash = current_cash
            return_stock = {}
            return remain_cash, return_stock 
        # Else start iteration
        while (current_cash - (unit * stock_unit * original_price_series.sum())) >= cash_threshold:
            unit += 1
        unit -= 1
        if unit != 0:
            target_stock_ids = target_df['stock_id']
            used_fund = unit * original_price_series.sum() * stock_unit * (1+commission)
            remain_cash = current_cash - used_fund
            target_stocks = target_stock_ids.tolist()
            cost_list = original_price_series.tolist()
            return_stock = dict(zip(target_stocks, zip(cost_list, [unit for i in range(len(cost_list))])))
            return remain_cash, return_stock
        else:
            assert buy_criterion in ['from_high', 'from_low', 'random'], 'buy_criterion must be from_high, from_low or random'
            if buy_criterion == 'from_high':
                index = original_price_series.sort_values(ascending=False).index
            elif buy_criterion == 'from_low':
                index = original_price_series.sort_values(ascending=True).index
            else:
                sample_size = target_df.shape[0]
                index = np.random.choice(sample_size, sample_size, replace=False)
            target_stock_ids = target_df['stock_id'][index]
            # Iteration
            used_fund = 0
            sub_index = 0
            cost_list = []
            for sub_index in range(len(index)):
                stock_price = original_price_series[index[sub_index]]
                cost_list.append(stock_price)
                stock_cost = stock_price * stock_unit * (1+commission)
                used_fund += stock_cost
                if used_fund > cash_threshold:
                    sub_index -= 1
                    used_fund -= stock_cost
                    break
            remain_cash = current_cash - used_fund
            target_stocks = target_df.loc[index[:(sub_index+1)], 'stock_id'].tolist()
            cost_lists = cost_list[:(sub_index+1)]
            return_stock = dict(zip(target_stocks, zip(cost_lists, [1 for i in range(len(cost_lists))])))
            return remain_cash, return_stock 
        
    # (2) Trade iteration
    # Configuration
    '''
    'current_hold': {'stock_id':(cost, quantity)},
    'current_cash':the left available cash after selling the filtered stock and buying the new stocks,
    'sold_stock':{'stock_id':((cost, quant), (sold_price, quant)) },
    'bought_stock': {'stock_id': (cost, quantity)}
    '''
    transaction_history = {}
    current_hold_all_info = {}# {'stock_id':(cost, quant)}
    current_cash = total_cash
    buying_price = 'open'
    selling_price = 'close'
    buy_criterion = 'from_low'
    # update here when using different trade method
    indicator_dict = indicator_dict['buy']
    cash_ratio = 0.2

    # Iteration (fixed structure)
    for date in indicator_dict.keys():
        # Check match of Signal date and OHLC date 
        date_index = price_df.date == date
        if not date_index.any():
            continue
        meet_all_stock_id = indicator_dict[date]
        ## (1) Sold first
        raw_sold_stock = list(current_hold_all_info.keys() - meet_all_stock_id)
        ### Filter out the qualified but unable to sell stocks
        temp_sold_df = price_df.loc[date_index].reset_index(drop = True)
        raw_stock_id_index = pd.Index(temp_sold_df.stock_id).get_indexer(raw_sold_stock)
        stock_id_index = []
        sold_stock = []
        for idx in range(len(raw_stock_id_index)):
            if raw_stock_id_index[idx] != -1:
                stock_id_index.append(raw_stock_id_index[idx])
                sold_stock.append(raw_sold_stock[idx])
        ### Start selling
        if len(sold_stock) != 0:
            cost_list = []
            quant_list = []
            for sold_stock_id in sold_stock:
                cost_list.append(current_hold_all_info[sold_stock_id][0])
                quant_list.append(current_hold_all_info[sold_stock_id][1])
                current_hold_all_info.pop(sold_stock_id)
            price_arr = temp_sold_df.loc[stock_id_index, selling_price].to_numpy()
            revenue_list = price_arr.tolist()
            current_cash += (price_arr * np.array(quant_list) * (1 - (commission + 0.003)) * 1000).sum()
            sold_stock_all_info = dict(zip(sold_stock, zip(zip(cost_list, quant_list), zip(revenue_list, quant_list))))
        else:
            sold_stock_all_info = dict()
        ## (2) Buy new
        available_stock = list(meet_all_stock_id - current_hold_all_info.keys())
        target_df = price_df.loc[(price_df.stock_id.isin(available_stock)) & (date_index) \
                                & (price_df[buying_price] > 0), ['stock_id', buying_price]].reset_index(drop = True)
        ## * Plug-in weigh method here
        current_cash, new_hold = Buy_stock_evenly_quant(target_df, total_cash, current_cash, commission, buy_criterion, cash_ratio)
        current_hold_all_info.update(new_hold)
        current_hold_all_info_return = current_hold_all_info.copy()
        # Record
        transaction_history[date] =  {'current_hold':current_hold_all_info_return, 'current_cash':current_cash,
                                  'sold_stock':sold_stock_all_info, 'bought_stock':new_hold}
    return transaction_history
