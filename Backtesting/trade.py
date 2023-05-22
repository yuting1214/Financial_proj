def all_meet_same_rule(indicator_dict, price_df, cash, commission):
    def Buy_stock(target_df, cash, commission, method):
        assert method in ['from_high', 'from_low', 'random'], 'method must be from_high, from_low or random'
        original_series = target_df[target_df.columns[target_df.columns != 'stock_id'][0]]
        if method == 'from_high':
            index = original_series.sort_values(ascending=False).index
        elif method == 'from_low':
            index = original_series.sort_values(ascending=True).index
        else:
            sample_size = target_df.shape[0]
            index = np.random.choice(sample_size, sample_size, replace=False)
        used_fund = 0
        unit = 1000
        sub_index = 0
        cost_list = []
        for sub_index in range(len(index)):
            stock_price = original_series[index[sub_index]]
            cost_list.append(stock_price)
            stock_cost = stock_price * unit * (1+commission)
            used_fund += stock_cost
            if used_fund > cash:
                sub_index -= 1
                used_fund -= stock_cost
                break
        if sub_index == -1:
            remain_cash = cash
            return_stock = {}
        else:
            remain_cash = cash - used_fund
            target_stock = target_df.loc[index[:(sub_index+1)], 'stock_id'].tolist()
            return_stock = dict(zip(target_stock, cost_list[:(sub_index+1)]))
        return remain_cash, return_stock
    # Setting
    '''
    'current_hold': {'stock_id':cost},
    'sold_stock':{'stock_id':(cost, sold_price)}
    'current_cash':the left available cash after selling the filtered stock and buying the new stocks.
    '''
    transaction_history = {}
    current_hold_all_info = {}# {'stock_id':cost}
    current_cash = cash
    buy_price_by = 'open'
    sell_price_by = 'close'
    method = 'from_low'
    indicator_dict = indicator_dict['buy']
    # Iteration
    for date in indicator_dict.keys():
        # Check match of Signal date and OHLC date 
        date_index = price_df.date == date
        if not date_index.any():
            continue
        meet_all_stock_id = indicator_dict[date]
        # Sold first
        sold_stock = current_hold_all_info.keys() - meet_all_stock_id
        if len(sold_stock) != 0:
            cost_list = []
            for sold_stock_id in sold_stock:
                cost_list.append(current_hold_all_info[sold_stock_id])
                current_hold_all_info.pop(sold_stock_id)
            temp_sold_df = price_df.loc[date_index].reset_index(drop = True)
            target_df = temp_sold_df.loc[pd.Index(temp_sold_df.stock_id).get_indexer(sold_stock), sell_price_by]
            revenue_list = target_df.tolist()
            current_cash += (target_df * (1 - (commission + 0.003)) * 1000).sum()
            sold_stock_all_info = dict(zip(sold_stock, zip(cost_list, revenue_list)))
        else:
            sold_stock_all_info = dict()
        # Buy new
        available_stock = meet_all_stock_id - current_hold_all_info.keys()
        target_df = price_df.loc[(price_df.stock_id.isin(available_stock)) & (date_index) \
                                & (price_df[buy_price_by] > 0), ['stock_id', buy_price_by]].reset_index(drop = True)
        current_cash, new_hold = Buy_stock(target_df, current_cash, commission, method)
        current_hold_all_info.update(new_hold)
        current_hold_all_info_return = current_hold_all_info.copy()
        # Record
        transaction_history[date] =  {'current_hold':current_hold_all_info_return, 'current_cash':current_cash,
                                  'sold_stock':sold_stock_all_info, 'bought_stock':new_hold}
    return transaction_history
