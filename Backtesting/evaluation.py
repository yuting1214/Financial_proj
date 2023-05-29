def Evaluation_table(transaction_record, price_df):
    """
    Generates an evaluation table based on transaction records and price data.

    Args:
        transaction_record (dict): A dictionary containing transaction records with dates as keys.
                                   Each transaction record should be a dictionary with the following keys:
                                   - 'current_cash' (float): The current cash value.
                                   - 'current_hold' (dict): A dictionary containing current holdings, where the keys
                                                             are stock IDs and the values are tuples with two elements:
                                                             [0] - Cost
                                                             [1] - Quantity
                                   - 'bought_stock' (dict): A dictionary containing new bought stocks, where the keys
                                                             are stock IDs and the values are tuples with two elements:
                                                             [0] - Cost
                                                             [1] - Quantity
                                   - 'sold_stock' (dict): A dictionary containing new sold stocks, where the keys
                                                             are stock IDs and the values are pair of tuples(cost, revenue) with two elements:
                                                             [0] - Cost
                                                             [1] - Quantity                                                            
        price_df (pandas.DataFrame): A DataFrame containing OHLC data

    Returns:
        pandas.DataFrame: The evaluation table with the following columns:
                          - 'date': The trade date.
                          - 'Cash': The current cash value.
                          - 'stock_value': The value of stocks held.
                          - 'total_value': The total portfolio value (cash + stock value).
                          - 'Hold_stock': The IDs of stocks held.
                          - 'Sold_stock': The ID of the sold stock.

    """
    eval_price = 'close'
    all_tradedate = pd.to_datetime(price_df.date.sort_values().unique()).tolist()
    transaction_date = pd.to_datetime(list(transaction_record.keys()))
    # Create match holding time
    match_time = []
    index = 1
    end_index = len(transaction_date) - 1
    for date in all_tradedate:
        if date < transaction_date[index]:
            match_time.append(transaction_date[index-1])
        else:
            if index < end_index:
                index += 1
                match_time.append(transaction_date[index-1])
            else:
                match_time.append(transaction_date[index])
    # Organize result
    date_df = pd.DataFrame({'date':all_tradedate, 'match_date':match_time})
    result_df = pd.DataFrame(list(map(lambda x: [pd.to_datetime(x[0]), 
                                                 x[1]['current_cash'],
                                                 ','.join(x[1]['current_hold']),
                                                 x[1]['sold_stock']], transaction_record.items())),
                             columns= ['date', 'Cash', 'Hold_stock','Sold_stock'])
    quant_list = []
    for item in test_trade_weight_new.items():
        content_dict = item[1]['current_hold']
        sub_quant = []
        sub_stock = []
        for key in content_dict.keys():
            sub_quant.append(content_dict[key][1])
            sub_stock.append(key)
        if len(sub_quant) == 0:
            quant_list.append([[], []])
        else:
            quant_list.append([sub_stock, sub_quant])
    result_df['Hold_quant'] = quant_list     
    merged_df = date_df.merge(result_df[['date', 'Hold_stock', 'Hold_quant', 'Cash']],
                              left_on = 'match_date', right_on='date', how = 'left').merge(
        result_df[['date', 'Sold_stock']], left_on = 'date_x', right_on='date', how = 'left')
    stock_quant_series = merged_df['Hold_quant'].apply(lambda x: pd.DataFrame({'stock_id':x[0], 'quant':x[1]}))
    temp_df = merged_df.apply(lambda x: price_df.loc[(price_df.date == x['date_x']) \
                                                                   & (price_df.stock_id.isin(x['Hold_stock'].split(','))),
                                                                   ['stock_id', eval_price]].reset_index(drop=True), axis = 1)
    # Iteration
    stock_value = []
    for idx in range(len(stock_quant_series)):
        temp_quant_df = stock_quant_series[idx]
        temp_price_df = temp_df[idx]
        ## Check available data for evaluation, if not, find the previous date and fill back if more than 5 days then count as zero
        start_idx = idx - 1
        count = 0
        while temp_quant_df.shape[0] != temp_price_df.shape[0]:
            missed_stocks = list(set(temp_quant_df['stock_id']) - set(temp_price_df['stock_id']))
            if count < 5:
                filled_df = price_df.loc[(price_df.date == merged_df.loc[start_idx, 'date_x']) \
                             & (price_df.stock_id.isin(missed_stocks)), ['stock_id', eval_price]]
            else:
                filled_df = pd.DataFrame({'stock_id': missed_stocks})
                filled_df[eval_price] = 0
            temp_price_df = pd.concat([temp_price_df, filled_df], ignore_index=True)
            start_idx -= 1
            count += 1
        if temp_quant_df.empty:
            stock_value.append(0)
        else:
            value = temp_quant_df.apply(lambda row: 1000 * row['quant'] * temp_price_df.loc[temp_price_df['stock_id'] == row['stock_id'], eval_price].iloc[0], axis=1).sum()
            stock_value.append(value)    
    merged_df['stock_value'] = stock_value
    merged_df['total_value'] = merged_df['Cash'] + merged_df['stock_value']
    export_df = merged_df[['date_x', 'Cash', 'stock_value', 'total_value', 'Hold_stock', 'Sold_stock']].rename(columns = {'date_x':'date'})
    return export_df
