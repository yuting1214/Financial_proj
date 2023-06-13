import pandas as pd

def calculate_wr_ratio(start, end, cond_df, open_df, close_df):
    """
    Calculates the WR ratio for stock trading.

    Args:
        start (datetime-like): Start date for selecting data.
        end (datetime-like): End date for selecting data.
        cond_df (pandas DataFrame): Conditional DataFrame with stock selection criteria.
        open_df (pandas DataFrame): DataFrame containing opening stock prices.
        close_df (pandas DataFrame): DataFrame containing closing stock prices.

    Returns:
        pandas DataFrame: Final DataFrame with stock_id, opening price, closing price, buy date, and sell date.

    Logic:
    If the previous selected stock_ids are not in the current selection, then sell.
    Only the buying criteria is provided, not the selling criteria.
    """

    # Check if open_df and close_df are pivot tables
    if isinstance(close_df.index, pd.DatetimeIndex):
        close_df = pd.melt(close_df[close_df.index >= start].reset_index(), id_vars=['date'],
                           value_vars=close_df.columns, var_name='stock_id', value_name='close')
    if isinstance(open_df.index, pd.DatetimeIndex):
        open_df = pd.melt(open_df[open_df.index >= start].reset_index(), id_vars=['date'],
                          value_vars=open_df.columns, var_name='stock_id', value_name='open')
    
    # Select relevant data based on start and end dates
    wr_cond = cond_df[(cond_df.index >= start) & (cond_df.index <= end)]
    last_date_index = wr_cond.index[-1]
    
    # Convert the condition DataFrame into a series of sets
    wr_series = wr_cond.apply(lambda x: set(wr_cond.columns[x]), axis=1)
    
    # Calculate the stocks to buy based on changes in the selected stock_ids
    buy_series = (wr_series - wr_series.shift()).fillna(wr_series).explode().reset_index().rename(columns={0: "stock_id"})
    
    # Calculate the stocks to sell based on changes in the selected stock_ids
    sell_series = (wr_series.shift() - wr_series).explode().reset_index().rename(columns={0: "stock_id"})
    
    # Create a DataFrame with the buying and selling stock_ids
    wr_df = pd.merge(buy_series, sell_series, on='stock_id', how='left', suffixes=('_buy', '_sell'))
    
    # Prevent multiple buying stocks
    ## (1) Set date_sell as NaT for the last date_buy
    wr_df.loc[wr_df.date_buy == last_date_index, 'date_sell'] = pd.NaT
    
    ## (2) Remove rows where date_sell is less than date_buy
    wr_df = wr_df[~(wr_df['date_sell'] < wr_df['date_buy'])]
    
    ## (3) Keep the first transaction for each stock
    wr_df = wr_df.drop_duplicates(subset=['date_buy', 'stock_id']).reset_index(drop=True)
    
    # Merge with open_df and close_df to get the stock prices for buy and sell dates
    final_wr_df = pd.merge(
        pd.merge(wr_df, open_df, left_on=['date_buy', 'stock_id'], right_on=['date', 'stock_id'], how='left'),
        close_df, left_on=['date_sell', 'stock_id'], right_on=['date', 'stock_id'], how='left')[['stock_id', 'open', 'close', 'date_buy', 'date_sell']]
    
    return final_wr_df
