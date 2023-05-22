from Backtest.TA.Overlays import SMA, ConSecRF

class Strategy:
    def __init__(self,
                 Data: Type[Data],):
        # 1. Retreive values for indicators calcuation 
        foreigner_balance = Data.get('foreigner_balance')
        volume = Data.get('volume')
        monthly_revenue = Data.get('monthly_revenue')
        # 2. Indicators calculation
        '''
        Two ways to calculate the Indicators:
        (1) pd.Dataframe methods
        (2) Data.I methods(For complicated indicators or other packages)
        Return:
        A dataframe with boolen values, date as index and stock_id as columns.
        '''
        ## (2-1) MA
        foreigner_balance_SMA_5 = Data.I(SMA, foreigner_balance, 5)
        foreigner_balance_SMA_10 = foreigner_balance.rolling(10).mean()
        volume_SMA_5 = volume.rolling(5).mean()
        ## (2-2) MoM monthly_revenue
        monthly_revenue_MOM = monthly_revenue.pct_change()
        con_3_rise = Data.I(ConSecRF, monthly_revenue_MOM, 'rise', 3)
        
        # 3. Signal creation
        Data.S('buy',
               ( (foreigner_balance_SMA_5 > foreigner_balance_SMA_10) &
                 (volume >= volume_SMA_5) & 
                 (con_3_rise.align_monthly_df())
               ), 30)
