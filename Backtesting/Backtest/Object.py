from Backtest.lib import get_closed_date
import pandas as pd
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Type, Union
from Backtest.Query import get_ohlc_df, get_data

class PyBackDataFrame(pd.DataFrame):
    # temporary properties
    _internal_names = pd.DataFrame._internal_names + ["fromdate", "todate"]
    _internal_names_set = set(_internal_names)
    
    @property
    def _constructor(self):
        return PyBackDataFrame

    @staticmethod
    def reshape(df1, df2):

        isfdf1 = isinstance(df1, PyBackDataFrame)
        isfdf2 = isinstance(df2, PyBackDataFrame)
        isdf1 = isinstance(df1, pd.DataFrame)
        isdf2 = isinstance(df2, pd.DataFrame)

        both_are_dataframe = (isfdf1 + isdf1) * (isfdf2 + isdf2) != 0

        if isinstance(df2, pd.Series):
            df2 = pd.DataFrame({c: df2 for c in df1.columns})

        if both_are_dataframe:
            index = df1.index.union(df2.index)
            columns = df1.columns.intersection(df2.columns)

            if len(df1.index) * len(df2.index) != 0:
                index_start = max(df1.index[0], df2.index[0])
                index = [t for t in index if index_start <= t]

            return df1.reindex(index=index, method='ffill')[columns], \
                df2.reindex(index=index, method='ffill')[columns]
        else:
            return df1, df2

    def __lt__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__lt__(df1, df2)

    def __gt__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__gt__(df1, df2)

    def __le__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__le__(df1, df2)

    def __ge__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__ge__(df1, df2)

    def __eq__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__eq__(df1, df2)

    def __ne__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__ne__(df1, df2)

    def __sub__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__sub__(df1, df2)

    def __add__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__add__(df1, df2)

    def __mul__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__mul__(df1, df2)

    def __truediv__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__truediv__(df1, df2)

    def __rshift__(self, other):
        return self.shift(-other)

    def __lshift__(self, other):
        return self.shift(other)

    def __and__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__and__(df1, df2)

    def __or__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__or__(df1, df2)

# Self-defined methods        
    def align_monthly_df(self) -> pd.DataFrame:
        closed_date = get_closed_date()
        work_date = pd.bdate_range(self.fromdate, self.todate)
        open_date = work_date[~work_date.isin(closed_date)]
        default_announce_date = pd.date_range(pd.Timestamp(self.fromdate) - pd.Timedelta('9d') - pd.DateOffset(months=1) ,
                                        pd.Timestamp(self.todate) - pd.Timedelta('9d'), freq='MS', inclusive='right') + \
                                        pd.DateOffset(days=9)
        # Update default announce date
        ## Special annoucement due to the government's policy
        speicial_date = {'2020/02/10': '2020/02/15', '2022/02/10': '2022/02/14'}
        announce_date = []
        min_open_date = open_date.min()
        for date in default_announce_date:
            if date < min_open_date:
                closed_date = date.strftime('%Y/%m/%d')
            else:
                closed_date = open_date[open_date >= date].min().strftime('%Y/%m/%d')
            if closed_date in speicial_date.keys():
                closed_date = speicial_date[closed_date]
            announce_date.append(closed_date)
        announce_date = pd.to_datetime(announce_date)
        # Match date
        match_time = []
        index = 1
        end_index = len(announce_date) - 1
        for date in open_date:
            if date < announce_date[index]:
                appen_index = index - 1
            else:
                if index < end_index:
                    index += 1
                    appen_index = index - 1
                else:
                    appen_index = index
            match_time.append(announce_date[appen_index].strftime('%Y/%m/%d'))
        match_time = pd.to_datetime(match_time)
        date_df = pd.DataFrame({'matched_date': match_time, 'open_date':open_date})
        return (self.set_index(announce_date)
                .merge(date_df, left_index = True, right_on = 'matched_date')
                .set_index('open_date')
                .drop('matched_date', axis = 1)
                .rename_axis('date')
                .sort_index()
                )

# Main Structures
class Data:
    def __init__(self,
                 fromdate: Union[str, pd.Timestamp] = None,
                 todate: Union[str, pd.Timestamp] = None,
                 stock_id: List[str] = None,) -> None:
        """
        A class used to retreive OHLC data based on fromdate, todate, and stock_id(optional) from database.
        """
        self.fromdate = fromdate
        self.todate = todate
        self.stock_id = stock_id
        self.data = get_ohlc_df(self.fromdate, self.todate, stock_id)
        self.SelectionRecord = {}
        self.buy = False
        self.sell = False
    # Manage attributes  
    @property
    def fromdate(self):
        return self._fromdate
    
    @fromdate.setter
    def fromdate(self, value):
        if value == None:
            value = pd.to_datetime('2000/01/01') 
        if not isinstance(value, (pd.Timestamp, str)):
            raise ValueError(' `fromdate` must be one of the following [`str`, `pd.Timestamp`]')
        else:
            value = pd.Timestamp(value)
        self._fromdate = value
        
    @property
    def todate(self):
        return self._todate
    
    @todate.setter
    def todate(self, value):
        if value == None:
            value = pd.Timestamp.now().date()
        if not isinstance(value, (pd.Timestamp, str)):
            raise ValueError(' `todate` must be one of the following [`str`, `pd.Timestamp`]')
        else:
            value = pd.Timestamp(value)
        self._todate = value    
        
    def get(self,
            ValueName: str) -> None:
        '''
        Get a dataframe with values of ValueName and index of date in PyBackDataFrame class.
        '''
        return_df = PyBackDataFrame(get_data(self.fromdate, self.todate, self.stock_id, ValueName))
        PyBackDataFrame.fromdate =  self.fromdate
        PyBackDataFrame.todate = self.todate
        return return_df
        
    def I(self,
          func: Callable,
          *args) -> pd.DataFrame:
        '''
        I stands for Indicators. Used for calculating indicators.
        '''
        return func(*args)

    def S(self,
          action: str,
          signals: pd.DataFrame,
          period: int = 1):
        '''
        S stands for Signals. Used for aggregating signals.
        '''
        action = action.lower()
        if action not in ['buy', 'sell']:
            raise TypeError(f'`{action}` should be either `buy` or `sell`.')
        signals_df = signals[::period]
        agg_result = signals_df.apply(lambda x: signals_df.columns[x].tolist(), axis = 1)
        if action == 'buy':
            self.buy = True
        if action == 'sell':
            self.sell = True
        self.SelectionRecord[action] = dict(zip(agg_result.index.strftime('%Y/%m/%d'), agg_result.values)) 

