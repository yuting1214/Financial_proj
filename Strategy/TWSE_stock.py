
from finlab import data
from finlab.backtest import sim
import numpy as np
import pandas as pd

ROE = data.get("fundamental_features:ROE稅後")
pb = data.get("price_earning_ratio:股價淨值比")
rev = data.get('monthly_revenue:當月營收')
rev_yoy_growth = data.get('monthly_revenue:去年同月增減(%)')

# Low is better
pb_rank = pb.apply(lambda row: 10 - pd.cut(row, bins=[row.quantile(i/10) for i in range(0, 11)], 
                                             labels=False, include_lowest=True), axis=1)
# High is better
ROE_rank = ROE.apply(lambda row: pd.cut(row, bins=[row.quantile(i/10) for i in range(0, 11)], 
                                             labels=False, include_lowest=True) + 1, axis=1)
# Weight
weight_df = ROE_rank * 0.6 + pb_rank*0.4

# 近2月平均營收
rev_ma = rev.average(2)
# 近2月平均營收創12個月來新高
rev_cond1 = rev_ma == rev_ma.rolling(12, min_periods=6).max()

# Stop loss
buy = (weight_df * rev_cond1).is_largest(10)
sim(buy, resample="Q", fee_ratio=1.425/1000/3, stop_loss=0.10)
