import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import finlab
from finlab import data
from query.local import Queryer
import plotly.graph_objects as go
from visual.utils import card_generate, candlestick_plot

#*** Change these two parts
# Plug-in here Strategy
def strategy(strategy_name, start_date, wr_base_date, in_period):
    # Data must include open and close for wr_df
    股本 = data.get('financial_statement:股本')
    price = data.get('price:收盤價')
    close = price 
    open = data.get('price:開盤價')
    vol = data.get('price:成交股數')

    # Strategy start here
    #===================================================================================
    ## 票面分割還原比例
    empty_df = pd.DataFrame(1, columns=price.columns, index=price.index)
    tse_par = data.get('par_value_change_tse:twse_par_value_change_divide_ratio')
    otc_par = data.get('par_value_change_otc:otc_par_value_change_divide_ratio')
    tse_divide_ratio = (empty_df*tse_par).fillna(1).cumprod()
    otc_divide_ratio = (empty_df*otc_par).fillna(1).cumprod()
    par_divide_ratio = tse_divide_ratio*otc_divide_ratio

    市值 = 股本 * price / 10 * 1000 * par_divide_ratio


    df1 = data.get('financial_statement:投資活動之淨現金流入_流出')
    df2 = data.get('financial_statement:營業活動之淨現金流入_流出')
    自由現金流 = (df1 + df2).rolling(4).mean()

    稅後淨利 = data.get('fundamental_features:經常稅後淨利')
    權益總計 = data.get('financial_statement:股東權益總額')
    股東權益報酬率 = 稅後淨利/ 權益總計

    營業利益成長率 = data.get('fundamental_features:營業利益成長率')

    當月營收 = data.get('monthly_revenue:當月營收')* 1000
    當季營收 = 當月營收.rolling(4).sum()
    市值營收比 = 市值 / 當季營收
    condition1 = (市值 < 1e10)
    condition2 = 自由現金流 > 0
    condition3 = 股東權益報酬率 > 0
    condition4 = 營業利益成長率 > 0
    condition5 = 市值營收比 < 2
    condition6 = vol > 200000
    rsv = (price - price.rolling(60).min()) / (price.rolling(60).max() - price.rolling(60).min())

    position = ((condition1 & condition2 
                & condition3 & condition4 
                & condition5 & condition6)
                * rsv).is_largest(10)
    final_cond = position.reindex(當月營收.index_str_to_date().index)
    #===================================================================================
    if in_period:
        select_cond = final_cond[final_cond.index==start_date]
        # Create selection df
        select_series = select_cond.apply(lambda x: set(select_cond.columns[x]), axis =1)
        select_df = (select_series
                    .explode()
                    .reset_index()
                    .rename(columns={0: "stock_id"}))
        select_df['strategy_name'] = strategy_name
        select_df['select_status'] = True
    else:
        select_df = None
    # Create Winning ratio df
    wr_cond = final_cond[(final_cond.index>=wr_base_date) & (final_cond.index<=start_date)]
    last_date_index = wr_cond.index[-1]
    wr_series = wr_cond.apply(lambda x: set(wr_cond.columns[x]), axis =1)
    open_df = pd.melt(open[open.index >= wr_base_date].reset_index(), id_vars=['date'], value_vars=open.columns, var_name='stock_id', value_name='open')
    close_df = pd.melt(close[close.index >= wr_base_date].reset_index(), id_vars=['date'], value_vars=close.columns, var_name='stock_id', value_name='close')
    buy_series = (wr_series - wr_series.shift()).fillna(wr_series).explode().reset_index().rename(columns={0: "stock_id"})
    sell_series = (wr_series.shift() - wr_series).explode().reset_index().rename(columns={0: "stock_id"})
    wr_df = pd.merge(buy_series, sell_series, on = 'stock_id', how='left', suffixes=('_buy', '_sell'))
    # Prevent multiple buying stocks
    ## (1)
    wr_df.loc[wr_df.date_buy==last_date_index, 'date_sell'] = pd.NaT
    ## (2) Remove date_sell < date_buy
    wr_df = wr_df[~(wr_df['date_sell'] < wr_df['date_buy'])]
    ## (3) Keep first transaction
    wr_df = wr_df.drop_duplicates(subset=['date_buy', 'stock_id']).reset_index(drop=True)
    final_wr_df = pd.merge(pd.merge(wr_df, open_df, left_on = ['date_buy', 'stock_id'], right_on = ['date','stock_id'], how='left'), \
        close_df, left_on = ['date_sell', 'stock_id'], right_on = ['date','stock_id'], how='left')[['stock_id', 'open', 'close', 'date_buy', 'date_sell']]
    return select_df, final_wr_df
# Update period
def updating_period():
  startofyear = pd.Timestamp(year=pd.Timestamp.today().year, month=1, day=1)
  start_date = startofyear + pd.Timedelta('9d') # 01/10/year
  tenth_month = pd.date_range(start=start_date, periods=12, freq=pd.DateOffset(months=1))
  idx = pd.Series(tenth_month.weekday)[tenth_month.weekday.isin([5,6])].index
  return_date = pd.Series(tenth_month)
  return_date[idx] = return_date[idx] + pd.offsets.BDay()
  return pd.DatetimeIndex(return_date)

# Configuration
conn_params = {
"host" : "localhost",
"database" : "Fin_proj",
"user" : "postgres",
"password" : "nckumark"
}
finlab.login("imYEE5zw6nTjQ2NxzJ7ToVfEOYW67YeK4qIigkessdnwkqZuar4dK1e7qYanqoNw#vip_m")

today_date = pd.Timestamp.today().normalize()
wr_base_date = pd.Timestamp(year=today_date.year, month=1, day=1) # 01/01/year
queryer = Queryer(today_date, conn_params)
start_date = queryer.start_date
start_date_str = start_date.strftime('%Y/%m/%d')
updating_list = updating_period()
strategy_name = '小資族資優生策略'
in_period = start_date in updating_list
select_df, wr_df = strategy(strategy_name, start_date, wr_base_date, in_period)
if in_period:
    print(f'{start_date_str} :Update & Insert')
    queryer.update_strategy_table()
    queryer.insert_df(select_df, 'strategy')

strategy_stock_df = queryer.get_strategy_stock_ids(strategy_name)
strategy_stock_ids = strategy_stock_df['stock_id'].tolist()

## DF preparation
### (1) Start_date's info_df
info_df = queryer.table_data(strategy_stock_ids)
sub_wr_df = wr_df.loc[wr_df.date_sell.isna(), ['stock_id', 'date_buy']]
sub_wr_df['date_buy'] = sub_wr_df['date_buy'].dt.strftime('%Y-%m-%d')
info_df = pd.merge(info_df, sub_wr_df, on='stock_id')
### (2) Diff ohlc_df
selected_date_open_df = wr_df.loc[wr_df.date_sell.isna(), ['stock_id', 'open']]
current_date_ohlc_df = queryer.get_stock_OHLC_date(start_date_str, strategy_stock_ids)
assert set(selected_date_open_df['stock_id']) == set(strategy_stock_ids)
diff_df = pd.merge(selected_date_open_df, current_date_ohlc_df[['stock_id', 'close']], on = 'stock_id')
diff_df = pd.merge(diff_df, info_df[['stock_id', 'stock_name']], on='stock_id')
diff_df['spread'] = (diff_df['close'].astype(float) - diff_df['open'].astype(float)).round(2)
diff_df['pct'] = (diff_df['spread'] / diff_df['open']).round(2)
diff_df = diff_df.sort_values('pct', ascending=False, ignore_index=True)
diff_df[['spread', 'close', 'pct']] = diff_df[['spread', 'close', 'pct']].astype(object)
### (3) CandleStick
# Start from 1.5 years ago till start_date
since_date_str = (start_date - pd.DateOffset(months=18)).strftime('%Y/%m/%d')
stock_df = queryer.get_stock_OHLC_since(since_date_str, strategy_stock_ids)

# App 
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
app.title = 'Finlab'

## (2-1) AG Table
ColDefs = [
    {
        "headerName": "股票代號",
        "field": "stock_id",
        "cellRenderer": "StockLink",
        "tooltipField": "stock_id",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "股票名稱",
        "field": "stock_name",
        "tooltipField": "stock_name",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "產業",
        "field": "Industry_name",
        "tooltipField": "Industry_name",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "子產業",
        "field": "subindustry_name",
        "tooltipField": "subindustry_name",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "買入日期",
        "field": "date_buy",
        "tooltipField": "date_buy",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "開盤",
        "field": "open",
        "tooltipField": "open",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "最高",
        "field": "high",
        "tooltipField": "high",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "最低",
        "field": "low",
        "tooltipField": "low",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "收盤",
        "field": "close",
        "tooltipField": "close",
        "tooltipComponent": "CustomTooltip",
    },
    {
        "headerName": "成交張數",
        "field": "volume",
        "tooltipField": "volume",
        "valueFormatter": {"function": "d3.format('(,.0f')(params.value)"},
        "tooltipComponent": "CustomTooltip",
        'cellStyle': {'textAlign': 'right'}
    },
    {
        "headerName": "成交金額",
        "field": "value",
        "tooltipField": "value",
        "valueFormatter": {"function": "d3.format('($,.0f')(params.value)"},
        "tooltipComponent": "CustomTooltip",
        'cellStyle': {'textAlign': 'right'}
    },
]

info_grid = dag.AgGrid(
    id="info-df",
    className="ag-theme-alpine-dark",
    rowData=info_df.to_dict("records"),
    columnDefs=ColDefs,
    defaultColDef={"resizable": True, "sortable": True, "filter": True},
    dashGridOptions={#"domLayout": "autoHeight",
                     "rowSelection": "single"},
)

## (2-2) Card
info_card = card_generate(info_df)
variation_card = card_generate(diff_df)

## App-layout
app.layout = dbc.Container([
    dbc.Row(
        dbc.Col(html.H1(f"{start_date_str} :Daily Stock List",
                        className='text-center text-info mb-2'),
                width=12)
    ),
    dbc.Row([
        dbc.Col(html.H1(f"Strategy: {'Mid-Class & Mid-Scale'}",
                        className='text-left text-success mb-6'), width=9),
        dbc.Col(html.H4(f"{start_date_str} - stock info",
                        className='text-left text-success mb-5'), width=3),            
            ], align = 'end'
    ),
    dbc.Row([
        dbc.Col([
            dbc.Row(dcc.Graph(id='candlestick')),
            dbc.Row(info_grid)
            ], width=9),
        info_card
    ], class_name="gx-0"),
    dbc.Row([
        dbc.Col(html.H5(f"Performance till now",
                        className='text-left text-success mb-4'), width=3),            
    ], align="end"),
    dbc.Row([
        variation_card,
    ], class_name='g-0'),
], fluid=True)

# Callback
@app.callback(
    Output("candlestick", "figure"),
    Input("info-df", "selectedRows"),
)
def candlestick_basedon_selected(selection):
    if selection:
        print(selection)
        stock_id = selection[0]['stock_id']
        print(f'Stock: {stock_id} is selected!')
        plot = candlestick_plot(stock_df, stock_id)
        return plot
    return go.Figure()

if __name__=='__main__':
    app.run_server(debug=False, port=8010)