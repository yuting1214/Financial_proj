from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# (1) Generate cards for OHLC info
def card_generate(info_df) -> dbc.Col:
    def one_card(stock_name, stock_id, close_price, spread, pct):
        icon = "bi bi-caret-down-fill" if spread < 0 else "bi bi-caret-up-fill"
        color = "danger" if spread < 0 else "success"
        dbc_card = dbc.Card(
            [
                dbc.CardHeader(
                    [
                        html.H4(stock_name, style={'text-align': 'left', 'align-self': 'flex-start', 'font-size':'22px'}),
                        html.H6(stock_id, style={'text-align': 'right', 'align-self': 'flex-end', 'font-size':'16px'}),
                    ], style={'display': 'flex', 'flex-direction': 'row','justify-content': 'space-between'}),
                dbc.CardBody(
                    [
                        html.H4(f"{close_price:,}", style={'text-align': 'center'}, className=f"text-{color}"),
                        html.Div([
                            html.Div([ html.I(className=icon),html.H5(spread)], style={'display': 'flex', 'flex-direction': 'row'}),
                            html.H5(f"{pct:.2%}")
                        ], style={'display': 'flex', 'flex-direction': 'row','justify-content': 'space-between'}, className=f"text-{color}")   
                    ],)
            ],
            style={'width': '5em'}, class_name='g-0 mb-0'
        )
        return dbc_card
    
    # Layout
    if info_df.shape[0] == 0:
        return dbc.Col([],  width=3, className="d-flex flex-column justify-content-between")
    card_layout = []
    for idx, row in info_df.iterrows():
        card = one_card(row['stock_name'], row['stock_id'], row['close'], row['spread'], row['pct'])
        if idx % 2 == 0:
            card_list = []
            card_list.append(card)
        else:
            card_list.append(card)
            card_layout.append(dbc.Row(dbc.CardGroup(card_list), class_name='g-0 mb-0'))
    # Include the last one
    if idx %2 == 0:
        card_layout.append(dbc.Row(dbc.CardGroup(card_list), class_name='g-0 mb-0'))
    return dbc.Col(card_layout,  width=3, className="gy-0 d-flex flex-column justify-content-between")

# (2) Generate CandleStick
def candlestick_plot(ohlc_df: pd.DataFrame, stock_id: str) -> go.Figure:
    # 1. Calculate MA columns
    def calculate_data(df):
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_index(inplace=False)
        df['MA5_close']=df['close'].rolling(5).mean()
        df['MA20_close']=df['close'].rolling(20).mean()
        df['MA60_close']=df['close'].rolling(60).mean()

        df['volume_unit'] = df['volume'] /1000
        df['MA5_vol']=df['volume_unit'].rolling(5).mean()
        df['MA20_vol']=df['volume_unit'].rolling(20).mean()
        df['MA60_vol']=df['volume_unit'].rolling(60).mean()
        df = df.reset_index()
        return df 
    #. 2 Generate plot
    def plot_func(df):
        stock_id = df['stock_id'].values[0]
        # Filter out empty date
        gap_date = set(pd.date_range(start=df.date[0], end=df.date.tolist()[-1], freq='d')) - set(df.date)
        gap_dates = pd.Series(list(gap_date)).dt.strftime('%Y-%m-%d').tolist()

        # Create subplots and mention plot grid size
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                    vertical_spacing=0.01, subplot_titles=('', ''), 
                    row_width=[0.2, 0.7])

        # Row 1
        ## 1. OHLC candlestick 
        fig.add_trace(go.Candlestick(x=df["date"], open=df["open"], high=df["high"],
                        low=df["low"], close=df["close"], name="OHLC", whiskerwidth=0, opacity=1) ,
                        row=1, col=1)
        color_map = ["#00995c" if rise else "#eb2409"  for rise in df["open"] < df["close"]]
        fig.update_traces(selector=dict(type="candlestick"), 
                        showlegend = False,
                        hoverlabel = dict(
                            bgcolor = color_map),
                        )
        ## 2. MA5, MA20, MA60
        col_names = df.columns[df.columns.str.contains('date|_close')].tolist()
        melt_MA_df = pd.melt(df[col_names], id_vars='date', value_vars=col_names[1:], var_name= 'MA')
        melt_MA_df['value'] = melt_MA_df['value'].round(2)
        color_dict = dict(zip(col_names[1:], ['rgb(240, 191, 76)', 'rgb(91, 209, 215)', 'RoyalBlue']))
        for MA in col_names[1:]:
            close_sr = melt_MA_df.loc[melt_MA_df.MA == MA, 'value']
            date_sr = melt_MA_df.loc[melt_MA_df.MA == MA, 'date']
            ma_trace = go.Scatter(x=date_sr, y=close_sr,
                                name=MA,
                                line=dict(color=color_dict[MA], width=1),
                                hovertemplate= MA + ':%{y}<extra></extra>',
                                hoverlabel=dict(
                                    font_size=8, 
                                    font_family="Rockwell",
                                    namelength = 0),
                                )
            fig.add_trace(ma_trace, row=1, col=1)   
        # Row 2
        ## 1. Volume(total value) bar chart
        # Bar trace for volumes on 2nd row without legend
        fig.add_trace(go.Bar(x=df['date'], y=df['volume_unit'],
                            marker_color=color_map,
                            showlegend=False,
                            hovertemplate='Vol:%{y:,.2s}<extra></extra>',)
                            , row=2, col=1)
        ## 2. MA5, MA20, MA60
        col_names = df.columns[df.columns.str.contains('date|_vol')].tolist()
        melt_MA_df = pd.melt(df[col_names], id_vars='date', value_vars=col_names[1:], var_name= 'MA')
        melt_MA_df['value'] = melt_MA_df['value'].round(2)
        color_dict = dict(zip(col_names[1:], ['rgb(240, 191, 76)', 'rgb(91, 209, 215)', 'RoyalBlue']))
        for MA in col_names[1:]:
            close_sr = melt_MA_df.loc[melt_MA_df.MA == MA, 'value']
            date_sr = melt_MA_df.loc[melt_MA_df.MA == MA, 'date']
            ma_trace = go.Scatter(x=date_sr, y=close_sr,
                                name=MA,
                                showlegend=False,
                                line=dict(color=color_dict[MA], width=1),
                                hovertemplate= MA + ':%{y:,.2s}<extra></extra>',
                                hoverlabel=dict(
                                    font_size=8, 
                                    font_family="Rockwell",
                                    namelength = 0),
                                )
            fig.add_trace(ma_trace, row=2, col=1)  

        # # Do not show OHLC's rangeslider plot 
        fig.update(layout_xaxis_rangeslider_visible=False)
        fig.update_layout(
            title = dict(
                text = f'{stock_id}',
                font_size = 18,
                yanchor='top',
                y=0.92,
                xanchor="left",
                x=0.045,
            ),
            template = 'plotly_dark',
            autosize=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=0.94,
                xanchor="left",
                x=0,
                bgcolor='rgba(0,0,0,0)',
                font=dict(
                    family="Courier",
                    size=12,
                    #color="black"
                    ),
                ),
            hovermode="x",
            xaxis=dict(rangebreaks=[dict(values=gap_dates)]),
            xaxis2=dict(
                rangebreaks=[dict(values=gap_dates) ],
                showgrid=False,
                linewidth=1,
                linecolor='black',
                mirror=True),
            margin=dict(
                l=50,
                r=50,
                b=50,
                t=50,
                pad=1
            ),
        ) 
        fig.update_xaxes(tickformat='%m/%d<br>%Y', nticks=25, tickfont=dict(size=9))
        fig.update_yaxes(side='right')
        return fig
    
    stock_df = ohlc_df[ohlc_df['stock_id']==stock_id]
    plot_df = calculate_data(stock_df)
    figure = plot_func(plot_df)
    return figure