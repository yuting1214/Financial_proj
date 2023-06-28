import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from Query.utils import stock_selection, date_prepare, ag_table_df, card_generate

# (1) Data preparation
## (1-1) Date
date = None
pre_day_num = 10
date_list = date_prepare(date, pre_day_num)
date_index = date_list[0]
date_index_str = date_index.strftime('%Y/%m/%d')

## (1-2) DataFrames
source = '財報狗'
value_name = 'total_balance'
value_map = {'total_balance':'三大法人當日總買賣超'} 
(industry_info_df, subindustry_info_df), (industry_ohlc_df, subindustry_ohlc_df), (plot_industry_df, plot_subindustry_df) = stock_selection(source, value_name, date_list)

# (2) Dashboard 
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
app.title = 'Three insti buy'
## (2-1) AG Table
table_industry_df = ag_table_df(industry_info_df, industry_ohlc_df, value_name)
table_subindustry_df = ag_table_df(subindustry_info_df, subindustry_ohlc_df, value_name)

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
        "headerName": value_map[value_name],
        "field": value_name,
        "tooltipField": value_name,
        "tooltipComponent": "CustomTooltip",
        "valueFormatter": {"function": "d3.format('(,.0f')(params.value)"},
        'cellStyle': {'textAlign': 'right'}
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

industry_grid = dag.AgGrid(
    id="industry-df",
    className="ag-theme-alpine-dark",
    rowData=table_industry_df.to_dict("records"),
    columnDefs=ColDefs,
    defaultColDef={"resizable": True, "sortable": True, "filter": True},
    dashGridOptions={"domLayout": "autoHeight"},
)

subindustry_grid = dag.AgGrid(
    id="subindustry-df",
    className="ag-theme-alpine-dark",
    rowData=table_subindustry_df.to_dict("records"),
    columnDefs=ColDefs,
    defaultColDef={"resizable": True, "sortable": True, "filter": True},
    dashGridOptions={"domLayout": "autoHeight"},
)

## (2-2) Card
industry_card = card_generate(table_industry_df)
subindustry_card = card_generate(table_subindustry_df)

## (2-3) Figure
fig_industry = px.line(plot_industry_df, x="date", y=value_name, color="Industry_name",  template="plotly_dark",)
fig_subindustry = px.line(plot_subindustry_df, x="date", y=value_name, color="subindustry_name",  template="plotly_dark",)

## (2-4) Markdown
markdown = dcc.Markdown('''
Source: 財報狗, Value: 三大法人總買賣超, Frequence: 日

Logic: 

1. Aggregate by ['date', stream] | stream = {'產業', '子產業'}

2. Criteria: Identify a list of industries and subindustries that have experienced {3} consecutive days of increasing {Value}.

3. Retrieve all stocks from the identified industries or subindustries including information such as ['Industry_name', 'subindustry_name', 'stock_name', 'stock_id'], and OHLC data
    1. Aggregate {Value} per stock_id on today_date.
    2. Fold all subindustries with the same stock_id using '|'.

4. Select only the top stocks with top {1} {Value} in each industry or subindustry.

5. Prepare industry or subindustry data with prev {10} date

6. For visual, if the numbers of industries or subindustries > 10, list top 10 {Value} in the graph.
''')


app.layout = dbc.Container([

    dbc.Row(
        dbc.Col(html.H1(f"{date_index_str} :Daily Target Stocks",
                        className='text-center text-info mb-2'),
                width=12)
    ),

    dbc.Row(
        dbc.Col(markdown)
    ),

    dbc.Row(
        dbc.Col(html.H1("Target Stocks by Industry",
                        className='text-center text-success mb-6'),
                width=12)
    ),

    dbc.Row(
        dbc.Col(dcc.Graph(id='example-graph', figure=fig_industry))
    ),

    dbc.Row([
        dbc.Col(industry_grid, width=9),
        industry_card
    ]),

    dbc.Row(
        dbc.Col(html.H1("Target Stocks by Sub-Industry",
                        className='text-center text-success mb-4'),
                width=12)
    ), 
    dbc.Row(
        dbc.Col(dcc.Graph(id='subind-graph', figure=fig_subindustry))
    ),

    dbc.Row([
        dbc.Col(subindustry_grid, width = 9,className="d-flex flex-column justify-content-between"),
        subindustry_card
    ], 'gx-0'),
], fluid=True)


# Callback section: connecting the components
# ************************************************************************
# Line chart - Single
# @app.callback(
#     Output('line-fig', 'figure'),
#     Input('my-dpdn', 'value')
# )
# def update_graph(stock_slctd):
#     dff = df[df['Symbols']==stock_slctd]
#     figln = px.line(dff, x='Date', y='High')
#     return figln


# # Line chart - multiple
# @app.callback(
#     Output('line-fig2', 'figure'),
#     Input('my-dpdn2', 'value')
# )
# def update_graph(stock_slctd):
#     dff = df[df['Symbols'].isin(stock_slctd)]
#     figln2 = px.line(dff, x='Date', y='Open', color='Symbols')
#     return figln2


# # Histogram
# @app.callback(
#     Output('my-hist', 'figure'),
#     Input('my-checklist', 'value')
# )
# def update_graph(stock_slctd):
#     dff = df[df['Symbols'].isin(stock_slctd)]
#     dff = dff[dff['Date']=='2020-12-03']
#     fighist = px.histogram(dff, x='Symbols', y='Close')
#     return fighist


if __name__=='__main__':
    app.run_server(debug=False, port=5566)
