# Video:    [Bootstrap with Cards - Dash Plotly](https://youtu.be/aEz1-72PKwc)
# Docs:     [Dash Bootstrap Components:](https://dash-bootstrap-components.opensource.faculty.ai/docs/components/card/)
#           [Dash HTML/CORE Components:](https://dash.plotly.com/dash-html-components)
from dash import Dash, dcc, html, Output, Input       # pip install dash
import dash_bootstrap_components as dbc               # pip install dash-bootstrap-components
import plotly.express as px                     # pip install pandas; pip install plotly express
import plotly.graph_objects as go

fig = go.Figure()

fig.add_trace(go.Indicator(
    mode = "number+delta",
    value = 200,
    domain = {'x': [0, 0.5], 'y': [0, 0.5]},
    delta = {'reference': 400, 'relative': True, 'position' : "left"}))

fig.update_layout(
    autosize=True,
    minreducedwidth=50,
    minreducedheight=25)

df = px.data.gapminder()

app = Dash(__name__, external_stylesheets=[dbc.themes.SPACELAB, dbc.icons.BOOTSTRAP])

card_main = dbc.Card(
    [
        dbc.CardImg(src="/assets/ball_of_sun.jpg", top=True, bottom=False,
                    title="Image by Kevin Dinkel", alt='Learn Dash Bootstrap Card Component'),
        dbc.CardBody(
            [
                html.H4("Learn Dash with Charming Data", className="card-title"),
                html.H6("Lesson 1:", className="card-subtitle"),
                html.P(
                    "Choose the year you would like to see on the bubble chart.",
                    className="card-text",
                ),
                dcc.Dropdown(id='user_choice', options=[{'label': yr, "value": yr} for yr in df.year.unique()],
                             value=2007, clearable=False, style={"color": "#000000"}),
                # dbc.Button("Press me", color="primary"),
                # dbc.CardLink("GirlsWhoCode", href="https://girlswhocode.com/", target="_blank"),
            ]
        ),
    ],
    color="dark",   # https://bootswatch.com/default/ for more card colors
    inverse=True,   # change color of text (black or white)
    outline=False,  # True = remove the block colors from the background and header
)

card_question = dbc.Card(
    [
        dbc.CardBody([
            html.H4("Question 1", className="card-title"),
            html.P("What was India's life expectancy in 1952?", className="card-text"),
            dbc.ListGroup(
                [
                    dbc.ListGroupItem("A. 55 years"),
                    dbc.ListGroupItem("B. 37 years"),
                    dbc.ListGroupItem("C. 49 years"),
                ], flush=True)
        ]),
    ], color="warning",
)

card_graph = dbc.Card(
        dcc.Graph(id='my_bar', figure={}), body=True, color="secondary",
)

# card = dbc.Card(
#     [
#         dbc.CardHeader(
#             [
#                         html.Div(
#             children=[
#                 html.H2("Left Header", style={'text-align': 'left'}),
#             ],
#             style={'width': '50%', 'display': 'inline-block'}
#         ),
#         html.Div(
#             children=[
#                 html.H2("Right Header", style={'text-align': 'right'}),
#             ],
#             style={'width': '50%', 'display': 'inline-block'}
#         )
#             ]
#         ),
#         dbc.CardBody(
#             [
#                 html.H4("Card title", className="card-title"),
#                 html.P("This is some card text", className="card-text"),
#             ]
#         ),
#         dbc.CardFooter("This is the footer"),
#     ],
#     style={"width": "18rem"},
# )

def make_card(coin):
    change = coin["price_change_percentage_24h"]
    price = coin["current_price"]
    color = "danger" if change < 0 else "success"
    icon = "bi bi-arrow-down" if change < 0 else "bi bi-arrow-up"
    return dbc.Card(
        html.Div(
            [
                html.H4(
                    [
                        html.Img(src=coin["image"], height=35, className="me-1"),
                        coin["name"],
                    ]
                ),
                html.H4(f"${price:,}"),
                html.H5(
                    [f"{round(change, 2)}%", html.I(className=icon), " 24hr"],
                    className=f"text-{color}",
                ),
            ],
            className=f"border-{color} border-start border-5",
        ),
        className="text-center text-nowrap my-2 p-2",
    )

icon = "bi bi-caret-up-fill"
price = 123
change = 0.4567
color = 'success'
card = dbc.Card(
    [
        dbc.CardHeader(
            [
                html.H4("Stock_id", style={'text-align': 'left', 'align-self': 'flex-start', 'font-size':'22px'}),
                html.H6("Right Header", style={'text-align': 'right', 'align-self': 'flex-end', 'font-size':'12px'}),
            ], style={'display': 'flex', 'flex-direction': 'row','justify-content': 'space-between'}),

        dbc.CardBody(
            [
                html.H4(f"{price:,}", style={'text-align': 'center'}),
                html.Div([
                html.I(className=icon),
                html.H6("Stock_id"),
                html.H6("Right Header"),
                ], style={'display': 'flex','justify-content': 'space-between'}, className=f"text-{color}")
                # html.H5(
                #     [html.I(className=icon), " 24hr", f"{round(change, 2)}%"], style={'text-align': 'center'},
                #     className=f"text-{color}",
                # ),
            ], )
    ],
    style={'width': '5em'}
)

app.layout = html.Div([
    dbc.Row([
        dbc.Col(html.H1("Bootstrap with Cards - Dash Plotly",
                        # rf. https://hackerthemes.com/bootstrap-cheatsheet/
                        className='text-center text-primary'
        ))
    ]),
    dbc.Row([dbc.Col(card_main, width=3),
             dbc.Col(card_question, width=3),
             dbc.Col(card_graph, width=6)], justify="around", className='gx-0'),  # justify="start", "center", "end", "between", "around"

    dbc.Row([
        dbc.Col(card_graph, width=6),
        dbc.Col([
            dbc.Row(dbc.CardGroup([card, card])),
            dbc.Row(dbc.CardGroup([card, card])),
            dbc.Row(dbc.CardGroup([card, card])),
            dbc.Row(dbc.CardGroup([card, ])),
            ], width=4)
        ])



])

cards = dbc.CardGroup([card, card])
# app.layout = cards


@app.callback(
    Output("my_bar", "figure"),
    [Input("user_choice", "value")]
)
def update_graph(value):
    fig = px.scatter(df.query("year=={}".format(str(value))), x="gdpPercap", y="lifeExp",
                     size="pop", color="continent", title=str(value),
                     hover_name="country", log_x=True, size_max=60).update_layout(showlegend=True, title_x=0.5)
    return fig


if __name__ == "__main__":
    app.run_server(debug=True, port=5566)
