import dash
import dash_html_components as html

# Create the Dash application
app = dash.Dash(__name__)

# Define the layout
app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H2("Left Header", style={'text-align': 'left'}),
            ],
            style={'width': '50%', 'display': 'inline-block'}
        ),
        html.Div(
            children=[
                html.H2("Right Header", style={'text-align': 'right'}),
            ],
            style={'width': '50%', 'display': 'inline-block'}
        )
    ]
)

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
