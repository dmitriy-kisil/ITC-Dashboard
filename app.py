import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

df = pd.read_csv("itctray3.csv", index_col=0)
df1 = df.groupby("author")["counts", "Date"].sum().reset_index()
df1 = df1.sort_values(by="counts", ascending=False).reset_index(drop=True)
df1 = df1.head(10)

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': df1["author"], 'y': df1["counts"], 'type': 'bar', 'name': 'SF'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])


if __name__ == '__main__':
    app.run_server(debug=True)
