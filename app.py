import dash
import datetime
from datetime import datetime as dt
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import plotly.graph_objs as go
import psycopg2
from itcfinally2 import create_conn, sort_by_dates, close_conn, prep_dashboard, check_table

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

print("Connnect to DB")
conn, cursor = create_conn()
print("Get data from DB")
(last_date_string, first_date_string,
 month_allowed, first_date, last_date, df) = prep_dashboard(conn, cursor)
print(last_date_string, first_date_string, month_allowed, first_date, last_date)
# df = sort_by_dates(conn, cursor)
print("Close connection to DB")
close_conn(conn, cursor)

# n_fixed_rows=1,
'''
        editable=True,
        filtering=True,
        sorting=True,
        sorting_type="multi",
        row_selectable="multi",
        row_deletable=True,
        selected_rows=[],
'''

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div([
    html.H1(children='ITC Dashboard',
            style={'textAlign': 'center'}),

    html.Div(children='''
        Build in Dash.
    ''', style={'textAlign': 'center'}),
    dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i, "deletable": True} for i in df.columns
        ],
        data=df.to_dict("rows"),

        pagination_settings={
            'current_page': 0,
            'page_size': 18
        },
        pagination_mode='be',
        style_table={
            # 'maxHeight': '600',
            'maxWidth': '1900',
            'overflowY': 'scroll'
        },
        style_cell={
            # all three widths are needed
            'minWidth': '120px', 'width': '120px', 'maxWidth': '120px',
            'whiteSpace': 'no-wrap',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
        },
        style_cell_conditional=[
            {'if': {'row_index': 'odd'},
             'backgroundColor': 'rgb(248, 248, 248)'
             },
            # {'if': {'column_id': 'title'},
            # 'width': '10px'},
            # {'if': {'column_id': 'sometext'},
            # 'width': '10px'},
        ],
    ),
    html.Div(children='Data available from {} to {}'.format(
        first_date_string, last_date_string)),
    # dcc.Input(id='select-date', value='', type='text',
    #           placeholder='Enter a date', style={'textAlign': 'center'}),
    dcc.DatePickerRange(
        id='my-date-picker-range',
        first_day_of_week=1,
        min_date_allowed=first_date,
        max_date_allowed=last_date,
        initial_visible_month=last_date,
    ),
    html.Div(id='output-container-date-picker-range'),
    html.Div(id='bar-chart-1'),
    html.Div(id='bar-chart-2'),
    html.Div(id='bar-chart-3'),
    html.Div(id='Pie-1'),
    html.Div(id='Pie-2'),
    html.Div(id='Scatter')

])


@app.callback(
    Output('bar-chart-1', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_barchart1(start_date, end_date):
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    df1 = dff.groupby("author")["counts", "date4"].sum().reset_index()
    dff = df1.sort_values(by="counts", ascending=False).reset_index(drop=True)
    return html.Div(
        dcc.Graph(
            id='barchart1',
            figure={
                "data": [
                    {
                        "x": dff["author"],
                        "y": dff["counts"],
                        "type": "bar",
                        "marker": {"color": "#0074D9"},
                    }
                ],
                "layout": {
                    "title": "Bar char author by counts",
                    "xaxis": {"title": "Authors"},
                    "yaxis": {"title": "Counts"}
                },
            },
        )
    )


@app.callback(
    Output('bar-chart-2', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_barchart2(start_date, end_date):
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    df1 = dff
    df1["hour"] = df1["date2"].apply(
        lambda x: dt.strptime(x, "%I:%M %p %d/%m/%Y").strftime("%H"))

    df1 = df1.groupby("hour")["author", "counts"].sum().reset_index()
    df1.sort_values(by="hour", ascending=True).reset_index(drop=True)
    dff = df1
    return html.Div(
        dcc.Graph(
            id='barchart2',
            figure={
                "data": [
                    {
                        "x": dff["hour"],
                        "y": dff["counts"],
                        "type": "bar",
                        "marker": {"color": "#0074D9"},
                    }
                ],
                "layout": {
                    "title": "Bar chart counts hourly",
                    "xaxis": {"title": "Hour",
                              # Choose what you want to see on xaxis! In this case list
                              "tickvals": dff["hour"],
                              "ticktext": dff["hour"]
                              },
                    "yaxis": {"title": "Counts"}
                },
            },
        )
    )


@app.callback(
    Output('bar-chart-3', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_barchart3(start_date, end_date):
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    dff = df
    df1 = dff
    df1["hour"] = df1["date2"].apply(
        lambda x: dt.strptime(x, "%I:%M %p %d/%m/%Y").strftime("%H"))
    df1 = df1.groupby("hour")["author", "title"].count().reset_index()
    df1.sort_values(by="hour", ascending=True).reset_index(drop=True)
    dff = df1
    return html.Div(
        dcc.Graph(
            id='barchart3',
            figure={
                "data": [
                    {
                        "x": dff["hour"],
                        "y": dff["title"],
                        "type": "bar",
                        "marker": {"color": "#0074D9"},
                    }
                ],
                "layout": {
                    "title": "Bar chart topics hourly",
                    "xaxis": {"title": "Hour",
                              # Choose what you want to see on xaxis! In this case list
                              "tickvals": dff["hour"],
                              "ticktext": dff["hour"]
                              },
                    "yaxis": {"title": "Number of topics"}
                },
            },
        )
    )


@app.callback(
    Output('Pie-1', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_pie1(start_date, end_date):
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    df1 = df.groupby("category")["date4", "title"].count().reset_index()
    df2 = df.groupby("category")["date4", "counts"].sum().reset_index()

    df3 = pd.merge(df1, df2)
    df3["avg"] = df3["counts"] / df3["title"]
    dff = df3
    return html.Div(
        dcc.Graph(
            id='piechart',
            figure={
                "data": [
                    go.Pie(labels=dff['category'],
                           values=dff['avg'])
                ],
                "layout": {
                    "title": "Pie chart category by counts",
                    "xaxis": {"title": "Category"},
                    "yaxis": {"title": "Counts"}
                },
            },
        )
    )


@app.callback(
    Output('Pie-2', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_graph3(start_date, end_date):
    global df
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    df = dff
    df1 = df.groupby("author")["date4", "title"].count().reset_index()
    df2 = df.groupby("author")["date4", "counts"].sum().reset_index()

    df3 = pd.merge(df1, df2)
    df3["avg"] = df3["counts"] / df3["title"]
    dff = df3
    return html.Div(
        dcc.Graph(
            id='piechart2',
            figure={
                "data": [
                    go.Pie(labels=dff['author'],
                           values=dff['avg'])
                ],
                "layout": {
                    "title": "Pie chart authors by counts",
                    "xaxis": {"title": "Authors"},
                    "yaxis": {"title": "Counts"}
                },
            },
        )
    )


@app.callback(
    Output('Scatter', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_scatter(start_date, end_date):
    global df
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    df0 = dff
    df0['date4'] = pd.to_datetime(df0['date2'], format="%I:%M %p %d/%m/%Y")
    df0['date4'] = (
            df0['date4']
            .apply(lambda x: dt.strftime(x, '%Y-%m-%d'))
            )
    df1 = df0.groupby("date4")["author", "title"].count().reset_index()
    df2 = df0.groupby("date4")["author", "counts"].sum().reset_index()

    df3 = pd.merge(df1, df2)
    df3["avg"] = df3["counts"] / df3["title"]
    dff = df3
    return html.Div(
        dcc.Graph(
            id='scatterchart',
            figure={
                "data": [
                    {
                        "x": dff["date4"],
                        "y": dff["avg"],
                        "type": "scatter",
                        "marker": {"color": "#0074D9"},
                    }
                ],
                "layout": {
                    "title": "Scatter char counts to topic ratio",
                    "xaxis": {"title": "Date",
                              # "range": [min(dff["date4"]),
                              #           max(dff["date4"])]
                              },
                    "yaxis": {"title": "Counts on topic"}
                },
            },
        )
    )


@app.callback(
    Output('output-container-date-picker-range', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_output_container(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime('%d %B, %Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime('%d %B, %Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        return 'Select a date to see it displayed here'
    else:
        return string_prefix


@app.callback(
    Output('datatable-interactivity', 'data'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date'),
     Input('datatable-interactivity', 'pagination_settings')])
def update_data_table(start_date, end_date, pagination_settings):
    if start_date is not None:
        dff = df.loc[df['date4'].isin([start_date])]
    if end_date is not None:
        dff = df.loc[df['date4'].isin([end_date])]
    if start_date and end_date is not None:
        dff = df[df['date4'].between(start_date, end_date)]
    else:
        dff = df
    # return dff.to_dict("rows")
    return dff.iloc[
        pagination_settings['current_page'] * pagination_settings['page_size']:
        (pagination_settings['current_page'] + 1) *
        pagination_settings['page_size']
    ].to_dict('rows')


if __name__ == '__main__':
    app.run_server(debug=True)
