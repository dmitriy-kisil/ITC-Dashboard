import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import dash_table
from datetime import datetime as dt
import plotly.graph_objs as go
from savetospred import get_config, connect_to_sheet, get_df

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
# df = pd.read_csv("itctray3.csv", index_col=0)

# print("Get config data")
oauth_file, sheet_name, wks_name = get_config()
# print("Connect to Google Spreadsheet")
wks = connect_to_sheet(oauth_file, sheet_name, wks_name)
# print("Get old df from spreadsheet")
df = get_df(wks)
# print(df.head(5))
# df["date3"] = pd.to_datetime(df["Date"], format="%I:%M %p %d/%m/%Y")
# df["date3"] = df["date3"].dt.strftime('%d-%m-%Y')

# del df["date3"]


# df1 = df.groupby("author")["counts", "Date"].sum().reset_index()
# df1 = df1.sort_values(by="counts", ascending=False).reset_index(drop=True)
# df = df1.head(10)
df["date3"] = pd.to_datetime(df["Date"], format="%I:%M %p %d/%m/%Y")
df["date3"] = df["date3"].dt.strftime('%d-%m-%Y')
df["date4"] = pd.to_datetime(df["Date"], format="%I:%M %p %d/%m/%Y")
last_date_string = df["date4"].dt.strftime('%d %B, %Y').tolist()[0]
first_date_string = df["date4"].dt.strftime('%d %B, %Y').tolist()[-1]
month_allowed = df["date4"].dt.strftime('%m-%Y').tolist()[0]
df["date4"] = df["date4"].dt.strftime('%Y-%m-%d')
first_date, last_date = df["date4"].min(), df["date4"].max()
# start_date, end_date = first_date, last_date
# print(first_date, last_date)
# del df["date3"], df["date4"]


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
        n_fixed_rows=1,
        style_table={
            'maxHeight': '600',
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
            {'if': {'column_id': 'title'},
             'width': '10px'},
            {'if': {'column_id': 'sometext'},
             'width': '10px'},
        ],
        editable=True,
        filtering=True,
        sorting=True,
        sorting_type="multi",
        row_selectable="multi",
        row_deletable=True,
        selected_rows=[],
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
        # min_date_allowed=dt(2019, 1, 1),
        # max_date_allowed=dt(2019, 2, 17),
        # initial_visible_month=dt(2019, 2, 17),
        # end_date=dt(2019, 2, 17)
    ),
    html.Div(id='output-container-date-picker-range'),
    html.Div(
            id='table-paging-with-graph-container'
        ),
    html.Div(id='new-bar-chart'),
    html.Div(id='new-bar-chart-2'),
    html.Div(id='Pie'),
    html.Div(id='Pie2'),

])

'''
@app.callback(
    Output(component_id='datatable-interactivity', component_property='data'),
    [Input(component_id='select-date', component_property='value')]
)
def update_output_div(choosed_date):
    if choosed_date is '':
        dff = df
    else:
        dff = df[df["date3"].isin([choosed_date])]
    return dff.to_dict("rows")
'''


@app.callback(
    dash.dependencies.Output('table-paging-with-graph-container', "children"),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_graph(start_date, end_date):
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        # start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        # end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # print("111")
        # dff = df[df["date3"].isin([choosed_date])]
    else:
        dff = df
        # print("222")
    df1 = dff.groupby("author")["counts", "Date"].sum().reset_index()
    df1 = df1.sort_values(by="counts", ascending=False).reset_index(drop=True)
    # df1 = df1.head(10)
    dff = df1
    return html.Div(
            dcc.Graph(
                id='barchart',
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
                        # "xaxis": {"automargin": True},
                        # "yaxis": {"automargin": True},
                        # "height": 250,
                        # "width": "400"
                        # "margin": {"t": 10, "l": 10, "r": 10},
                    },
                },
            )
    )


@app.callback(
    dash.dependencies.Output('new-bar-chart', "children"),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_graph(start_date, end_date):
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        # start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        # end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # print("111")
        # dff = df[df["date3"].isin([choosed_date])]
    else:
        dff = df
        # print("222")
    df1 = dff
    df1["hour"] = df1["Date"].apply(
                lambda x: dt.strptime(x, "%I:%M %p %d/%m/%Y").strftime("%H"))
    # df.sort_values(by="hour", ascending=True).reset_index(drop=True)

    # print(set(df1["hour"].tolist()))

    df1 = df1.groupby("hour")["author", "counts"].sum().reset_index()
    df1.sort_values(by="hour", ascending=True).reset_index(drop=True)
    # df1 = df1.head(10)
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
                        # "xaxis": {"automargin": True},
                        # "yaxis": {"automargin": True},
                        # "height": 250,
                        # "width": "400"
                        # "margin": {"t": 10, "l": 10, "r": 10},
                    },
                },
            )
    )


@app.callback(
    dash.dependencies.Output('new-bar-chart-2', "children"),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_graph(start_date, end_date):
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        # start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        # end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # print("111")
        # dff = df[df["date3"].isin([choosed_date])]
    else:
        dff = df
        # print("222")
    df1 = dff
    df1["hour"] = df1["Date"].apply(
                lambda x: dt.strptime(x, "%I:%M %p %d/%m/%Y").strftime("%H"))
    # df.sort_values(by="hour", ascending=True).reset_index(drop=True)

    # print(set(df1["hour"].tolist()))

    df1 = df1.groupby("hour")["author", "title"].count().reset_index()
    df1.sort_values(by="hour", ascending=True).reset_index(drop=True)
    # df1 = df1.head(10)
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
                        # "xaxis": {"automargin": True},
                        # "yaxis": {"automargin": True},
                        # "height": 250,
                        # "width": "400"
                        # "margin": {"t": 10, "l": 10, "r": 10},
                    },
                },
            )
    )


@app.callback(
    Output('Pie', "children"),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_graph2(start_date, end_date):
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        # start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        # end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # dff = df[df["date3"].isin([choosed_date])]
    else:
        dff = df
    return html.Div(
            dcc.Graph(
                id='piechart',
                figure={
                    "data": [
                        go.Pie(labels=dff['category'],
                               values=dff['counts'])
                    ],
                    "layout": {
                        "title": "Pie chart category by counts",
                        "xaxis": {"title": "Category"},
                        "yaxis": {"title": "Counts"}
                        # "xaxis": {"automargin": True},
                        # "yaxis": {"automargin": True},
                        # "height": 250,
                        # "width": "400"
                        # "margin": {"t": 10, "l": 10, "r": 10},
                    },
                },
            )
    )


@app.callback(
    Output('Pie2', "children"),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_graph3(start_date, end_date):
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        # start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        # end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        # string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # dff = df[df["date3"].isin([choosed_date])]
    else:
        dff = df
    '''
    if choosed_date is '':
        dff = df
    else:
        dff = df[df["date3"].isin([choosed_date])]
    '''
    return html.Div(
            dcc.Graph(
                id='piechart2',
                figure={
                    "data": [
                        go.Pie(labels=dff['author'],
                               values=dff['counts'])
                    ],
                    "layout": {
                        "title": "Pie chart authors by counts",
                        "xaxis": {"title": "Authors"},
                        "yaxis": {"title": "Counts"}
                        # "xaxis": {"automargin": True},
                        # "yaxis": {"automargin": True},
                        # "height": 250,
                        # "width": "400"
                        # "margin": {"t": 10, "l": 10, "r": 10},
                    },
                },
            )
    )


@app.callback(
    Output('output-container-date-picker-range', 'children'),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_output(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime('%d %B, %Y')
        # start_date_string2 = start_date.strftime('%d-%m-%Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime('%d %B, %Y')
        # end_date_string2 = end_date.strftime('%d-%m-%Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        return 'Select a date to see it displayed here'
    else:
        return string_prefix


@app.callback(
    Output(component_id='datatable-interactivity', component_property='data'),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_output2(start_date, end_date):
    string_prefix = 'You have selected: '
    dff = df
    if start_date and end_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime('%B %d, %Y')
        # start_date_string2 = start_date.strftime('%m-%d-%Y')
        start_date_string3 = start_date.strftime('%Y-%m-%d')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime('%B %d, %Y')
        # end_date_string2 = end_date.strftime('%m-%d-%Y')
        end_date_string3 = end_date.strftime('%Y-%m-%d')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
        # mask = (dff['date3'] > start_date_string2) & (dff['date3'] <= end_date_string2)
        # print(start_date, end_date)
        # dff = dff.loc[mask]
        # dff = dff[dff["date3"].isin(pd.date_range(start_date_string3, end_date_string3))]
        dff = df[df['date4'].between(start_date_string3, end_date_string3)]
        # dff = df[df["date3"].isin([choosed_date])]
        return dff.to_dict("rows")
    else:
        dff = df
        return dff.to_dict("rows")


if __name__ == '__main__':
    app.run_server(debug=True)
