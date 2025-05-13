# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.
import argparse
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import time
from dash import Dash, html, dcc, Input, Output, State
from dash.exceptions import PreventUpdate
from lib.data_processing import DataProcessing
from lib import data_loader
import dash_bootstrap_components as dbc

# ------------------Retrieve Data---------------------
while True:
    try:
        start_date = datetime(2025, 1, 1, 0, 0)  # September 1, 2022, 00:00:00 UTC
        end_date = datetime(2025, 1, 1, 23, 59, 59)     # September 2, 2022, 00:00:00 UTC
        
        # User inputs the desired hours (for example, from 10 AM to 3 PM)
        start_hour = int(input("Enter the start hour (0-23): "))  # E.g., 10
        end_hour = int(input("Enter the end hour (0-23): "))  # E.g., 15

        # Create start and end times with the user-provided hours
        start_time = datetime(2025, 1, 1, start_hour, 0, 0)  # E.g., 10:00:00 AM
        end_time = datetime(2025, 1, 1, end_hour, 59, 59)  # E.g., 3:59:59 PM

        # Convert to milliseconds since epoch
        start_timestamp = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)

        print(f"Start Timestamp: {start_timestamp}")
        print(f"End Timestamp: {end_timestamp}")

        # time interval in minutes
        time_interval = int(input("Enter time interval (in minutes):"))
        if time_interval < 1:
            raise ValueError

        # data folder contains data for September 1, 2022
        dir_name = './data/'
        raw_data = data_loader.get_data(dir_name, start_timestamp, end_timestamp)

        status_list = []
        timestamp_list = []

        if len(raw_data) != 0:
            data_process = DataProcessing(raw_data, time_interval, start_timestamp, end_timestamp)
            df = data_process.get_aggregated_data
            status_list = df['status_code'].unique().tolist()
            timestamp_list = df['ts'].unique().tolist()
        else:
            print('no data found')
            break

    except ValueError as e:
        print(e)
    else:
        break

# ------------------DASH LAYOUT----------------------
app = Dash(title='Heatmaps', external_stylesheets=[dbc.themes.CYBORG])
server = app.server
app.config.suppress_callback_exceptions = True

controls = dbc.Card([
    html.Div([
        dbc.Label("Filter Values Greater than"),
        dbc.Input(id="input1", type="number", value=0),
    ], style={"display": "inline-block", "padding-right": "10px"}),
    html.Div([
        dbc.Label("Filter Values Less than"),
        dbc.Input(id="input2", type="number", value=0),
    ], style={"display": "inline-block"}),
    html.Div([
        dbc.Label("Select a Graph Type"),
        dcc.Dropdown(id="graph_type_dropdown",
                     options=[
                         {'label': 'Data Center vs Services', 'value': 'datacenter_services'},
                         {'label': 'Caller-Callee Pairs', 'value': 'caller_callee_pairs'},
                     ],
                     value='datacenter_services',
                     multi=False,
                     clearable=False,
                     style={"width": "60%"}),
    ]),
    html.Div([
        dbc.Label("Select Metrics"),
        dcc.Dropdown(id="stats_dropdown",
                     options=[
                         {'label': 'Call Volume', 'value': 'count'},
                         {'label': 'Average Response Time', 'value': 'avg'},
                         {'label': 'Max Response Time', 'value': 'max'},
                         {'label': 'Min Response Time', 'value': 'min'},
                         {'label': 'Standard Deviation of Response Times', 'value': 'std'},
                     ],
                     value='count',
                     multi=False,
                     clearable=False,
                     style={"width": "60%"}),
    ]),
    html.Div([
        html.Div([
            dbc.Label("Select Status Code(s)"),
            dcc.Dropdown(id="status_dropdown",
                         options=[{"label": i, "value": i} for i in sorted(status_list)],
                         value=[],
                         multi=True,
                         clearable=False,
                         style={"width": "60%"})
        ]),
        html.Div([
            dcc.Checklist(
                id="select-all",
                options=[{'label': 'Select All', 'value': 1}],
                value=[],
                style={"width": "60%", "padding-top": "10px"},
                inputStyle={"margin-right": "10px"}
            ),
        ], id='checklist-container')
    ]),
    html.Div([
        html.Div([
            dbc.Label("Select a Value Type"),
            dcc.RadioItems(id="value_type_radiobutton",
                           options={},
                           value="absolute_value",
                           inline=True,
                           style={"width": "60%"},
                           inputStyle={"margin-right": "10px"}
                           )
        ]),
        html.Div([
            dbc.Label("Select a Range Type"),
            dcc.RadioItems(id="range_radiobutton",
                           options=[
                               {'label': 'Constant Range', 'value': 'constant_range'},
                               {'label': 'Variable Range', 'value': 'variable_range'}],
                           value='constant_range',
                           inline=True,
                           style={"width": "60%"},
                           inputStyle={"margin-right": "10px"}
                           ),
        ]),
    ])
], body=True,
)

app.layout = dbc.Container(
    [
        html.H4(children='CloudHeatMap', style={'textAlign': 'center'}),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(controls, md=12),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Loading([
                        dcc.Graph(
                            id='graph',
                            config={'displayModeBar': True, 'toImageButtonOptions': {'height': None, 'width': None}},
                            className="m-4"
                        )
                    ]),
                    md=12
                ),
            ],
            align="center",
        ),
        # dbc.Row([
        #     html.A(
        #         html.Button("Download as HTML"),
        #         id="download",
        #         href="",
        #         download="plotly_graph.html"
        #     )
        # ])
    ], fluid=True,
)


# function for creating a dataframe with row name and col name and empty value
def create_master_dataframe(list1, list2):
    row_name = sorted(set(list1), reverse=True)
    col_name = sorted(set(list2), reverse=True)
    master_df = pd.DataFrame(index=row_name, columns=col_name)
    return master_df


# function for filtering dataframe
def filter_dataframe(input_df, status_code_list, select_all, value_type, input1, input2, aggregation_type):
    # check if the dataframe contains the aggregation_type else return an empty dataframe
    if input_df.columns.str.contains(aggregation_type).any():
        # accumulated_df contains the total value for all status codes
        accumulated_df = input_df.groupby(['row', 'col'])
        accumulated_df = accumulated_df.apply(data_process.aggregation_functions).reset_index()

        accumulated_df.rename(columns={aggregation_type: 'total'}, inplace=True)

        accumulated_df = accumulated_df.drop(accumulated_df.columns.difference(['row', 'col', 'status_code', 'total']),
                                             axis=1)

        # filtering the dataframe based on status code
        if len(status_code_list) == 0 or len(select_all) > 0:  # if nothing selected from the dropdown show total values
            filtered_df = accumulated_df.copy()
            filtered_df.rename(columns={'total': 'result'}, inplace=True)

        else:  # else generate heatmaps for the selected status codes from the status_code_list
            filtered_df = input_df.copy()
            filtered_df = filtered_df[filtered_df['status_code'].isin(status_code_list)]

            # Aggregating the filtered dataframe by selected status codes
            filtered_df = filtered_df.groupby(['row', 'col'])
            filtered_df = filtered_df.apply(data_process.aggregation_functions).reset_index()

            filtered_df = filtered_df.drop(
                filtered_df.columns.difference(['row', 'col', 'status_code', aggregation_type]), axis=1)

            if value_type == "percentage_value":
                filtered_df = filtered_df.merge(accumulated_df[['row', 'col', 'total']],
                                                on=['row', 'col'],
                                                how='inner')
                filtered_df['result'] = (filtered_df[aggregation_type] / filtered_df['total']) * 100
                filtered_df.drop(['total'], axis=1, inplace=True)
            else:
                filtered_df.rename(columns={aggregation_type: 'result'}, inplace=True)

        # Fixing the range based on the filtered dataframe
        z_min = 0
        z_max = filtered_df['result'].max()

        # Filtering the dataframe based on text inputs
        if input1 or input2:
            if input1 > 0 and input2 > 0:  # both filters given
                filtered_df = filtered_df[(filtered_df['result'] >= input1) & (filtered_df['result'] <= input2)]
                z_min = input1
                z_max = input2
            else:  # one of them given
                if input1 > 0:
                    filtered_df = filtered_df[filtered_df['result'] >= input1]
                    z_min = input1
                elif input2 > 0:
                    filtered_df = filtered_df[filtered_df['result'] <= input2]
                    z_max = input2

        return filtered_df, z_min, z_max

    else:
        empty_df = pd.DataFrame()
        return empty_df, 0, 0


# callback for status dropdown
@app.callback(
    Output('status_dropdown', 'value'),
    [Input('select-all', 'value')],
    [State('status_dropdown', 'options')])
def select_all_dropdowns(select_all, options):
    if len(select_all) > 0:
        return [i['value'] for i in options]
    else:
        return []


# callback for selecting all status codes
@app.callback(
    Output('checklist-container', 'children'),
    [Input('status_dropdown', 'value')],
    [State('status_dropdown', 'options'),
     State('select-all', 'value')])
def update_checklist(chosenValues, availableChoices, select_all):
    if len(chosenValues) < len(availableChoices) and len(select_all) == 0:
        raise PreventUpdate()
    if len(chosenValues) == len(availableChoices) and len(select_all) == 1:
        raise PreventUpdate()
    if len(chosenValues) < len(availableChoices) and len(select_all) == 1:
        return dcc.Checklist(id='select-all',
                             options=[{'label': 'Select All', 'value': 1}], value=[1])
    else:
        return dcc.Checklist(id='select-all',
                             options=[{'label': 'Select All', 'value': 1}], value=[1])


# callback for value type selection
@app.callback(
    Output("value_type_radiobutton", "options"),
    [Input("status_dropdown", "value"),
     Input("stats_dropdown", "value")],
    [State('select-all', 'value')]

)
def update_radiobutton(selectedStatusCodes, selectedStatsType, selectedAll):
    if not selectedStatusCodes or len(selectedAll) > 0 or selectedStatsType != 'count':
        return [
            {"label": "Value", "value": "absolute_value"},
            {"label": "Percentage", "value": "percentage_value", "disabled": True},
        ]
    else:
        return [
            {"label": "Value", "value": "absolute_value"},
            {"label": "Percentage", "value": "percentage_value"},
        ]


# main function
@app.callback(
    Output('graph', 'figure'),
    [
        Input('input1', 'value'),
        Input('input2', 'value'),
        Input('status_dropdown', 'value'),
        Input('value_type_radiobutton', 'value'),
        Input('range_radiobutton', 'value'),
        Input('stats_dropdown', 'value'),
        Input('graph_type_dropdown', 'value')
    ],
    [
        State('select-all', 'value'),
        State('stats_dropdown', 'options')
    ]
)
def update_figure(input1, input2, status_code_list, value_type, range_type, aggregation_type, graph_type, select_all,
                  agg_selection):
    
    
    # filter only required graph_type and discard others
    plot_df = df[df['type'] == graph_type]

    # print(graph_type)
    # print(plot_df)
    frames = []
    range_values = []
    blank_fig = go.Figure(
        data=[],
        layout=go.Layout(
            autosize=True,
            height=None,
            width=None,
            yaxis={'showgrid': False},
            xaxis={'showgrid': False},
        )
    )

    # For single selection in the dropdown, the value is str.
    # For multi selection the value is a list.
    if type(status_code_list) == str:
        status_code_list = [status_code_list]

    # x-axis and y-axis title
    yaxis_name = graph_type.split("_")[0].upper()
    xaxis_name = graph_type.split("_")[1].upper()

    aggregation_label = [x['label'] for x in agg_selection if x['value'] == aggregation_type]
    title_x = aggregation_label[0]

    if len(status_code_list) == 0 or len(select_all) > 0:
        title_x += "(All Kinds)"
    else:
        title_x += "<br>(Status code(s) " + str(status_code_list) + ") in " + \
                   value_type.split("_")[0] + " " + value_type.split("_")[1]

    # master_df is the structure of the graph (rows and columns will be fixed)
    master_df = create_master_dataframe(plot_df['row'].tolist(), plot_df['col'].tolist())

    aggregated_df, z_min1, z_max1 = filter_dataframe(plot_df, status_code_list, select_all, value_type,
                                                     input1, input2, aggregation_type)

    if aggregated_df.empty:
        return "Metric not found for the given data", blank_fig
    else:
        aggregated_df = aggregated_df.pivot(index='row', columns='col', values='result')
        master_df.update(aggregated_df, overwrite=True)

        frames.append(
            go.Frame(
                name='Aggregated View',
                data=[
                    go.Heatmap(z=master_df,
                               x=master_df.columns,
                               y=master_df.index,
                               zmin=z_min1,
                               zmax=z_max1)
                ]
            )
        )

        # Heatmap Animation.
        # Creating multiple dataframes from plot_df for each timestamp.
        for time_frame in sorted(timestamp_list):
            temp_df = plot_df.copy()
            temp_df = temp_df[temp_df['ts'] == time_frame]

            filtered_df, z_min, z_max = filter_dataframe(temp_df, status_code_list, select_all, value_type,
                                                         input1, input2, aggregation_type)

            range_values.append(z_max)

            # Copying the values from the filtered df and putting it back to the master structure
            # so that the frame remains the same
            filtered_df = filtered_df.pivot(index='row', columns='col', values='result')
            master_df.update(filtered_df, overwrite=True)

            # Appending the individual frames to the master frames
            if master_df is not None:
                frames.append(
                    go.Frame(
                        name=time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(int(time_frame) / 1000)),
                        data=[
                            go.Heatmap(z=master_df,
                                       x=master_df.columns,
                                       y=master_df.index,
                                       zmin=z_min,
                                       zmax=z_max)
                        ]
                    )
                )
            else:
                raise PreventUpdate

        # if constant range is selected, change the zmax of the frames
        # to the max of range_values (list of each frame's max value)
        if range_type == 'constant_range':
            for i, f in enumerate(frames):
                if i > 0:  # skipping the first frame as it's the aggregated view
                    f['data'][0]['zmax'] = max(range_values)

        # Figure Layout
        fig = go.Figure(
            data=frames[0].data,
            frames=frames,
            layout=go.Layout(
                dragmode='pan',
                autosize=True,
                height=None,
                width=None,
                yaxis={"title": yaxis_name, "dtick": 1},
                xaxis={"title": xaxis_name, "tickangle": -60, "side": 'top', "dtick": 1},
                legend=dict(
                    itemclick="toggleothers",  # Click behavior for legend items
                    itemdoubleclick="toggle"
                ),
                title={
                    'text': title_x,
                    'y': 0.98,  # Move the title a bit higher to avoid overlap
                    'x': 0.2,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'pad': {'b': 30}  # Add bottom padding to the title
                },
                margin=dict(
                    t=100  # Add some top margin to give more space between the title and plot
                )
            )
        )
        # play-pause config
        fig.update_layout(
            updatemenus=[{
                'buttons': [
                    {
                        'args': [None, {'frame': {'duration': 500, 'redraw': True},
                                        'transition': {'duration': 500, 'easing': 'quadratic-in-out'}}],
                        'label': 'Play',
                        'method': 'animate'
                    },
                    {
                        'args': [[None], {'frame': {'duration': 0, 'redraw': False},
                                          'mode': 'immediate',
                                          'transition': {'duration': 0}}],
                        'label': 'Pause',
                        'method': 'animate'
                    }
                ],
                'direction': 'left',
                'pad': {'r': 10, 't': 100},
                'showactive': False,
                'type': 'buttons',
                'x': 0.1,
                'xanchor': 'right',
                'y': 0,
                'yanchor': 'top'
            }],
            sliders=[
                {
                    "steps": [{"args": [[f.name],
                                        {
                                            "frame": {"duration": 0, "redraw": True},
                                            "mode": "immediate",
                                        },
                                        ],
                               "label": f.name, "method": "animate",
                               }
                              for f in frames],
                }
            ]
        )

        return fig


if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Run the Dash app.')
    parser.add_argument('--host', default=None, help='Host address to serve the app on (default: None)')
    args = parser.parse_args()

    # Run the server with the specified host
    app.run_server(debug=False, host=args.host)
