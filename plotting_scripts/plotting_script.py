import pandas as pd
from pyhdf.SD import SD, SDC
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import plotly.graph_objects as go
import h5py


def build_caltrack_df(filedir, filename, fraction):
    file = filedir + filename
    hdf = SD(file, SDC.READ)

    df_cal = pd.DataFrame({
        'lat': hdf.select('Latitude').get(),
        'lon': hdf.select('Longitude').get(),
        'time': hdf.select('Time').get()
    })

    # Add the coordinate attribute
    df_cal = build_coord_attr(df_cal)

    # Cut down the number of rows returned
    # this will help the plotting script not die
    df_return = return_fraction_of_df(df_cal, fraction)

    return df_return


def build_coord_attr(df):
    df = df.assign(coord=lambda x: str(round(x.lat, 5)) + ', ' + str(round(x.lon, 5)))
    return df


def make_datetime_from_polder_file_name(file_name):
    datetime = file_name.split('_')
    timestamp = datetime[2].split('T')
    date = timestamp[0]
    time = timestamp[1].replace('-', ':')

    return str(date) + ' ' + str(time)


def build_polder_df(filedir, filename, fraction=0.1):
    """
    fraction: decides how many rows to return

    """

    file = filedir + filename

    # Create a file instance
    f = h5py.File(file, 'r')

    df_pol = pd.DataFrame({
        'lat': np.array(f['Geolocation_Fields']['Latitude']).flatten(),
        'lon': np.array(f['Geolocation_Fields']['Longitude']).flatten(),
        'time': pd.Timestamp(make_datetime_from_polder_file_name(filename))
    })

    fill_val = -99999.0

    # Remove 'Fill Value' rows so we only have real data
    df_pol = df_pol.query(f"lat != {fill_val} & lon != {fill_val}")

    # Add the coordinate attribute
    df_pol = build_coord_attr(df_pol)

    # Cut down the number of rows returned
    # this will help the plotting script not die
    df_return = return_fraction_of_df(df_pol, fraction)

    return df_return


def return_fraction_of_df(df, fraction):
    return_fraction = int(1 / fraction)
    df_return = df.iloc[::return_fraction, :]

    return df_return


def build_figures(list_dfs):
    """
    Order your dfs such that the last df is the one you want on top (think layers, first df is lowest layer)
    """
    fig = go.Figure()

    col_list = ['rgb(255, 0, 0)', 'rgb(0, 255, 0)', 'rgb(0, 0, 255)']

    for i in range(len(list_dfs)):
        df = list_dfs[i]
        color = col_list[i]

        fig.add_trace(
            go.Scattergeo(
                lon=df['lon'],
                lat=df['lat'],
                #     hoverinfo = 'text',
                #     text = df['coord'],
                mode='markers',
                marker=dict(
                    size=2,
                    color=color,
                    line=dict(
                        width=3,
                        color='rgba(68, 68, 68, 0)'
                    )
                )
            )
        )

    fig.show()
    fig.update_layout(margin=dict(l=1, r=1, t=1, b=1))

