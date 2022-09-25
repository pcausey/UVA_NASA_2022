# -*- coding: utf-8 -*-
"""
Created on Fri Nov  5 11:13:34 2021

@author: whetz
"""

import pandas as pd
import h5py
from pyhdf.SD import SD, SDC
import matplotlib.pyplot as plt
import os
import seaborn as sns
import numpy as np
import plotly.graph_objects as go


def get_tracked_parasol(
    file,
    step=20,
    north_tracks=2,
    south_tracks=2,
    lat=34.540852,
    lon=-68.379303,
    buffer=0.001,
):
    """
    Creates a dataframe with all wavelength/polarization states/viewing angles
    from PARASOL files with normalized radiances and degrees of linear polarization.
    Organizes by tracks centered around a point of interest

    Parameters
    ----------
    file : str
        Name of hdf file to be explored.
    step : int, optional
        The number of indices that will be offset for each track. The default is 20.
    north_tracks : int, optional
        Number of tracks that should be explored above the point of interest.
        The default is 2.
    south_tracks : int, optional
        Number of tracks that should be explored below the point of interest.
        The default is 2.
    lat : float, optional
        Latitude of the point of interest to be explored. The default is 34.540852.
    lon : float, optional
        Longitude of the point of interest to be explored. The default is -68.379303.
    buffer : TYPE, optional
        The +/- range that will be used to filter for lat and lon. The default is 0.001.

    Returns
    -------
    Formatted data frame with normalized radiances and degrees of linear polarization
    for all specified tracks surrounding the indicated point of interest within the file.

    """

    rad_list = [
        "Normalized_Radiance_443NP",
        "Normalized_Radiance_490P",
        "Normalized_Radiance_1020NP",
        "Normalized_Radiance_565NP",
        "Normalized_Radiance_670P",
        "Normalized_Radiance_763NP",
        "Normalized_Radiance_765NP",
        "Normalized_Radiance_865P",
        "Normalized_Radiance_910NP",
    ]
    q_fields = ["Q_Stokes_490P", "Q_Stokes_670P", "Q_Stokes_865P"]
    u_fields = ["U_Stokes_490P", "U_Stokes_670P", "U_Stokes_865P"]

    hdf = SD(file, SDC.READ)
    lats = list(hdf.select("Latitude").get())
    lons = list(hdf.select("Longitude").get())

    lat1 = lat - buffer
    lat2 = lat + buffer
    lon1 = lon - buffer
    lon2 = lon + buffer

    df_coord = pd.DataFrame({"lat": lats, "lon": lons})
    index = df_coord.query(
        f"lat > {lat1} & lat < {lat2} & lon > {lon1} & lon < {lon2}"
    ).index[0]

    master_df = pd.DataFrame()

    # Loop through each track based on initial index and specified step
    for i in range(-1 * (south_tracks), north_tracks + 1):
        ind = index + step * i

        # Get the coordinates & time
        lat = pd.DataFrame(hdf.select("Latitude").get()).loc[ind][0]
        lon = pd.DataFrame(hdf.select("Longitude").get()).loc[ind][0]
        time = pd.DataFrame(hdf.select("Time").get()).loc[ind][0]

        # Loop through each radiance
        rad_df = pd.DataFrame()
        for rad in rad_list:
            temp = pd.DataFrame(hdf.select(rad).get()).loc[ind]
            transform = [
                round(i * (10 ** (-4)), 5) if i > -1000 else i for i in list(temp)
            ]

            df_temp = pd.DataFrame(transform).transpose()
            df_temp["Wavelength"] = (rad).replace("Normalized_Radiance_", "")
            rad_df = pd.concat([rad_df, df_temp])
        rad_df = rad_df.reset_index(drop=True)
        rad_cols = [i for i in range(16)]
        rad_cols.append("wavelength")
        rad_df.columns = rad_cols

        # Get q data and add to the dataframe
        q_df = pd.DataFrame()
        for q in q_fields:

            temp = pd.DataFrame(hdf.select(q).get()).loc[ind]
            transform = [
                round(i * (10 ** (-4)), 5) if i > -1000 else i for i in list(temp)
            ]

            df_temp = pd.DataFrame(transform).transpose()

            df_temp["wavelength"] = q.replace("Q_Stokes_", "")
            q_df = pd.concat([q_df, df_temp])
        qcols = [i for i in range(16)]
        qcols.append("wavelength")
        q_df.columns = qcols

        # Get u data and add to the dataframe
        u_df = pd.DataFrame()
        for u in u_fields:

            temp = pd.DataFrame(hdf.select(q).get()).loc[ind]
            transform = [
                round(i * (10 ** (-4)), 5) if i > -1000 else i for i in list(temp)
            ]

            df_temp = pd.DataFrame(transform).transpose()
            df_temp["wavelength"] = u.replace("U_Stokes_", "")
            u_df = pd.concat([u_df, df_temp])
        ucols = [i for i in range(16)]
        ucols.append("wavelength")
        u_df.columns = ucols

        # Melt the matrices
        rad_melt = rad_df.melt(
            id_vars="wavelength",
            value_vars=[i for i in range(16)],
            var_name="angle",
            value_name="norm_rad",
        )

        q_melt = q_df.melt(
            id_vars="wavelength",
            value_vars=[i for i in range(16)],
            var_name="angle",
            value_name="q",
        )
        u_melt = u_df.melt(
            id_vars="wavelength",
            value_vars=[i for i in range(16)],
            var_name="angle",
            value_name="u",
        )

        # join each matrix
        df_merge = pd.merge(
            left=rad_melt, right=q_melt, how="left", on=["wavelength", "angle"]
        )
        df_merge = pd.merge(
            left=df_merge, right=u_melt, how="left", on=["wavelength", "angle"]
        )

        # replace missing data with np.nan
        df_merge.replace(-32768, np.NaN)

        # Calclate Degree of linear polarization
        df_merge["dolp"] = round(
            np.sqrt(df_merge.q ** 2 + df_merge.u ** 2) / df_merge.norm_rad, 3
        )

        # Add traack column
        df_merge["track"] = i

        # Filter for non-missing values
        df_merge = df_merge.query("norm_rad > -1000")

        # Add in coordinate values and time values
        df_merge["lat"] = [lat for i in range(len(df_merge))]
        df_merge["lon"] = [lon for i in range(len(df_merge))]
        df_merge["coord"] = [
            str(round(df_merge.loc[i].lat, 5))
            + ", "
            + str(round(df_merge.loc[i].lon, 5))
            for i in range(len(df_merge))
        ]
        df_merge["time"] = [time for i in range(len(df_merge))]

        # Add to master df and go to next track
        master_df = pd.concat([master_df, df_merge]).reset_index(drop=True)
    return master_df


def plot_tracked_parasol(master_df, np=True):
    """
    Creates visuals for the Normalized radiances and degrees of linear polarization
    for each track generated in get_tracked_parasol

    Parameters
    ----------
    master_df : Pandas DataFrame
        Formatted DataFrame generated from get_tracked_parasol.
    np : bool, optional
        True will result in a plot for non-polarized wavelengths. False will plot
        degree of linear polarization for polarized wavelengths. The default is True.

    Returns
    -------
    None.

    """

    non_pols = ["443NP", "1020NP", "565NP", "763NP", "765NP", "910NP"]

    pols = ["490P", "670P", "865P"]
    df_np = master_df[master_df.wavelength.isin(non_pols)]
    df_p = master_df[master_df.wavelength.isin(pols)]

    if np:
        g = sns.FacetGrid(df_np, col="coord", height=10, aspect=0.75)
        g.map_dataframe(sns.lineplot, x="angle", y="norm_rad", hue="wavelength")
        g.fig.subplots_adjust(top=0.9)
        g.fig.suptitle(
            (
                "Normalized Radiance for Non-Polarized Wavelenghts by Viewing Angle"
            ).upper()
        )
        g.add_legend()
    else:
        g = sns.FacetGrid(df_p, col="coord", height=10, aspect=0.75)
        g.map_dataframe(sns.lineplot, x="angle", y="dolp", hue="wavelength")
        g.fig.subplots_adjust(top=0.9)
        g.fig.suptitle(("Degree of Linear Polarization by Viewing Angle").upper())
        g.add_legend()


def get_activate_aods(f):
    """
    Generates a dataframe from a GRASP L2 files with aerosol products (AOD non-coarse
    and non-fine) for coordinates that lie within the ACTIVATE region.

    Parameters
    ----------
    f : h5py object
        h5py object with the GRASP file you wish to analyze.

    Returns
    -------
    activate : Pandas DataFrame
        DataFrame with aerosol products for the ACTIVATE region.

    """

    aod_fields = [
        i
        for i in list(f["L2-GRASP"].keys())
        if "AOD" in i
        if "AAOD" not in i
        if "Coarse" not in i
        if "Fine" not in i
    ]

    lats = np.array(f["L2-GRASP"]["Latitude"])
    lons = np.array(f["L2-GRASP"]["Longitude"])

    indices = np.where((lats < 39) & (lats > 32) & (lons > -75) & (lons < -70))

    aod_df = pd.DataFrame()
    for aod in aod_fields:
        arr = np.array(f["L2-GRASP"][aod])
        arr_temp = arr[indices[0], indices[1]]
        aod_df = pd.concat([aod_df, pd.DataFrame({aod: arr_temp})], axis=1)
    activate = pd.DataFrame(
        {
            "lat": lats[indices[0], indices[1]],
            "lon": lons[indices[0], indices[1]],
        }
    )
    activate = pd.concat([activate, aod_df], axis=1)

    activate["coord"] = [
        str(round(activate.lat.loc[i], 3)) + ", " + str(round(activate.lon.loc[i], 3))
        for i in range(len(activate))
    ]

    return activate


def plot_GRASP_aod(activate, aod_field):
    """
    Creates a plotly Scattergeo plot showing aerosol optical depth for a single
    AOD field. Color is set to AOD.

    Parameters
    ----------
    activate : Pandas DataFrame
        Data Frame generated from get_activate_aods.
    aod_field : str
        Name of the field you wish to plot.

    Returns
    -------
    None.

    """
    activate = activate[["lat", "lon", "coord", aod_field]].query(
        f"{aod_field} > -1000"
    )
    fig = go.Figure()

    fig.add_trace(
        go.Scattergeo(
            lon=activate["lon"],
            lat=activate["lat"],
            hoverinfo="text",
            text=activate["coord"] + " aod: " + activate[aod_field].astype(str),
            mode="markers",
            marker=dict(size=2, color=activate[aod_field]),
        )
    )

    fig.update_layout(margin=dict(l=1, r=1, t=1, b=1))
    fig.show()
