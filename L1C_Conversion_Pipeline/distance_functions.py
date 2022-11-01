# -*- coding: utf-8 -*-
"""
Created on Sun Aug 28 12:02:55 2022

@author: xxpit
"""

import numpy as np
import pandas as pd


def calc_spherical_distance(lat1, lon1, 
                            lat_compare, lon_compare, 
                            loc_units='degrees',
                            dist_units='km',
                            dist_type='great_circle',
                            verbose=True):
    
    """
    **SUMMARY:**
    Calculates the distances between a point of interest and comparison 
        point(s). Identifies the minimum calculated distance and reports
        the pairing and index of the minimum comparison point.
        
    **INPUTS:**
    lat1 (float)         - latitude of interest point
    lon1 (float)         - longitude of interest point
    lat_compare (float or list) - latitude(s) of comparison point(s)
    lon_compare (float or list) - longitude(s) of comparison point(s)
    Loc_units (str)      - units of (lat, lon) pairs (degrees)
    dist_units (str)     - units of preferred distance (km or mi)
    dist_type (str)      - type of distance to calculate (great_circle or haversine)
    verbose (bool)    - report minimum of distances with index
    
    **OUTPUTS:**
    distances (float or list) - calculated distances
    """
    
    # ###################
    # CHECK LOCATION UNITS
    # ###################
    # Check units of input lats/lons and convert to radians if degrees
    lat1_old, lon1_old, lat_compare_old, lon_compare_old = lat1, lon1, lat_compare, lon_compare
    
    if loc_units == 'degrees':
        lat1 = np.radians(lat1)
        lon1 = np.radians(lon1)
        lat_compare = np.radians(lat_compare)
        lon_compare = np.radians(lon_compare)
        
    else:
        raise NotImplementedError("""'loc_units' other than 'degrees' have not
                                  been implemented.""")
      
    # ###################
    # CHECK DISTANCE UNTIS
    # ###################
    # Check distance units and set the radius of the earth to km or mi                       
    if dist_units == 'km':
        radius = 6371
    elif dist_units == 'mi':
        radius = 3958.756
    else:
        raise ValueError("Please ensure 'dist_units' is either 'km' or 'mi'")
        
    # ###################
    # CALCULATE DISTANCES
    # ###################
    # Calculate the specified distance using various methods
    # Utilizes numpy to broadcast input lat1/lon1 values to work with both
    # singular comparisons and list comparisons
    if dist_type == 'great_circle':
        distances = radius * (
            np.arccos(
                np.sin(lat1) * np.sin(lat_compare)
                + np.cos(lat1) * np.cos(lat_compare) * np.cos(lon1 - lon_compare)
            )
        )
        
    elif dist_type == 'haversine':
        dlon = lon_compare - lon1
        dlat = lat_compare - lat1
        a = (
                np.sin(dlat / 2) ** 2
                + np.cos(lat1) * np.cos(lat_compare) * np.sin(dlon / 2) ** 2
        )
        distances = 2 * radius * np.arcsin(np.sqrt(a))
    
    else:
        raise NotImplementedError("""Distances other than 'great_circle' or 
                                  'haversine' have not been implemented.""")
                                                
    ####################
    # OUTPUT RESULTS
    #################### 
    index_min = np.argmin(distances)
    # index_min = distances.idxmin()
    
    # Non verbose results
    if len(lat_compare) == 1:
        report_lat = lat_compare_old
        report_lon = lon_compare_old
    else:
        report_lat = lat_compare_old.iloc[index_min]
        report_lon = lon_compare_old.iloc[index_min]

    # Output a verbose description of minimum distance match
    if verbose and type(lat_compare) == int:
        print("""The minimum {dist_type} distance for ({lat1}, {lon1})
               corresponds to the point ({min_dist_lat}, {min_dist_lon}) 
               at index {index_min}""".format(dist_type=dist_type,
                                              lat1=lat1_old,
                                              lon1=lon1_old,
                                              min_dist_lat=report_lat,
                                              min_dist_lon=report_lon,
                                              index_min=index_min))
            
    elif verbose:
        print("""The minimum {dist_type} distance for ({lat1}, {lon1})
               corresponds to the point ({min_dist_lat}, {min_dist_lon}) 
               at index {index_min}""".format(dist_type=dist_type,
                                              lat1=np.degrees(lat1),
                                              lon1=np.degrees(lon1),
                                              min_dist_lat=report_lat,
                                              min_dist_lon=report_lon,
                                              index_min=index_min))

    return distances, index_min, report_lat, report_lon


def run_epa_data_lookup(d1_lat, d1_lon, epa_df, fill_value):
    from L1C_Conversion_Pipeline.EPA_Pipeline.epa_file import EPA_LATITUDE, EPA_LONGITUDE

    try:
        # TE CHANGES: FILTER EPA DATA
        lat_mask = (epa_df[EPA_LATITUDE] >= d1_lat - 1) & (epa_df[EPA_LATITUDE] <= d1_lat + 1)
        lon_mask = (epa_df[EPA_LONGITUDE] >= d1_lon - 1) & (epa_df[EPA_LONGITUDE] <= d1_lon + 1)
        # We're pre-filtering epa_data on date before passing into this function
        # date_mask = (epa_data['Date Local'] == d1_date)

        # If no epa data meets the lat/long/date criteria, return fill values
        epa_df_tmp = epa_df[lat_mask & lon_mask]  # & date_mask

        # Could change to have as fill value
        if len(epa_df_tmp) == 0:
            return fill_value
        # Else: run the lookup
        else:
            dist, index_min, min_lat, min_lon = \
                calc_spherical_distance(
                    d1_lat, d1_lon,
                    epa_df_tmp[EPA_LATITUDE], epa_df_tmp[EPA_LONGITUDE],
                    verbose=False
                )

            # subset data to only contain closest point data
            # min_dist_df = epa_df_tmp[(epa_df_tmp[EPA_LATITUDE] == min_lat) &
            #                          (epa_df_tmp[EPA_LONGITUDE] == min_lon)]

            return epa_df_tmp.iloc[index_min]['Arithmetic Mean']

    except Exception as e:
        print(f'EPA Lookup: error on lat: {d1_lat} and lon: {d1_lon}')
        return fill_value


def build_empty_dataframe(df, fill_value):
    dict_copy = {}
    for col in df.columns:
        dict_copy[col] = fill_value

    df = pd.DataFrame(dict_copy, index=[0])
    return df


def run_grasp_data_lookup(d1_lat, d1_lon, grasp_df, fill_value):
    from L1C_Conversion_Pipeline.Grasp_Pipeline.grasp_dict import GRASP_LATITUDE, GRASP_LONGITUDE

    try:
        # Limit search to 1 degree in all directions
        lat_mask = ((grasp_df[GRASP_LATITUDE] >= d1_lat - 1) & (grasp_df[GRASP_LATITUDE] <= d1_lat + 1))
        lon_mask = ((grasp_df[GRASP_LONGITUDE] >= d1_lon - 1) & (grasp_df[GRASP_LONGITUDE] <= d1_lon + 1))

        grasp_data = grasp_df[lat_mask & lon_mask]

        # Look for where other cols aren't equal to fill value (we'll just default to fill value if it is)
        cols = [col for col in grasp_df.columns]
        cols.remove(GRASP_LONGITUDE)
        cols.remove(GRASP_LATITUDE)

        query = ' & '.join([f'{col} != {fill_value}' for col in cols])
        grasp_data = grasp_data.query(query)

        if len(grasp_data) == 0:
            # Only filter by Lat and Lon
            # grasp_data = grasp_df[lat_mask & lon_mask]

            # if len(grasp_data) == 0:
            #     # Return a dataframe with all rows filled with Fill Value
            return build_empty_dataframe(grasp_data, fill_value)

        # Run below if Lat/Lon/PM25 filter > 0 OR Lat/Lon filter > 0
        dist, index_min, min_lat, min_lon = \
            calc_spherical_distance(
                d1_lat, d1_lon,
                grasp_data[GRASP_LATITUDE], grasp_data[GRASP_LONGITUDE],
                verbose=False
            )

        # subset data to only contain closest point data
        # min_dist_df = grasp_data[(grasp_data[GRASP_LATITUDE] == min_lat) &
        #                          (grasp_data[GRASP_LONGITUDE] == min_lon)]
        # data = min_dist_df.iloc[0]

        data = grasp_data.iloc[[index_min]]

        # Return a Row
        return data

    except Exception as e:
        print(f'Grasp Lookup: error on lat: {d1_lat} and lon: {d1_lon}')
        return fill_value
