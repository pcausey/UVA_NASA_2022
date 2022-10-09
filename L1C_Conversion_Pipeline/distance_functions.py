# -*- coding: utf-8 -*-
"""
Created on Sun Aug 28 12:02:55 2022

@author: xxpit
"""

import numpy as np
import pandas as pd


def calc_spherical_distance(lat1, lon1, 
                            lat_compare, lon_compare, 
                            loc_units = 'degrees', 
                            dist_units = 'km',
                            dist_type = 'great_circle',
                            verbose = True):
    
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
    
    ####################
    # CHECK LOCATION UNITS
    #################### 
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
      
    ####################
    # CHECK DISTANCE UNTIS
    ####################   
    # Check distance units and set the radius of the earth to km or mi                       
    if dist_units == 'km':
        radius = 6371
    elif dist_units == 'mi':
        radius = 3958.756
    else:
        raise ValueError("Please ensure 'dist_units' is either 'km' or 'mi'")
        
    ####################
    # CALCULATE DISTANCES
    ####################    
    # Calculate the specified distance using various methods
    # Utilizes numpy to broadcast input lat1/lon1 values to work with both
    # singular comparisons and list comparisons
    if dist_type == 'great_circle':
        distances =  radius * (np.arccos(np.sin(lat1) * np.sin(lat_compare) + 
                    np.cos(lat1) * np.cos(lat_compare) * np.cos(lon1 - lon_compare)))
        
    elif dist_type == 'haversine':
        dlon = lon_compare - lon1
        dlat = lat_compare - lat1
        a = (np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat_compare) * 
            np.sin(dlon / 2) ** 2)
        distances = 2 * radius * np.arcsin(np.sqrt(a))
    
    else:
        raise NotImplementedError("""Distances other than 'great_circle' or 
                                  'haversine' have not been implemented.""")
                                                
    ####################
    # OUTPUT RESULTS
    #################### 
    index_min = np.argmin(distances)
    
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
               at index {index_min}""".format(dist_type = dist_type,
                                            lat1 = lat1_old,
                                            lon1 = lon1_old,
                                            min_dist_lat = report_lat,
                                            min_dist_lon = report_lon,
                                            index_min = index_min))
            
    elif verbose:
        print("""The minimum {dist_type} distance for ({lat1}, {lon1})
               corresponds to the point ({min_dist_lat}, {min_dist_lon}) 
               at index {index_min}""".format(dist_type = dist_type,
                                            lat1 = np.degrees(lat1),
                                            lon1 = np.degrees(lon1),
                                            min_dist_lat = report_lat,
                                            min_dist_lon = report_lon,
                                            index_min = index_min))

    
    return distances, index_min, report_lat, report_lon


def get_closest_pm25(lat1, lon1, dates, epa_data):
    
    """
    SUMMARY:
        
    INPUTS: lat1 (list)   -  
            lon1 (list)   - 
            epa_data (df) -
        
    OUTPUT:
    
    """    

    # temp df to hold lat lon interest
    df = pd.DataFrame({
            'lat': lat1,
            'lon': lon1,
            'date': dates
        })
    
    pm25 = []
    # for every row, calc distance for epa data and get minimum distance 
    # and closest date, append the pm25 from that date to list
    for idx, row in df.iterrows():
        # TODO: Limit EPA data to specific date

        dist, index_min, min_lat, min_lon = calc_spherical_distance(row['lat'],
                                                row['lon'],
                                                epa_data['Latitude'],
                                                epa_data['Longitude'], 
                                                verbose = False)
    
        # subset data to only contain closest point data
        min_dist_df = epa_data[(epa_data['Latitude'] == min_lat) &
                               (epa_data['Longitude'] == min_lon)]
        
        # sort by date, then use .get_loc to find closest index based on 
        # closest time
        min_dist_df = min_dist_df.sort_values(by = 'Date Local')
        min_dist_df['Date Local'] = pd.to_datetime(min_dist_df['Date Local'])
        min_dist_df['Date'] = min_dist_df['Date Local'] # not neccessary 
        min_dist_df = min_dist_df.set_index('Date Local')
        min_dist_df = min_dist_df.groupby(min_dist_df.index).first()
    
        nearest_idx = min_dist_df.index.get_loc(row['date'], method = 'nearest')
        nearest_row = min_dist_df.iloc[nearest_idx]
        
        pm25.append(nearest_row['Arithmetic Mean']) # can use other measure?
        
    return pm25


def run_epa_data_lookup(d1_lat, d1_lon, epa_data, fill_value):
    
    # TE CHANGES: FILTER EPA DATA
    lat_mask = (epa_data['Latitude'] >= d1_lat - 1) & (epa_data['Latitude'] <= d1_lat + 1)
    lon_mask = (epa_data['Longitude'] >= d1_lon - 1) & (epa_data['Longitude'] <= d1_lon + 1)
    # We're prefiltering epa_data on date before passing into this function
    # date_mask = (epa_data['Date Local'] == d1_date)

    # If no epa data meets the lat/long/date criteria, return fill values
    epa_data = epa_data[lat_mask & lon_mask]  # & date_mask

    # Could change to have as fill value
    if len(epa_data) == 0:
        return fill_value
    # Else: run the lookup
    else:
        dist, index_min, min_lat, min_lon = calc_spherical_distance(d1_lat, d1_lon, epa_data['Latitude'],
                                                                    epa_data['Longitude'], verbose=False)

        # subset data to only contain closest point data
        min_dist_df = epa_data[(epa_data['Latitude'] == min_lat) &
                               (epa_data['Longitude'] == min_lon)]

        # sort by date, then use .get_loc to find closest index based on
        # closest time
        # min_dist_df = min_dist_df.sort_values(by='Date Local')
        # min_dist_df['Date Local'] = pd.to_datetime(min_dist_df['Date Local'])
        # min_dist_df['Date'] = min_dist_df['Date Local']  # not neccessary
        # min_dist_df = min_dist_df.set_index('Date Local')

        # min_dist_df = min_dist_df.groupby(min_dist_df.index).first()

        # nearest_idx = min_dist_df.index.get_loc(d1_date, method='nearest')
        # nearest_row = min_dist_df.iloc[nearest_idx]

        # pm25.append(nearest_row['Arithmetic Mean']) # can use other measure?

        return min_dist_df['Arithmetic Mean'][0]
        

def get_closest_point(df1_lat, df1_lon, df1_dates, 
                      df2_lat, df2_lon, df2_dates):
    
    df1 = pd.DataFrame({
            'lat': df1_lat,
            'lon': df1_lon,
            'date': df1_dates
        })
    df1['date'] = pd.to_datetime(df1['date'])
    
    df2 = pd.DataFrame({
            'lat': df2_lat,
            'lon': df2_lon,
            'date': df2_dates
        }).reset_index()
    df2['date'] = pd.to_datetime(df2['date'])
    
    closest_index = []
    for idx, row in df1.iterrows():

        # TODO: Limit df2 data to specific date (or time window from caltrak?)

        dist, index_min, min_lat, min_lon = calc_spherical_distance(row['lat'],
                                                row['lon'],
                                                df2['lat'],
                                                df2['lon'], 
                                                verbose = False)
        
        # subset data to only contain closest point data
        # index is still tracked due to index column
        min_dist_df = df2[(df2['lat'] == min_lat) &
                          (df2['lon'] == min_lon)]
        
        min_dist_df = min_dist_df.sort_values(by = 'date')
        min_dist_df['date_keep'] = min_dist_df['date'] # not neccessary 
        min_dist_df = min_dist_df.set_index('date')
        min_dist_df = min_dist_df.groupby(min_dist_df.index).first()
        
        nearest_idx = min_dist_df.index.get_loc(row['date'], method = 'nearest')
        nearest_row = min_dist_df.iloc[nearest_idx]
        
        closest_index.append(nearest_row['index'])

    return closest_index
    
    
    
    
    
    
    
    
    
    
