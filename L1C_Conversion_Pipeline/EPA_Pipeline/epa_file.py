from L1C_Conversion_Pipeline.variables import *
import pandas as pd
import numpy as np
from L1C_Conversion_Pipeline.distance_functions import run_epa_data_lookup


EPA_LATITUDE = 'Latitude'
EPA_LONGITUDE = 'Longitude'


class EPAFile:
    def __init__(self, epa_file_name, epa_path, file_date):
        self.epa_dict = {}
        self.epa_data = self.extract_epa_dated_data(epa_path, file_date, epa_file_name)

    @ staticmethod
    def extract_epa_dated_data(epa_path, file_date, epa_file_name):
        # Limit EPA data to specific day
        epa_data = pd.read_csv(epa_path + epa_file_name)

        cols_to_keep = ['Date Local', EPA_LATITUDE, EPA_LONGITUDE, 'Arithmetic Mean']
        epa_data = epa_data[cols_to_keep]

        date_mask = (epa_data['Date Local'] == file_date)
        epa_data = epa_data[date_mask]

        return epa_data

    def run_epa_matching_to_caltrack(self, caltrack):
        fill_value = -99999.0

        lats = caltrack.final_dict['geolocation_data']['latitude']['data'][0]
        lons = caltrack.final_dict['geolocation_data']['longitude']['data'][0]

        epa_matcher = EPAMatcher(self.epa_data, fill_value)
        function = epa_matcher.match_single_lat_lon

        matching_list_of_pm25 = map(function, lats, lons)
        matching_list_of_pm25 = list(matching_list_of_pm25)

        # for i in range(0, len(caltrack.final_dict['geolocation_data']['latitude']['data'][0])):
        #     lat = caltrack.final_dict['geolocation_data']['latitude']['data'][0][i]
        #     lon = caltrack.final_dict['geolocation_data']['longitude']['data'][0][i]
        #
        #     epa_pm25_data.append(run_epa_data_lookup(lat, lon, self.epa_data, fill_value))

        epa_pm25_data = np.array(matching_list_of_pm25, dtype=np.float32)

        self.epa_dict = {
            DATA: epa_pm25_data,
            FILL: fill_value,
            LONG_NAME: 'EPA Measured Particulate Matter 2.5nm',
            SCALE: 1.0,
            UNITS: 'nm'
        }


class EPAMatcher:
    def __init__(self, epa_df, fill_value):
        self.epa_df = epa_df
        self.fill_value = fill_value

    def match_single_lat_lon(self, d1_lat, d1_lon):

        epa_data = self.filter_epa_data(d1_lat, d1_lon)

        epa_pm25 = self.run_epa_data_lookup(d1_lat, d1_lon, epa_data)

        return epa_pm25

    def filter_epa_data(self, d1_lat, d1_lon):
        degree_search = 0.1

        # TE CHANGES: FILTER EPA DATA
        lat_mask = (self.epa_df[EPA_LATITUDE] >= d1_lat - degree_search) \
                   & (self.epa_df[EPA_LATITUDE] <= d1_lat + degree_search)
        lon_mask = (self.epa_df[EPA_LONGITUDE] >= d1_lon - degree_search) \
                   & (self.epa_df[EPA_LONGITUDE] <= d1_lon + degree_search)
        # We're pre-filtering epa_data on date before passing into this function
        # date_mask = (epa_data['Date Local'] == d1_date)

        # If no epa data meets the lat/long/date criteria, return fill values
        epa_df_tmp = self.epa_df[lat_mask & lon_mask]

        return epa_df_tmp

    def run_epa_data_lookup(self, d1_lat, d1_lon, epa_df_tmp):
        from L1C_Conversion_Pipeline.distance_functions import calc_spherical_distance

        try:
            # Could change to have as fill value
            if len(epa_df_tmp) == 0:
                return self.fill_value
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
            return self.fill_value
