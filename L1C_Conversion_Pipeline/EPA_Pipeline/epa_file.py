from L1C_Conversion_Pipeline.variables import *
import pandas as pd
import numpy as np
from scipy.spatial import KDTree


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

        epa_data = epa_data.sort_values(["Latitude", "Longitude"])
        epa_data = epa_data.reset_index(drop=True)

        return epa_data

    def run_epa_matching_to_caltrack(self, caltrack):
        fill_value = -99999.0

        lats = caltrack.final_dict['geolocation_data']['latitude']['data'][0]
        lons = caltrack.final_dict['geolocation_data']['longitude']['data'][0]
        caltrack_lat_lon = np.stack([lats, lons], axis=1)

        # _, closest_1_idx = epa_kdtree.query(caltrack_lat_lon, 1)
        # closest_1_latlon = self.epa_data[closest_1_idx]

        epa_lat_lon = np.stack([self.epa_data['Latitude'].to_numpy(),
                                self.epa_data['Longitude'].to_numpy()], axis=1)
        epa_kdtree = KDTree(epa_lat_lon)

        idx = epa_kdtree.query_ball_point(caltrack_lat_lon, r=1)

        list_pm25 = []
        for x in idx:
            if len(x) > 0:
                list_pm25.append(self.epa_data.loc[x[0]]['Arithmetic Mean'])
            else:
                list_pm25.append(fill_value)

        # epa_matcher = EPAMatcher(self.epa_data, fill_value)
        #
        # function = epa_matcher.filter_epa_data
        # list_of_df_slices = map(function, lats, lons)
        # list_of_df_slices_proc = list(list_of_df_slices)
        #
        # function = epa_matcher.run_epa_data_lookup
        # matching_list_of_pm25 = map(function, lats, lons, list_of_df_slices_proc)
        # matching_list_of_pm25 = list(matching_list_of_pm25)

        # for i in range(0, len(caltrack.final_dict['geolocation_data']['latitude']['data'][0])):
        #     lat = caltrack.final_dict['geolocation_data']['latitude']['data'][0][i]
        #     lon = caltrack.final_dict['geolocation_data']['longitude']['data'][0][i]
        #
        #     epa_pm25_data.append(run_epa_data_lookup(lat, lon, self.epa_data, fill_value))

        epa_pm25_data = np.array(list_pm25, dtype=np.float32)

        self.epa_dict = {
            DATA: epa_pm25_data,
            FILL: fill_value,
            LONG_NAME: 'EPA Measured Particulate Matter 2.5nm',
            SCALE: 1.0,
            UNITS: 'nm'
        }
