from L1C_Conversion_Pipeline.variables import *
import pandas as pd
import numpy as np
from L1C_Conversion_Pipeline.distance_functions import run_epa_data_lookup


class EPAFile:
    def __init__(self, epa_file_name, epa_path, file_date):
        self.epa_dict = {}
        self.epa_data = self.extract_epa_dated_data(epa_path, file_date, epa_file_name)

    @ staticmethod
    def extract_epa_dated_data(epa_path, file_date, epa_file_name):
        # Limit EPA data to specific day
        epa_data = pd.read_csv(epa_path + epa_file_name)
        date_mask = (epa_data['Date Local'] == file_date)
        epa_data = epa_data[date_mask]

        return epa_data

    def run_epa_matching_to_caltrak(self, caltrack):
        fill_value = -99999.0

        epa_pm25_data = []

        for i in range(0, len(caltrack.final_dict['geolocation_data']['latitude']['data'][0])):
            lat = caltrack.final_dict['geolocation_data']['latitude']['data'][0][i]
            lon = caltrack.final_dict['geolocation_data']['longitude']['data'][0][i]

            epa_pm25_data.append(run_epa_data_lookup(lat, lon, self.epa_data, fill_value))

        epa_pm25_data = np.array(epa_pm25_data, dtype=np.float32)

        self.epa_dict = {
            DATA: epa_pm25_data,
            FILL: fill_value,
            LONG_NAME: 'EPA Measured Particulate Matter 2.5nm',
            SCALE: 1.0,
            UNITS: 'nm'
        }

