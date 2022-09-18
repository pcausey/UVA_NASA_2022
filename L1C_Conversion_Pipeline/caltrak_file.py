import numpy as np
import copy
from typing import NamedTuple
from L1C_Conversion_Pipeline.hdf_file_class import HDFFile
from variables import *


class HDFColumn(NamedTuple):
    """ Class for holding the conversion dictionary column details"""
    outputName: str
    inputName: str
    folder: str
    wavelength: str = None


cal_list = [
    HDFColumn(outputName='latitude', inputName='Latitude',
              wavelength=None, folder='geolocation_data'),
    HDFColumn(outputName='longitude', inputName='Longitude',
              wavelength=None, folder='geolocation_data'),
    HDFColumn(outputName='time', inputName='Time',
              wavelength=None, folder='bin_attributes')
]

cal_data_directional = [
    HDFColumn(outputName='solar_zenith', inputName='Solar_Zenith_Angle',
              wavelength=None, folder='geolocation_data'),
    HDFColumn(outputName='sensor_zenith', inputName='View_Zenith_Angle_670P2',
              wavelength=None, folder='geolocation_data'),
    HDFColumn(outputName='relative_azimuth', inputName='Relative_Azimuth_Angle_670P2',
              wavelength=None, folder='geolocation_data')
]

cal_I_non_polarized = [
    HDFColumn(outputName='I', inputName='Normalized_Radiance_443NP',
              wavelength='443', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_1020NP',
              wavelength='1020', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_565NP',
              wavelength='565', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_763NP',
              wavelength='763', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_765NP',
              wavelength='765', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_910NP',
              wavelength='910', folder='observation_data'),
]

cal_I_polarized = [
    HDFColumn(outputName='I', inputName='Normalized_Radiance_490P',
              wavelength='490', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_670P',
              wavelength='670', folder='observation_data'),
    HDFColumn(outputName='I', inputName='Normalized_Radiance_865P',
              wavelength='865', folder='observation_data'),
]

cal_q = [
    HDFColumn(outputName='Q', inputName='Q_Stokes_490P',
              wavelength='490', folder='observation_data'),
    HDFColumn(outputName='Q', inputName='Q_Stokes_670P',
              wavelength='670', folder='observation_data'),
    HDFColumn(outputName='Q', inputName='Q_Stokes_865P',
              wavelength='865', folder='observation_data'),
]


cal_u = [
    HDFColumn(outputName='U', inputName='U_Stokes_490P',
              wavelength='490', folder='observation_data'),
    HDFColumn(outputName='U', inputName='U_Stokes_670P',
              wavelength='670', folder='observation_data'),
    HDFColumn(outputName='U', inputName='U_Stokes_865P',
              wavelength='865', folder='observation_data')
]

wavelength_dict = {
    'I_P': cal_I_polarized,
    'I_NP': cal_I_non_polarized,
    'Q': cal_q,
    'U': cal_u,
}

OBSERVATION_DATA = 'observation_data'


class CaltrakFile:

    def __init__(self, file_path):
        self.file = HDFFile(file_path)
        self.final_dict = {}

    def build_final_dict(self):

        # Lat/Lon/Time
        for item in cal_list:
            self.build_lat_long_time(item)

        # Data Directional Fields (Angles)
        for item in cal_data_directional:
            self.build_data_directional_fields(item)

        # Sensor View Bands
        self.build_sensor_view_bands(cal_I_non_polarized, 'intensity_wavelengths')
        self.build_sensor_view_bands(cal_I_polarized, 'polarization_wavelengths')

        # Observation Data (DOLP, Q over I, U over I, )
        self.build_observation_data()

    @staticmethod
    def rescale_data(orig_data, old_scale, new_scale, fill_value):
        # convert to float so we can use NaN
        tmp_data = orig_data.astype(float)

        # Change 'fill value' to NaN so scalar mulitplication doesn't impact 'fill value' cells
        tmp_data[orig_data == fill_value] = np.nan

        tmp_unscaled_data = tmp_data / old_scale
        tmp_rescaled_data = tmp_unscaled_data * new_scale

        tmp_rescaled_data = np.nan_to_num(x=tmp_rescaled_data, nan=fill_value)

        return tmp_rescaled_data

    @staticmethod
    def replace_inf_fill(fill, data):

        if fill == -np.inf:
            fill = -99999
            data[data == -np.inf] = fill

        return fill, data

    @staticmethod
    def check_sub_dictionary(dictionary, sub_dictionary):
        if sub_dictionary not in dictionary:
            dictionary[sub_dictionary] = {}

        return dictionary

    def build_lat_long_time(self, item):

        self.final_dict = self.check_sub_dictionary(self.final_dict, item.folder)
        self.final_dict = self.check_sub_dictionary(self.final_dict[item.folder], item.outputName)

        self.final_dict[item.folder][item.outputName][SCALE] = \
            self.file.parse_field_variable(item.inputName, SCALE)
        self.final_dict[item.folder][item.outputName][LONG_NAME] = \
            self.file.parse_field_variable(item.inputName, LONG_NAME)
        self.final_dict[item.folder][item.outputName][UNITS] = \
            self.file.parse_field_variable(item.inputName, UNITS)

        fill = self.file.parse_field_variable(item.inputName, FILL)
        data = self.file.parse_field_variable(item.inputName, DATA)

        # Replace -inf with a number
        fill, data = self.replace_inf_fill(fill, data)

        # Expand the dimensions
        data = np.expand_dims(data, axis=0)

        self.final_dict[item.folder][item.outputName][DATA] = data
        self.final_dict[item.folder][item.outputName][FILL] = fill

    def build_data_directional_fields(self, item):

        self.final_dict = self.check_sub_dictionary(self.final_dict, item.folder)
        self.final_dict = self.check_sub_dictionary(self.final_dict[item.folder], item.outputName)

        self.final_dict[item.folder][item.outputName] = {}

        self.final_dict[item.folder][item.outputName][LONG_NAME] = \
            self.file.parse_field_variable(item.inputName, LONG_NAME)
        self.final_dict[item.folder][item.outputName][UNITS] = \
            self.file.parse_field_variable(item.inputName, UNITS)

        # Rescale the value to 0.01
        tmp_scale = self.file.parse_field_variable(item.inputName, SCALE)
        tmp_fill = self.file.parse_field_variable(item.inputName, FILL)
        tmp_data = self.file.parse_field_variable(item.inputName, DATA)
        new_scale = 0.01

        data = self.rescale_data(
            tmp_data, tmp_scale, new_scale, tmp_fill
        )

        # Expand the dimensions
        data = np.expand_dims(data, axis=0)

        # Change 'fill value' back to Original Fill Value
        self.final_dict[item.folder][item.outputName][DATA] = data
        self.final_dict[item.folder][item.outputName][SCALE] = new_scale
        self.final_dict[item.folder][item.outputName][FILL] = tmp_fill

    def build_measurement_dict(self):

        measurement_dict = {}

        for wavelength_cat in wavelength_dict.keys():
            # Set some empty variables here
            fields = []
            arrays = []
            scales = []
            long_names = []
            fills = []
            units = []

            for item in wavelength_dict[wavelength_cat]:
                fields.append(item.inputName)

                fill = self.file.parse_field_variable(item.inputName, FILL)
                long_name = self.file.parse_field_variable(item.inputName, LONG_NAME)
                units = self.file.parse_field_variable(item.inputName, UNITS)
                data = self.file.parse_field_variable(item.inputName, DATA)
                scale = self.file.parse_field_variable(item.inputName, SCALE)

                fills.append(fill)
                long_names.append(long_name)
                units.append(units)

                # Rescale the value to 0.01
                new_scale = 1
                data = self.rescale_data(
                    data, scale, new_scale, fill
                )

                # Expand the dimensions
                data = np.expand_dims(data, axis=0)

                scales.append(new_scale)
                arrays.append(data)

            # All scales, fills, units are the same
            # only save one copy
            if len(np.unique(scales)) == 1:
                scales = scales[0]
            if len(np.unique(fills)) == 1:
                fills = fills[0]
            if len(np.unique(units)) == 1:
                units = units[0]

            # Add our fields to the measurement dictionary
            cat = wavelength_cat
            measurement_dict[cat] = {}
            measurement_dict[cat]['fields'] = fields
            measurement_dict[cat][SCALE] = scales
            measurement_dict[cat][LONG_NAME] = long_names
            measurement_dict[cat][FILL] = fills
            measurement_dict[cat][UNITS] = units

            # Change shape of data arrays
            # such that we get 1 x 60k x 3/6 x 16
            tmp_data = np.stack(arrays, axis=0)
            tmp_data = np.swapaxes(tmp_data, 0, 1)
            tmp_data = np.swapaxes(tmp_data, 1, 2)

            measurement_dict[cat][DATA] = tmp_data

        return measurement_dict

    def build_sensor_view_bands(self, list_wave, field_name):
        # sensor_view_bands
        #   This is just a list of the different wave lengths
        #   so its # wavelengths columns by number 'sheets' as rows
        # * intensity_wavelengths [np]
        # * polarization_wavelengths [p]

        SENSOR_BANDS = 'sensor_views_bands'

        self.final_dict = self.check_sub_dictionary(self.final_dict, SENSOR_BANDS)
        self.final_dict = self.check_sub_dictionary(self.final_dict[SENSOR_BANDS], field_name)

        list_wavelengths = []
        for item in list_wave:
            list_wavelengths.append(np.full(16, item.wavelength, dtype=int))

        self.final_dict[SENSOR_BANDS][field_name][SCALE] = 1
        self.final_dict[SENSOR_BANDS][field_name][LONG_NAME] = field_name
        self.final_dict[SENSOR_BANDS][field_name][FILL] = 32767
        self.final_dict[SENSOR_BANDS][field_name][UNITS] = 'nanometers'
        self.final_dict[SENSOR_BANDS][field_name][DATA] = np.stack(list_wavelengths)

    def build_observation_data(self):

        reverse_scale = 1000
        new_scale = 1 / reverse_scale

        measurement_dict = self.build_measurement_dict()

        scale = measurement_dict['Q'][SCALE]
        fill = measurement_dict['Q'][FILL]

        # Create tensors for each of our I, Q, and U stokes datasets
        for key in measurement_dict.keys():
            if key == 'I_P':
                I_arr = np.multiply(copy.deepcopy(measurement_dict[key][DATA]), scale)
                I_arr[np.abs(I_arr) == fill] = 1

            elif key == 'Q':
                Q_arr = np.multiply(copy.deepcopy(measurement_dict[key][DATA]), scale)
                Q_arr[np.abs(Q_arr) == fill] = 1

            elif key == 'U':
                U_arr = np.multiply(copy.deepcopy(measurement_dict[key][DATA]), scale)
                U_arr[np.abs(U_arr) == fill] = 1

            else:
                continue

        # Observation Data
        self.final_dict[OBSERVATION_DATA]['I_PARASOL'] = measurement_dict['I_NP']
        self.build_dolp(Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill)
        self.build_q_over_i(Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill)
        self.build_u_over_i(Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill)

    def write_to_observation_data(self, scale, fill, long_name, units, data, folder):

        # Write the DOLP data to the final dictionary
        self.final_dict[OBSERVATION_DATA][folder][SCALE] = scale
        self.final_dict[OBSERVATION_DATA][folder][LONG_NAME] = long_name
        self.final_dict[OBSERVATION_DATA][folder][FILL] = fill
        self.final_dict[OBSERVATION_DATA][folder][UNITS] = units
        self.final_dict[OBSERVATION_DATA][folder][DATA] = data

    def build_dolp(self, Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill):

        DOLP = 'DOLP_PARASOL'

        self.final_dict = self.check_sub_dictionary(self.final_dict, OBSERVATION_DATA)
        self.final_dict = self.check_sub_dictionary(self.final_dict[OBSERVATION_DATA], DOLP)

        DOLP_arr_unfltrd = (
                np.divide(
                    np.sqrt(
                        np.add(np.square(Q_arr), np.square(U_arr))
                    ), I_arr
                ) * reverse_scale
        )

        # Write the observation data in final dictionary
        self.write_to_observation_data(
            scale=new_scale,
            long_name='Degree of linear polarization',
            fill=fill,
            units='None',
            data=np.round(DOLP_arr_unfltrd).astype(int),
            folder=DOLP
        )

    def build_q_over_i(self, measurement_dict, Q_arr, I_arr, reverse_scale, new_scale, fill):
        Q_OVER_I_NAME = 'Q_over_I_PARASOL'

        self.final_dict = self.check_sub_dictionary(self.final_dict, OBSERVATION_DATA)
        self.final_dict = self.check_sub_dictionary(self.final_dict[OBSERVATION_DATA], Q_OVER_I_NAME)

        Q_over_I = np.divide(Q_arr, I_arr) * reverse_scale

        Q_over_I[
            np.where(
                (measurement_dict['I_P'][DATA] == fill)
                | (measurement_dict['Q'][DATA] == fill)
                | (measurement_dict['U'][DATA] == fill)
            )] = fill

        # Write to Observation Data in the final dictionary
        self.write_to_observation_data(
            scale=new_scale,
            long_name='Q over I',
            fill=fill,
            units='None',
            data=np.round(Q_over_I).astype(int),
            folder=Q_OVER_I_NAME
        )

    def build_u_over_i(self, measurement_dict, Q_arr, I_arr, reverse_scale, new_scale, fill):
        U_OVER_I_NAME = 'U_over_I_PARASOL'

        self.final_dict = self.check_sub_dictionary(self.final_dict, OBSERVATION_DATA)
        self.final_dict = self.check_sub_dictionary(self.final_dict[OBSERVATION_DATA], U_OVER_I_NAME)

        U_over_I = np.divide(Q_arr, I_arr) * reverse_scale

        U_over_I[
            np.where(
                (measurement_dict['I_P'][DATA] == fill)
                | (measurement_dict['Q'][DATA] == fill)
                | (measurement_dict['U'][DATA] == fill)
            )] = fill

        # Write to Observation Data in the final dictionary
        self.write_to_observation_data(
            scale=new_scale,
            long_name='U over I',
            fill=fill,
            units='None',
            data=np.round(U_over_I).astype(int),
            folder=U_OVER_I_NAME
        )
