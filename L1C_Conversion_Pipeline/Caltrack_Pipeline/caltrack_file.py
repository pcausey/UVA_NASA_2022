import copy
import numpy as np
from L1C_Conversion_Pipeline.Caltrack_Pipeline.caltrack_dict import *
from L1C_Conversion_Pipeline.hdf_file_class import HDFFile, HDFColumn
from L1C_Conversion_Pipeline.variables import *


class CaltrackFile(HDFFile):

    def __init__(self, file_path):
        HDFFile.__init__(self, file_path)
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

        # Change 'fill value' to NaN so scalar multiplication doesn't impact 'fill value' cells
        tmp_data[orig_data == fill_value] = np.nan

        # Rescale the Data
        tmp_unscaled_data = tmp_data * old_scale
        tmp_rescaled_data = tmp_unscaled_data / new_scale

        # Change 'fill value' back to Original Fill Value
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

    def check_dictionary(self, folder, sub_folder):
        self.final_dict = self.check_sub_dictionary(self.final_dict, folder)
        self.final_dict[folder] = self.check_sub_dictionary(self.final_dict[folder], sub_folder)

    def build_lat_long_time(self, item):

        self.check_dictionary(item.folder, item.outputName)

        field = self.return_field_object(item.inputName)

        # Replace -inf with a number
        tmp_fill, tmp_data = self.replace_inf_fill(field.fill, field.data)

        # Expand the dimensions
        tmp_data = np.expand_dims(tmp_data, axis=0)

        self.final_dict[item.folder][item.outputName] = self.write_to_dictionary(
            scale=field.scale,
            data=tmp_data,
            fill=tmp_fill,
            long_name=field.long_name,
            units=field.units
        )

    def build_data_directional_fields(self, item):

        self.check_dictionary(item.folder, item.outputName)

        field = self.return_field_object(item.inputName)

        # Rescale the value to 0.01
        new_scale = 0.01
        tmp_data = self.rescale_data(
            field.data, field.scale, new_scale, field.fill
        )

        # Expand the dimensions
        tmp_data = np.expand_dims(tmp_data, axis=0)

        self.final_dict[item.folder][item.outputName] = self.write_to_dictionary(
            scale=new_scale,
            data=tmp_data,
            fill=field.fill,
            long_name=field.long_name,
            units=field.units
        )

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
                field = self.return_field_object(item.inputName)

                fields.append(item.inputName)
                fills.append(field.fill)
                long_names.append(field.long_name)
                units.append(field.units)

                # Rescale the value to 1
                new_scale = 1
                tmp_data = self.rescale_data(
                    field.data, field.scale, new_scale, field.fill
                )

                # Expand the dimensions
                tmp_data = np.expand_dims(tmp_data, axis=0)

                scales.append(new_scale)
                arrays.append(tmp_data)

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

        self.check_dictionary(SENSOR_BANDS, field_name)

        list_wavelengths = []
        for item in list_wave:
            list_wavelengths.append(np.full(16, item.wavelength, dtype=int))

        tmp_data = np.stack(list_wavelengths)
        tmp_data = np.swapaxes(tmp_data, 0, 1)

        self.final_dict[SENSOR_BANDS][field_name] = self.write_to_dictionary(
            scale=1,
            data=tmp_data,
            fill=32767,
            long_name=field_name,
            units='nanometers'
        )

    def build_observation_data(self):

        reverse_scale = 1
        new_scale = 1 / reverse_scale

        measurement_dict = self.build_measurement_dict()
        # self.final_dict['measurement_dict'] = measurement_dict

        scale = measurement_dict[Q][SCALE]
        fill = measurement_dict[Q][FILL]

        # Create tensors for each of our I, Q, and U stokes datasets
        I_arr = np.multiply(copy.deepcopy(measurement_dict[I_P][DATA]), scale)
        I_arr[I_arr == fill] = 314159  # np.abs(I_arr)

        Q_arr = np.multiply(copy.deepcopy(measurement_dict[Q][DATA]), scale)
        Q_arr[Q_arr == fill] = 314159

        U_arr = np.multiply(copy.deepcopy(measurement_dict[U][DATA]), scale)
        U_arr[U_arr == fill] = 314159

        # Observation Data
        self.build_i_parasol(measurement_dict, new_scale, fill)
        self.build_dolp(measurement_dict, Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill)
        self.build_q_over_i(measurement_dict, Q_arr, I_arr, reverse_scale, new_scale, fill)
        self.build_u_over_i(measurement_dict, U_arr, I_arr, reverse_scale, new_scale, fill)

    def build_i_parasol(self, measurement_dict, new_scale, fill):
        I_PARASOL = 'I_PARASOL'

        self.check_dictionary(OBSERVATION_DATA, I_PARASOL)

        tmp_data = np.swapaxes(measurement_dict[I_NP][DATA], 2, 3) * new_scale

        # Write to Observation Data in the final dictionary
        self.final_dict[OBSERVATION_DATA][I_PARASOL] = self.write_to_dictionary(
            scale=new_scale,
            data=tmp_data,  # np.round(tmp_data).astype(int),
            fill=fill,
            long_name=I_PARASOL,
            units='None',
        )

    @staticmethod
    def write_to_dictionary(scale, fill, long_name, units, data):

        return {SCALE: scale, LONG_NAME: long_name, FILL: fill, UNITS: units, DATA: data}

    def return_field_object(self, field_name):

        field = self.return_field(field_name=field_name)

        field_obj = HDFColumn(
            fill=field._FillValue,
            scale=field.scale_factor,
            units=field.units,
            long_name=field.long_name,
            data=field.get()
        )

        return field_obj

    def write_to_observation_data(self, new_scale, data, fill, long_name, folder):

        self.check_dictionary(OBSERVATION_DATA, folder)

        # Write to Observation Data in the final dictionary
        self.final_dict[OBSERVATION_DATA][folder] = self.write_to_dictionary(
            scale=new_scale,
            data=data,  # np.round(data).astype(int),
            fill=fill,
            long_name=long_name,
            units='None',
        )

    def build_dolp(self, measurement_dict, Q_arr, U_arr, I_arr, reverse_scale, new_scale, fill):

        DOLP_Folder = 'DOLP_PARASOL'

        DOLP_arr_unfiltered = (
                np.divide(
                    np.sqrt(
                        np.add(np.square(Q_arr), np.square(U_arr))
                    ), I_arr
                ) * new_scale
        )

        DOLP_arr_unfiltered[
                np.where(
                    (measurement_dict[I_P][DATA] == fill)
                    | (measurement_dict[Q][DATA] == fill)
                    | (measurement_dict[U][DATA] == fill)
                )] = fill

        # dolp = np.round(DOLP_arr_unfiltered).astype(int)
        dolp = np.swapaxes(DOLP_arr_unfiltered, 2, 3)

        # Write to Observation Data in the final dictionary
        self.write_to_observation_data(new_scale, dolp, fill, 'Degree of linear polarization', DOLP_Folder)

    def build_q_over_i(self, measurement_dict, Q_arr, I_arr, reverse_scale, new_scale, fill):
        Q_OVER_I_FOLDER = 'Q_over_I_PARASOL'

        Q_over_I = np.divide(Q_arr, I_arr) * new_scale

        Q_over_I[
            np.where(
                (measurement_dict[I_P][DATA] == fill)
                | (measurement_dict[Q][DATA] == fill)
                # | (measurement_dict[U][DATA] == fill)
            )] = fill
        #
        # Q_over_I = np.round(Q_over_I).astype(int)
        Q_over_I = np.swapaxes(Q_over_I, 2, 3)

        # Write to Observation Data in the final dictionary
        self.write_to_observation_data(new_scale, Q_over_I, fill, 'Q over I', Q_OVER_I_FOLDER)

    def build_u_over_i(self, measurement_dict, U_arr, I_arr, reverse_scale, new_scale, fill):
        U_OVER_I_FOLDER = 'U_over_I_PARASOL'

        U_over_I = np.divide(U_arr, I_arr) * new_scale

        U_over_I[
            np.where(
                (measurement_dict[I_P][DATA] == fill)
                # | (measurement_dict[Q][DATA] == fill)
                | (measurement_dict[U][DATA] == fill)
            )] = fill

        # U_over_I = np.round(U_over_I).astype(int)
        U_over_I = np.swapaxes(U_over_I, 2, 3)

        self.write_to_observation_data(new_scale, U_over_I, fill, 'U over I', U_OVER_I_FOLDER)


