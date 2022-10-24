import numpy as np
import pandas as pd
from L1C_Conversion_Pipeline.hdf_file_class import HDFFile, HDFColumn
from L1C_Conversion_Pipeline.Grasp_Pipeline.grasp_dict import grasp_list
from L1C_Conversion_Pipeline.variables import *
from L1C_Conversion_Pipeline.distance_functions import run_grasp_data_lookup


class GraspFile(HDFFile):
    def __init__(self, file_path):
        HDFFile.__init__(self, file_path)
        self.final_dict = {}
        self.grasp_matching_dict = {}
        self.fill_value = -99999.0

    def build_final_dict(self):

        for item in grasp_list:
            self.check_sub_dictionary(self.final_dict, item.folder)

            # create the dictionary within the dictionary for this field
            self.final_dict[item.folder][item.outputName] = {}

            field_obj = self.return_field_object(item.inputName)

            self.final_dict[item.folder][item.outputName] = self.write_to_dictionary(
                field_obj.scale, field_obj.fill, field_obj.long_name, field_obj.units, field_obj.data
            )

    def return_field_object(self, field_name):

        field = self.return_field(field_name)

        # Preferred process compared with 'hasattr'
        # If attribute doesn't exist, will throw attribute error
        # https://stackoverflow.com/questions/610883/how-do-i-determine-if-an-object-has-an-attribute-in-python
        try:
            units = field.units
        except AttributeError:
            units = "None"

        try:
            scale = field.scale_factor
        except AttributeError:
            scale = 1

        try:
            long_name = field.long_name
        except AttributeError:
            long_name = field_name

        try:
            fill_value = field._FillValue
        except AttributeError:
            fill_value = self.fill_value

        # convert into on dimensional array
        # we can later convert the whole dataset to a dataframe
        # for matching to caltrack file
        data = field[:].data
        data_v2 = np.reshape(data, data.size)

        field_obj = HDFColumn(
            fill=fill_value,
            scale=scale,
            units=units,
            long_name=long_name,
            data=data_v2
        )

        return field_obj

    @staticmethod
    def check_sub_dictionary(dictionary, sub_dictionary):
        if sub_dictionary not in dictionary:
            dictionary[sub_dictionary] = {}

        return dictionary

    @staticmethod
    def write_to_dictionary(scale, fill, long_name, units, data):

        return {SCALE: scale, LONG_NAME: long_name, FILL: fill, UNITS: units, DATA: data}

    def build_grasp_df(self):
        dict_to_df = {}
        for item in self.final_dict['grasp']:
            dict_to_df[item] = self.final_dict['grasp'][item]['data']

        grasp_df = pd.DataFrame(dict_to_df)
        grasp_df = grasp_df.fillna(self.fill_value)

        # Look for where pm25 isn't empty (we'll just default to fill value if it is)
        pm25_mask = (grasp_df['pm25'] != self.fill_value)

        grasp_df = grasp_df[pm25_mask]

        return grasp_df

    def run_grasp_matching_to_caltrak(self, caltrack):

        grasp_pm25_data = []

        grasp_df = self.build_grasp_df()

        len_caltrack = len(caltrack.final_dict['geolocation_data']['latitude']['data'][0])

        for i in range(0, len_caltrack):
            lat = caltrack.final_dict['geolocation_data']['latitude']['data'][0][i]
            lon = caltrack.final_dict['geolocation_data']['longitude']['data'][0][i]

            grasp_pm25_data.append(run_grasp_data_lookup(lat, lon, grasp_df, self.fill_value))

            if i % 1000 == 0:
                print(f"Completed {i} out of {len_caltrack}")
                # break

        grasp_pm25_data = np.array(grasp_pm25_data, dtype=np.float32)

        self.grasp_matching_dict = {
            DATA: grasp_pm25_data,
            FILL: self.fill_value,
            LONG_NAME: 'Grasp Calculated Particulate Matter 2.5nm',
            SCALE: 1.0,
            UNITS: 'nm'
        }
