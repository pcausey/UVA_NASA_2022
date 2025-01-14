import numpy as np
import pandas as pd
from L1C_Conversion_Pipeline.hdf_file_class import HDFFile, HDFColumn
from L1C_Conversion_Pipeline.Grasp_Pipeline.grasp_dict import grasp_list, GRASP_LATITUDE, GRASP_LONGITUDE
from L1C_Conversion_Pipeline.variables import *
from scipy.spatial import KDTree


class GraspFile(HDFFile):
    def __init__(self, file_path):
        HDFFile.__init__(self, file_path)
        self.final_dict = {}
        self.grasp_matching_dict = {}
        self.fill_value = -99999.0

    def build_final_dict(self):

        for item in grasp_list:
            # create the dictionary within the dictionary for this field
            self.final_dict[item.outputName] = {}

            field_obj = self.return_field_object(item.inputName)

            self.final_dict[item.outputName] = self.write_to_dictionary(
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
        for item in self.final_dict:
            dict_to_df[item] = self.final_dict[item][DATA]

        grasp_df = pd.DataFrame(dict_to_df)

        # This works because the Grasp File has all its 'fill values' as 'NaN'
        grasp_df = grasp_df.fillna(self.fill_value)

        # Look for where Latitude and Longitude aren't empty
        lat_mask = (grasp_df[GRASP_LATITUDE] != self.final_dict[GRASP_LATITUDE][FILL])
        lon_mask = (grasp_df[GRASP_LONGITUDE] != self.final_dict[GRASP_LONGITUDE][FILL])

        # Look for where pm25 isn't empty (we'll just default to fill value if it is)
        # pm25_mask = (grasp_df['pm25'] != self.fill_value)

        grasp_df = grasp_df[lat_mask & lon_mask]

        return grasp_df

    def run_grasp_matching_to_caltrack(self, caltrack):

        lats = caltrack.final_dict['geolocation_data']['latitude']['data'][0]
        lons = caltrack.final_dict['geolocation_data']['longitude']['data'][0]
        caltrack_lat_lon = np.stack([lats, lons], axis=1)

        grasp_df = self.build_grasp_df()

        grasp_lat_lon = np.stack([grasp_df[GRASP_LATITUDE].to_numpy(),
                                  grasp_df[GRASP_LONGITUDE].to_numpy()], axis=1)
        grasp_kdtree = KDTree(grasp_lat_lon)

        idx = grasp_kdtree.query_ball_point(caltrack_lat_lon, r=0.1)

        matching_list_of_dfs = []
        for x in idx:
            if len(x) > 0:
                matching_list_of_dfs.append(grasp_df.iloc[[x[0]]])
            else:
                matching_list_of_dfs.append(self.build_empty_dataframe(grasp_df, self.fill_value))

        full_matching_df = pd.concat(matching_list_of_dfs)

        for col in full_matching_df:
            self.grasp_matching_dict[col] = {
                DATA: np.array(full_matching_df[col], dtype=np.float32),
                FILL: self.fill_value,
                LONG_NAME: self.final_dict[col][LONG_NAME],
                SCALE: self.final_dict[col][SCALE],
                UNITS: self.final_dict[col][UNITS]
            }

    @staticmethod
    def build_empty_dataframe(df, fill_value):
        dict_copy = {}
        for col in df.columns:
            dict_copy[col] = fill_value

        df = pd.DataFrame(dict_copy, index=[0])
        return df
