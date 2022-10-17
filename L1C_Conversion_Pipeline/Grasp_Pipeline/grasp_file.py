from L1C_Conversion_Pipeline.hdf_file_class import HDFFile, HDFColumn
from L1C_Conversion_Pipeline.Grasp_Pipeline.grasp_dict import grasp_list
from L1C_Conversion_Pipeline.variables import *


class GraspFile(HDFFile):
    def __init__(self, file_path):
        HDFFile.__init__(self, file_path)
        self.final_dict = {}

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
            fill_value = 99999

        field_obj = HDFColumn(
            fill=fill_value,
            scale=scale,
            units=units,
            long_name=long_name,
            data=field[:].data
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

    # def run_epa_matching_to_caltrak(self, caltrak):
    #     fill_value = -99999.0
    #
    #     grasp_pm25_data = []
    #
    #     for i in range(0, len(caltrak.final_dict['geolocation_data']['latitude']['data'][0])):
    #         lat = caltrak.final_dict['geolocation_data']['latitude']['data'][0][i]
    #         lon = caltrak.final_dict['geolocation_data']['longitude']['data'][0][i]
    #
    #         epa_pm25_data.append(run_epa_data_lookup(lat, lon, self.epa_data, fill_value))
    #
    #     epa_pm25_data = np.array(epa_pm25_data, dtype=np.float32)
    #
    #     self.epa_dict = {DATA: epa_pm25_data,
    #                 FILL: fill_value,
    #                 LONG_NAME: 'EPA Measured Particulate Matter 2.5nm',
    #                 SCALE: 1.0,
    #                 UNITS: 'nm'}
