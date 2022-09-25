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

        field_obj = HDFColumn(
            fill=field._FillValue,
            scale=field.scale_factor,
            units=field.Units,
            long_name=field.Long_Name,
            data=field.get()
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
