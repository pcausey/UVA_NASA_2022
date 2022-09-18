import h5py
from pyhdf.SD import SD, SDC
import os
from variables import *


class HDFFile:

    HDF4 = '.hdf'
    HDF5 = '.h5'

    def __init__(self, file_path):
        self.file_type = self.parse_file_type(file_path)
        self.file = self.return_file(file_path)

    @staticmethod
    def parse_file_type(file_path):
        split_tup = os.path.splitext(file_path)
        file_type = split_tup[1]

        return file_type

    def return_file(self, file_path):
        """ Check if file is hdf5 or hdf4

            Inputs: file_path - where file is located on computer
            Outputs:
                f - actual file that we're going to use
                file_type - extension used by other functions to access data
        """

        if self.file_type == self.HDF5:
            f = h5py.File(file_path, "r")
        elif self.file_type == self.HDF4:
            f = SD(file_path, SDC.READ)
        else:
            raise Exception("Unknown File Type")

        return f

    def parse_field(self, field):
        """ Return field values from file based on file_type

            Inputs: file, field, file_type
            Outputs: List of field values
        """

        if self.file_type == self.HDF5:
            output = self.file[field]
        elif self.file_type == self.HDF4:
            output = self.file.select(field)
        else:
            raise Exception("Unknown File Type")

        return output

    def parse_field_variable(self, field, variable):
        """ Return field values from file based on file_type

                    Inputs: file, field, file_type
                    Outputs: List of field values
        """

        var = None

        if variable == FILL:
            if self.file_type == self.HDF5:
                var = self.parse_field.fill
            elif self.file_type == self.HDF4:
                var = self.parse_field._FillValue
            else:
                raise Exception("Unknown File Type")

        elif variable == UNITS:
            if self.file_type == self.HDF5:
                var = self.parse_field.fill
            elif self.file_type == self.HDF4:
                var = self.parse_field.units
            else:
                raise Exception("Unknown File Type")

        elif variable == DATA:
            if self.file_type == self.HDF5:
                var = self.parse_field.fill
            elif self.file_type == self.HDF4:
                var = self.parse_field.get()
            else:
                raise Exception("Unknown File Type")

        elif variable == LONG_NAME:
            if self.file_type == self.HDF5:
                var = self.parse_field.fill
            elif self.file_type == self.HDF4:
                var = self.parse_field.long_name
            else:
                raise Exception("Unknown File Type")

        elif variable == SCALE:
            if self.file_type == self.HDF5:
                var = self.parse_field.fill
            elif self.file_type == self.HDF4:
                var = self.parse_field.scale_factor
            else:
                raise Exception("Unknown File Type")

        return var
