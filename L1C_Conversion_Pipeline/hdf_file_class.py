import h5py
from pyhdf.SD import SD, SDC
import os
from L1C_Conversion_Pipeline.variables import *
from netCDF4 import Dataset
from typing import NamedTuple
import numpy as np


class HDFColumn(NamedTuple):
    """ Class for holding the conversion dictionary column details"""
    scale: float
    fill: str
    units: str
    data: np.array
    long_name: str


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

    def return_field(self, field_name):
        """ Return field values from file based on file_type

            Inputs: file, field, file_type
            Outputs: List of field values
        """

        if self.file_type == self.HDF5:
            output = self.file[field_name]
        elif self.file_type == self.HDF4:
            output = self.file.select(field_name)
        else:
            raise Exception("Unknown File Type")

        return output

    @staticmethod
    def write_nc(variable_dict, filename, verbose=False):
        """
        Writes a .NC file, a hierarchical data format used in L1C, with our newly formatted and aggregated data
        variable_dict = file_dict in the rest of the document
        When verbose = True it will output a running log of complete tasks
        """

        with Dataset(filename, mode='w', format='NETCDF4') as nc:

            for cat in variable_dict.keys():
                if verbose:
                    print("Starting:", cat)

                # Create the category group to store the variables
                nc.createGroup(cat)

                for var in variable_dict[cat].keys():
                    if verbose:
                        print(var)

                    shape = variable_dict[cat][var][DATA].shape

                    # Fill the dimension with variables
                    dimensions = []
                    for i in range(len(shape)):
                        dim_name = f'{var}_{i}'
                        nc.createDimension(dim_name, size=shape[i])
                        dimensions.append(dim_name)

                    # Create the variable instance
                    if verbose:
                        print('creating variable')
                    nc[cat].createVariable(var, datatype='i8', dimensions=dimensions,
                                           fill_value=variable_dict[cat][var][FILL])

                    # Create variable metadata
                    if verbose:
                        print('creating the metadataverse')
                    nc[cat][var].long_name = variable_dict[cat][var][LONG_NAME]
                    nc[cat][var].units = variable_dict[cat][var][UNITS]
                    nc[cat][var].scale_factor = variable_dict[cat][var][SCALE]

                    # Create variable array
                    if verbose:
                        print('creating variable array')
                    nc[cat][var][:] = variable_dict[cat][var][DATA]

                    if verbose:
                        print("")
