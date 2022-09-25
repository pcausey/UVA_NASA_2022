# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 12:39:28 2022

@author: whetz
"""
from netCDF4 import Dataset
import netCDF4

def write_nc(variable_dict, filename):

    nc = Dataset(filename, mode='w')

    for cat in variable_dict.keys():

        # Create the category group to store the variables
        nc.createGroup(cat)

        for var in variable_dict[cat].keys():
            print(var)

            # Fill the dimension with variables
            dimensions = []
            for i in range(len(variable_dict[cat][var]['array'].shape)):
                dim_name = f'{var}_{i}'
                nc.createDimension(dim_name, size=None)
                dimensions.append(dim_name)

            # Create the variable instance
            nc[cat].createVariable(var, datatype='f4', dimensions=dimensions, fill_value=variable_dict[cat][var]['fill_value'])

            # Create variable metadata 
            nc[cat][var].long_name = variable_dict[cat][var]['long_name']
            nc[cat][var].units = variable_dict[cat][var]['units']
            nc[cat][var].scale_factor = variable_dict[cat][var]['scale']

            # Create variable array 
            nc[cat][var][:] = variable_dict[cat][var]['array']
            
    return nc