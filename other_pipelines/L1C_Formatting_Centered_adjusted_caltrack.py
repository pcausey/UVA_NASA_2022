# -*- coding: utf-8 -*-
"""
Created on Tue May 10 15:46:12 2022

@author: whetz
"""
import h5py
import os
import numpy as np
import copy
from netCDF4 import Dataset

# # Set the directory and then the filepath within the directory for the file you wish to convert
# directory = '/project/sdscap-shakeri/nasa/HDF'
# file_path = 'PARASOL/L1_B-HDF.v1.00/2008/2008_06_01/POLDER3_L1B-BG1-080146M_2008-06-01T00-08-19_V1-00.h5'
#
# # Set the filepath you wish to write to along with the name of the file you wish to write
# write_path = '/gpfs/gpfs0/project/sdscap-shakeri/nasa/UVA_NASA_2021'
# write_name = 'example_centered.nc'


def output_centered_l1c_file(directory, file_path, write_path, write_name):

    # Set the number of pixels you want to look at
    pixels = 3

    # Validation on the pixel count, making sure it's an odd number
    if pixels <= 0 or pixels % 1 != 0:
        print("Error, enter a valid pixel number, pixels being set to 3")
        pixels = 3
    elif pixels % 2 == 0 and pixels > 2:
        print("Error, even number of pixels chosen. Pixels being reset")
        pixels -= 1

    # Set the directory
    os.chdir(directory)

    # Create the hdf file object in python
    f = h5py.File(file_path, "r")

    # Get relevant information and data from the I490P field
    # i490 = f['Data_Directional_Fields']['I490P']
    i490 = f['Normalized_Radiance_490P']
    i490_fill = i490.attrs['_FillValue']
    i490_arr = np.array(i490)
    i490_arr.shape
    ### Cut down extra lat/lons
    # We only want to use columns and rows where actual data exists
    rows = []
    cols = []
    for row in range(i490.shape[0]):
        inds = np.where(np.abs(i490_arr[row]) != i490_fill)
        if (len(inds[0])) > 0:
            rows.append(row)
            continue

    for col in range(i490.shape[1]):
        inds = np.where(np.abs(i490_arr[:,col]) != i490_fill)
        if len(inds[0]) > 0:
            cols.append(col)
            continue

    # Set the shape of the pared down tensor
    new_shape = (i490_arr[rows])[:,cols].shape
    shape = i490_arr.shape


    # Angles, Altitude, & Location
    # We want to store as much information in integer format with scale factors as possible
    # These variables are set as conversion factors to go between float and a reasonably accurae integer
    reverse_scale = 1000
    new_scale = 1/reverse_scale

    # This initializes the dictionary where we'll store all of our information before we cast it to an HDF file.
    # A dictionary is a natural analog for an HDF file since it also utilizes hierarchical organization
    final_dict = {}
    final_dict['geolocation_data'] = {}

    # Get latitude array and corresponding indices
    # We only want to look at datapoints for which there are legitimate coordinates
    # Here we'll compile a group of indices that correspond to the satellites center of vision
    # Width of the vision is set by "pixels" above

    # Geo inds will be the compiled list of centered indices
    geo_inds = [[],[]]

    #lats = np.array(f['Geolocation_Fields']['Latitude'])
    lats
    #lons = np.array(f['Geolocation_Fields']['Longitude'])

    flag = True

    # Loop through our latitudes
    for i in range(lats.shape[0]):
        #caltrack geolocation data looks different. Uses -Infinity as the fill value
        inds = np.where(np.abs(lats[i]) != 99999)[0]

        vals = lats[i][inds]

        # Ensure that there is only one latitude value for each row
        if len(np.unique(vals)) > 1:
            print("Error", i)
            break
        # If there are no values in this row continue
        elif len(np.unique(vals)) == 0:
            # If len == 0 then skip this element
            continue
        else:
            val = np.unique(vals)[0]

        # Find the center of the satellite's view
        center_ind = int(np.sum(inds)/len(inds))

        # If there aren't any surrounding values continue
        # Otherwise append our relevant indices list
        if center_ind < 1:
            continue
        else:
            temp_lats = lats[i][center_ind-(pixels-2):center_ind+(pixels-1)]
            geo_inds[0].extend([i for u in range(3)])
            geo_inds[1].extend([u for u in range(center_ind-1,center_ind+2)])

        if flag:
            new_lats = np.array([temp_lats])
            flag = False
        else:
            new_lats = np.append(new_lats, np.array([temp_lats]), axis=0)

    # Set the shape of our tensor for later
    geo_shape = (new_lats.shape)

    # Write our geolocation data to the final dictionary
    # Warnings here are typical, but do not affect data fidelity right now
    for field in ['Latitude','Longitude','surface_altitude']:
        if field == 'surface_altitude':
            tag = 'altitude'
        else:
            tag = field.lower()

        final_dict['geolocation_data'][tag] = {}

        # final_dict['geolocation_data'][tag]['long_name'] = f['Geolocation_Fields'][field].attrs['long_name']
        final_dict['geolocation_data'][tag]['long_name'] = f[field].attrs['long_name']
        # final_dict['geolocation_data'][tag]['units'] = f['Geolocation_Fields'][field].attrs['units']
        final_dict['geolocation_data'][tag]['units'] = f[field].attrs['units']

        if tag in ['latitude','longitude']:
            # temp_arr = np.array(f['Geolocation_Fields'][field])
            temp_arr = np.array(f[field])
            # fill = f['Geolocation_Fields'][field].attrs['_FillValue']
            fill = f[field].attrs['_FillValue']
            fill_inds = np.where(temp_arr == fill)
            temp_arr = temp_arr*reverse_scale
            temp_arr[fill_inds] = fill
            fill = int(fill)

            final_dict['geolocation_data'][tag]['fill'] = fill
            final_dict['geolocation_data'][tag]['scale'] = new_scale
            final_dict['geolocation_data'][tag]['data'] = np.round(temp_arr[geo_inds].reshape(geo_shape)).astype(int)
        else:
            final_dict['geolocation_data'][tag]['scale'] = f['Geolocation_Fields'][field].attrs['scale_factor']
            final_dict['geolocation_data'][tag]['data'] = (np.array(f['Geolocation_Fields'][field]))[geo_inds].reshape(geo_shape)
            final_dict['geolocation_data'][tag]['fill'] = f['Geolocation_Fields'][field].attrs['_FillValue']

    # Write our data directional fields using the aggregated list of indices
    for field in ['thetas','thetav','phi']:
        if field == 'thetas':
            tag = 'solar_zenith'
        elif field == 'thetav':
            tag = 'sensor_zenith'
        elif field == 'phi':
            tag = 'relative_azimuth'

        # Define the shape of the data
        field_shape = list(geo_shape)
        field_shape.append(np.array(f['Data_Directional_Fields'][field]).shape[-1])
        field_shape = tuple(field_shape)

        final_dict['geolocation_data'][tag] = {}
        final_dict['geolocation_data'][tag]['scale'] = f['Data_Directional_Fields'][field].attrs['scale_factor']
        final_dict['geolocation_data'][tag]['long_name'] = f['Data_Directional_Fields'][field].attrs['long_name']
        final_dict['geolocation_data'][tag]['fill'] = f['Data_Directional_Fields'][field].attrs['_FillValue']
        final_dict['geolocation_data'][tag]['units'] = f['Data_Directional_Fields'][field].attrs['units']
        final_dict['geolocation_data'][tag]['data'] = (np.array(f['Data_Directional_Fields'][field]))[geo_inds].reshape(field_shape)


    # Generate I, Q, & U fields
    # Use the indices that we've aggreagated to capture the measurement fields that we require

    # Set an interim dictionary which will hold all of our raw I, Q, & U data and metadata before we transform it and write it to the final dictionary and HDF
    measurement_dict = {}

    # Loop through each of these fields
    for cat in ['I_np','I_p','Q','U']:
        print(cat)

        # Look for either polarized or non-polarized fields
        if cat == 'I_np':
            tag = cat.replace('_np','')
            fields = ([field for field in list(f['Data_Directional_Fields'].keys()) if (tag in field)])
        else:
            tag = cat.replace('_p','')
            fields = ([field for field in list(f['Data_Directional_Fields'].keys()) if (tag in field) and ('NP' not in field)])

        print("tag:", tag)
        fields.sort()

        # Define the shape of the data
        field_shape = list(geo_shape)
        field_shape.append(np.array(f['Data_Directional_Fields'][field]).shape[-1])
        field_shape = tuple(field_shape)

        # Set some empty variables here
        arrays = []
        scales = []
        long_names = []
        fills = []
        units = []

        # Loop through each of the relvant fields and look for the information that we require
        for field in fields:

            print(field, end=", ")

            scales.append(f['Data_Directional_Fields'][field].attrs['scale_factor'])
            long_names.append(f['Data_Directional_Fields'][field].attrs['long_name'])
            fills.append(f['Data_Directional_Fields'][field].attrs['_FillValue'])
            units.append(f['Data_Directional_Fields'][field].attrs['units'])

            arrays.append(np.array(f['Data_Directional_Fields'][field])[geo_inds].reshape(field_shape))

        if len(np.unique(scales)) == 1:
            scales = scales[0]
        if len(np.unique(fills)) == 1:
            fills = fills[0]
        if len(np.unique(units)) == 1:
            units = units[0]

        # Add our fields to the measurement dictionary
        measurement_dict[cat] = {}
        measurement_dict[cat]['fields'] = fields
        measurement_dict[cat]['scale'] = scales
        measurement_dict[cat]['long_name'] = long_names
        measurement_dict[cat]['fill'] = fills
        measurement_dict[cat]['units'] = units
        measurement_dict[cat]['data'] = np.stack(arrays,axis=3)

    # Create Observation Data field in the Final Dictionary and Populate
    # Set the empty field within the final dictionary
    final_dict['observation_data'] = {}

    # Take the non-polarized intensity readings and move them directly to the final dictionary
    # No transformations needed here
    final_dict['observation_data']['I_PARASOL'] = copy.deepcopy(measurement_dict['I_np'])


    ## Wavelengths
    # Here we set the map that tells us which indices correspond to each wavelength and polarization state

    # Set the empty field sensor view bands in our final dictionary
    final_dict['sensor_views_bands'] = {}

    # Loop through polarized and non-polarized settings
    for i in [True, False]:
        if i:
            tag = 'I_p'
            field_name = 'polarization_wavelengths'
            lambdas = [int(field.replace('I','').replace('P','')) for field in measurement_dict[tag]['fields']]

        else:
            tag = 'I_np'
            field_name = 'intensity_wavelengths'
            lambdas = [int(field.replace('I','').replace('NP','').replace('P','')) for field in measurement_dict[tag]['fields']]


        shape = measurement_dict[tag]['data'].shape
        new_shape = []
        lambda_arrs = []

        for lam in lambdas:
            lambda_arrs.append(np.full(16,lam))

        full_lambdas_arr = np.stack(lambda_arrs,axis=1)

        final_dict['sensor_views_bands'][field_name] = {}
        final_dict['sensor_views_bands'][field_name]['scale'] = 1
        final_dict['sensor_views_bands'][field_name]['long_name'] = 'field_name'
        final_dict['sensor_views_bands'][field_name]['fill'] = 32767
        final_dict['sensor_views_bands'][field_name]['units'] = 'tbd'
        final_dict['sensor_views_bands'][field_name]['data'] = full_lambdas_arr

    ## DOLP
    # Here we'll calculate the degree of linear polarization tensors
    # DOLP = sqrt(Q^2 + U^2)/I

    scale = measurement_dict['Q']['scale']
    fill = measurement_dict['Q']['fill']

    # Create tensors for each of our I, Q, and U stokes datasets
    for key in measurement_dict.keys():
        if key == 'I_p':
            I_arr = copy.deepcopy(measurement_dict[key]['data']) * scale
            I_arr[np.abs(I_arr) == fill] = 1

        elif key == 'Q':
            Q_arr = copy.deepcopy(measurement_dict[key]['data']) * scale
            Q_arr[np.abs(Q_arr) == fill] = 1

        elif key == 'U':
            U_arr = copy.deepcopy(measurement_dict[key]['data']) * scale
            U_arr[np.abs(U_arr) == fill] = 1

        else:
            continue

    # Create the unfiltered degreee of linear polarization (DOLP) tensor
    DOLP_arr_unfltrd = np.divide(np.sqrt(np.add(np.square(Q_arr), np.square(U_arr))), I_arr)*reverse_scale

    # Write the DOLP data to the final dictionary
    final_dict['observation_data']['DOLP_PARASOL'] = {}
    final_dict['observation_data']['DOLP_PARASOL']['scale'] = new_scale
    final_dict['observation_data']['DOLP_PARASOL']['long_name'] = 'Degree of linear polarization'
    final_dict['observation_data']['DOLP_PARASOL']['fill'] = fill
    final_dict['observation_data']['DOLP_PARASOL']['units'] = 'None'
    final_dict['observation_data']['DOLP_PARASOL']['data'] = np.round(DOLP_arr_unfltrd).astype(int)

    ## Q over I, U over I
    # Acquire and write our Q/I and U/I datasets to the final_dictionary along with metadata
    Q_over_I = np.divide(Q_arr, I_arr)*reverse_scale
    U_over_I = np.divide(U_arr, I_arr)*reverse_scale

    Q_over_I[np.where((measurement_dict['I_p']['data'] == fill) |
                              (measurement_dict['Q']['data'] == fill) |
                              (measurement_dict['U']['data'] == fill))] = fill

    U_over_I[np.where((measurement_dict['I_p']['data'] == fill) |
                              (measurement_dict['Q']['data'] == fill) |
                              (measurement_dict['U']['data'] == fill))] = fill

    final_dict['observation_data']['Q_over_I_PARASOL'] = {}
    final_dict['observation_data']['Q_over_I_PARASOL']['scale'] = new_scale
    final_dict['observation_data']['Q_over_I_PARASOL']['long_name'] = 'Q over I'
    final_dict['observation_data']['Q_over_I_PARASOL']['fill'] = fill
    final_dict['observation_data']['Q_over_I_PARASOL']['units'] = 'None'
    final_dict['observation_data']['Q_over_I_PARASOL']['data'] = np.round(Q_over_I).astype(int)

    final_dict['observation_data']['U_over_I_PARASOL'] = {}
    final_dict['observation_data']['U_over_I_PARASOL']['scale'] = new_scale
    final_dict['observation_data']['U_over_I_PARASOL']['long_name'] = 'U over I'
    final_dict['observation_data']['U_over_I_PARASOL']['fill'] = fill
    final_dict['observation_data']['U_over_I_PARASOL']['units'] = 'None'
    final_dict['observation_data']['U_over_I_PARASOL']['data'] = np.round(U_over_I).astype(int)

    # Change the directory to where you want to write the file to and convert the file in question
    os.chdir(write_path)
    write_nc(final_dict, write_name, verbose=True)


def write_nc(variable_dict, filename, verbose=False):
    """
    Writes a .NC file, a hierarchical data format used in L1C, with our newly formatted and aggregated data
    variable_dict = file_dict in the rest of the document
    When verbose = True it will output a running log of complete tasks
    """

    nc = Dataset(filename, mode='w', format='NETCDF4')

    for cat in variable_dict.keys():
        if verbose:
            print("Starting:", cat)

        # Create the category group to store the variables
        nc.createGroup(cat)

        for var in variable_dict[cat].keys():
            if verbose:
                print(var)

            shape = variable_dict[cat][var]['data'].shape

            # Fill the dimension with variables
            dimensions = []
            for i in range(len(shape)):
                dim_name = f'{var}_{i}'
                nc.createDimension(dim_name, size=shape[i])
                dimensions.append(dim_name)

            # Create the variable instance
            if verbose:
                print('creating variable')
            nc[cat].createVariable(var, datatype='i8', dimensions=dimensions, fill_value=variable_dict[cat][var]['fill'])

            # Create variable metadata
            if verbose:
                print('creating the metadataverse')
            nc[cat][var].long_name = variable_dict[cat][var]['long_name']
            nc[cat][var].units = variable_dict[cat][var]['units']
            nc[cat][var].scale_factor = variable_dict[cat][var]['scale']

            # Create variable array
            if verbose:
                print('creating variable array')
            nc[cat][var][:] = variable_dict[cat][var]['data']

            if verbose:
                print("")

    nc.close()
