from L1C_Conversion_Pipeline.caltrack_dict import list_caltrack, LAT_LONG_ALT
import h5py
from pyhdf.SD import SD, SDC
import os
import numpy as np

HDF4 = '.hdf'
HDF5 = '.h5'


class ConvertHDFFile:
    def __init__(self, pixels, file_path):
        self.pixels = self.check_pixels(pixels)
        self.file_type = self.parse_file_type(file_path)
        self.file = self.return_file(file_path, self.file_type)

        # This initializes the dictionary where we'll store all of our information before we cast it to an HDF file.
        # A dictionary is a natural analog for an HDF file since it also utilizes hierarchical organization
        self.final_dict = {}
        self.final_dict['geolocation_data'] = {}

    def convert_file(self):
        """ Converts a file from HDF to L1C """

        # Caltrack files are already centered;
        # Only need to center 'Polder' files
        # inds = np.where(np.abs(col_values)[0] != '-Infinity')
        # vals = col_values[i][inds]
        # TODO: Figure out how this works in Polder files
        if self.file_type == HDF5:
            geoshape = self.center_view(self.file)

        # For Column in Map
        for col in list_caltrack:

            # Process File
            self.final_dict = self.process_column(col, self.final_dict)

            print('hurray')

        # Write to L1C file

    @staticmethod
    def check_pixels(pixels):
        # Validation on the pixel count, making sure it's an odd number

        if pixels <= 0 or pixels % 1 != 0:
            print("Error, enter a valid pixel number, pixels being set to 3")
            pixels = 3
        elif pixels % 2 == 0 and pixels > 2:
            print("Error, even number of pixels chosen. Pixels being reset")
            pixels -= 1

        return pixels

    @staticmethod
    def parse_file_type(file_path):
        split_tup = os.path.splitext(file_path)
        file_type = split_tup[1]

        return file_type

    @staticmethod
    def return_file(file_path, file_type):
        """ Check if file is hdf5 or hdf4

            Inputs: file_path - where file is located on computer
            Outputs:
                f - actual file that we're going to use
                file_type - extension used by other functions to access data
        """

        if file_type == HDF5:
            f = h5py.File(file_path, "r")
        elif file_type == HDF4:
            f = SD(file_path, SDC.READ)
        else:
            raise Exception("Unknown File Type")

        return f

    def parse_field(self, field):
        """ Return field values from file based on file_type

            Inputs: file, field, file_type
            Outputs: List of field values
        """

        if self.file_type == HDF5:
            output = self.file[field]
        elif self.file_type == HDF4:
            output = self.file.select(field)
        else:
            raise Exception("Unknown File Type")

        return output

    def process_column(self, col, tmp_dict):

        col_values = self.parse_field(
            field=col.inputName
        )

        if col.function_to_process == LAT_LONG_ALT:
            tmp_dict = self.process_lat_long_altitude(col_values, col, tmp_dict)
        # elif col.function_to_process == I_Q_U:
        #     tmp_dict = process_i_q_u(col_values, col, tmp_dict)

        return tmp_dict

    @staticmethod
    def center_view(file):

        # Get latitude array and corresponding indices
        # We only want to look at datapoints for which there are legitimate coordinates
        # Here we'll compile a group of indices that correspond to the satellites center of vision
        # Width of the vision is set by "pixels" above

        # Geo inds will be the compiled list of centered indices
        geo_inds = [[], []]

        lats = np.array(file['Geolocation_Fields']['Latitude'])
        lats
        lons = np.array(file['Geolocation_Fields']['Longitude'])

        flag = True

        # Loop through our latitudes
        for i in range(lats.shape[0]):
            # caltrack geolocation data looks different. Uses -Infinity as the fill value

            # Where lats[i] is not equal to fill values; return first element in list
            inds = np.where(np.abs(lats[i]) != 99999)[0]

            #
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
            center_ind = int(np.sum(inds) / len(inds))

            # If there aren't any surrounding values continue
            # Otherwise append our relevant indices list
            if center_ind < 1:
                continue
            else:
                temp_lats = lats[i][center_ind - (pixels - 2):center_ind + (pixels - 1)]
                geo_inds[0].extend([i for u in range(3)])
                geo_inds[1].extend([u for u in range(center_ind - 1, center_ind + 2)])

            if flag:
                new_lats = np.array([temp_lats])
                flag = False
            else:
                new_lats = np.append(new_lats, np.array([temp_lats]), axis=0)

        # Set the shape of our tensor for later
        geo_shape = (new_lats.shape)

        return geo_shape

    @staticmethod
    def rescale_column(fill_val, temp_arr, reverse_scale, new_fill):
        """ Rescale all values in a column; replace fill values as needed """

        # Find indices where fill value is used
        fill_inds = np.where(temp_arr == fill_val)

        # Reverse the Scale and 'unscale' temp_arr
        # TODO: Why are we reversing the Scale?
        temp_arr = temp_arr * reverse_scale
        temp_arr[fill_inds] = new_fill

        temp_arr = np.round(temp_arr).astype(int)

        return temp_arr

    def process_lat_long_altitude(self, col_values, col, final_dict):
        """ Angles, Altitude, & Location
            We want to store as much information in integer format with scale factors as possible
            These variables are set as conversion factors to go between float and a reasonably accurae integer
        """

        final_dict['geolocation_data'][col.outputName] = {}

        for item in ['long_name', 'units']:
            final_dict['geolocation_data'][col.outputName][item] = col_values.attributes()[item]

        fill_val = col_values.attributes()['_FillValue']

        # Array of values from File
        temp_arr = np.array(col_values.get())

        if col.outputName in ['lats', 'lons']:
            # If Lats and Lons, let's Rescale

            reverse_scale = 1000
            new_scale = 1 / reverse_scale
            # New Fill must be an Integer
            new_fill = 99999999

            rescaled_col = self.rescale_column(fill_val, temp_arr, reverse_scale, new_fill)

            final_dict['geolocation_data'][col.outputName]['fill'] = new_fill

            final_dict['geolocation_data'][col.outputName]['scale'] = new_scale

            # Orig: np.round(temp_arr[geo_inds].reshape(geo_shape)).astype(int)
            # But we're not reshaping to center anything
            final_dict['geolocation_data'][col.outputName]['data'] = rescaled_col

        elif col.outputName in ['surface_altitude']:

            # Orig: f['Geolocation_Fields'][col.outputName].attrs['scale_factor']
            final_dict['geolocation_data'][col.outputName]['scale'] = col_values.attributes()['scale_factor']

            # Orig: f['Geolocation_Fields'][col.outputName].attrs[_FillValue']
            final_dict['geolocation_data'][col.outputName]['fill'] = fill_val

            # Orig: (np.array(f['Geolocation_Fields'][field]))[geo_inds].reshape(geo_shape)
            final_dict['geolocation_data'][col.outputName]['data'] = temp_arr

        return final_dict


if __name__ == '__main__':

    pixels = 3
    file_path = '/Users/pcause/Downloads/CALTRACK-333m_PAR-L1B_V1-00_2009-07-04T15-09-32ZD.hdf'
    # file_path = '/Users/pcause/Downloads/POLDER3_L1B-BG1-080160M_2008-06-01T23-12-43_V1-00.h5'

    l1c_conv = ConvertHDFFile(pixels, file_path)
    l1c_conv.convert_file()

