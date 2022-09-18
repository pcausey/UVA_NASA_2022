from typing import NamedTuple

I_Q_U = 'I_Q_U'
LAT_LONG_ALT = 'LatLongAlt'


class HDFColumn(NamedTuple):
    """ Class for holding the conversion dictionary column details"""
    outputName: str
    inputName: str
    unpack: bool
    list_str_unpack: list
    list_wavelength_unpack: list
    function_to_process: str


list_caltrack = [
    HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[],
              list_wavelength_unpack=[], function_to_process=LAT_LONG_ALT),
    HDFColumn(outputName='lons', inputName='Longitude', unpack=False, list_str_unpack=[],
              list_wavelength_unpack=[], function_to_process=LAT_LONG_ALT),
    # TODO: Find Surface Altitude in CalTrack
    # HDFColumn(outputName='surface_altitude', inputName='Longitude', unpack=False, list_str_unpack=[], list_wavelength_unpack=[]),

    HDFColumn(outputName='i490', inputName='Normalized_Radiance_490P', unpack=False, list_str_unpack=[],
              list_wavelength_unpack=[], function_to_process=I_Q_U),

    HDFColumn(outputName='i', inputName='Normalized_Radiance_', unpack=True,
              list_str_unpack=['P'],
              list_wavelength_unpack=['490', '670', '865'],
              function_to_process=I_Q_U)

    # HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[], list_wavelenght_unpack=[]),
    # HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[], list_wavelenght_unpack=[]),
    # HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[], list_wavelenght_unpack=[]),
    # HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[], list_wavelenght_unpack=[]),
    # HDFColumn(outputName='lats', inputName='Latitude', unpack=False, list_str_unpack=[], list_wavelenght_unpack=[]),
    #

]


# Output Name
# Input Listing
# Do we need to unpack? [yes/no]
# list of items to unpack (NP vs P, wavelengths)
# list to unpack

