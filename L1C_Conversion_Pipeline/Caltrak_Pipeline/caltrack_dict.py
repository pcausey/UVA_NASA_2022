from typing import NamedTuple
from L1C_Conversion_Pipeline.Caltrak_Pipeline.variables import *


class CalTrakColumn(NamedTuple):
    """ Class for holding the conversion dictionary column details"""
    outputName: str
    inputName: str
    folder: str
    wavelength: str = None


cal_list = [
    CalTrakColumn(outputName='latitude', inputName='Latitude',
                  folder=GEOLOCATION_DATA),
    CalTrakColumn(outputName='longitude', inputName='Longitude',
                  folder=GEOLOCATION_DATA),
    CalTrakColumn(outputName='time', inputName='Time',
                  folder=BIN_ATTRIBUTES)
]

cal_data_directional = [
    CalTrakColumn(outputName='solar_zenith', inputName='Solar_Zenith_Angle',
                  folder=GEOLOCATION_DATA),
    CalTrakColumn(outputName='sensor_zenith', inputName='View_Zenith_Angle_670P2',
                  folder=GEOLOCATION_DATA),
    CalTrakColumn(outputName='relative_azimuth', inputName='Relative_Azimuth_Angle_670P2',
                  folder=GEOLOCATION_DATA)
]

cal_I_non_polarized = [
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_443NP',
                  wavelength='443', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_1020NP',
                  wavelength='1020', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_565NP',
                  wavelength='565', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_763NP',
                  wavelength='763', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_765NP',
                  wavelength='765', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_NP, inputName='Normalized_Radiance_910NP',
                  wavelength='910', folder=OBSERVATION_DATA),
]

cal_I_polarized = [
    CalTrakColumn(outputName=I_P, inputName='Normalized_Radiance_490P',
                  wavelength='490', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_P, inputName='Normalized_Radiance_670P',
                  wavelength='670', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=I_P, inputName='Normalized_Radiance_865P',
                  wavelength='865', folder=OBSERVATION_DATA),
]

cal_q = [
    CalTrakColumn(outputName=Q, inputName='Q_Stokes_490P',
                  wavelength='490', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=Q, inputName='Q_Stokes_670P',
                  wavelength='670', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=Q, inputName='Q_Stokes_865P',
                  wavelength='865', folder=OBSERVATION_DATA),
]


cal_u = [
    CalTrakColumn(outputName=U, inputName='U_Stokes_490P',
                  wavelength='490', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=U, inputName='U_Stokes_670P',
                  wavelength='670', folder=OBSERVATION_DATA),
    CalTrakColumn(outputName=U, inputName='U_Stokes_865P',
                  wavelength='865', folder=OBSERVATION_DATA)
]

wavelength_dict = {
    I_P: cal_I_polarized,
    I_NP: cal_I_non_polarized,
    Q: cal_q,
    U: cal_u,
}
