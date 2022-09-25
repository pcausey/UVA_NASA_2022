from typing import NamedTuple


class GRASPColumn(NamedTuple):
    """ Class for holding the conversion dictionary column details"""
    outputName: str
    inputName: str
    folder: str
    

grasp_list = [
    GRASPColumn(outputName='latitude', inputName='Latitude', folder='grasp'),
    GRASPColumn(outputName='longitude', inputName='Longitude', folder='grasp'),
    GRASPColumn(outputName='pm25', inputName='PM25', folder='grasp'),
    GRASPColumn(outputName='time', inputName='Time', folder='grasp'),
    GRASPColumn(outputName='AOD443', inputName='AOD443', folder='grasp'),
    GRASPColumn(outputName='AOD490', inputName='AOD490', folder='grasp'),
    GRASPColumn(outputName='AOD565', inputName='AOD565', folder='grasp'),
    GRASPColumn(outputName='AOD670', inputName='AOD670', folder='grasp'),
    GRASPColumn(outputName='AOD865', inputName='AOD865', folder='grasp'),
    GRASPColumn(outputName='AOD1020', inputName='AOD1020', folder='grasp'),
    GRASPColumn(outputName='AAOD443', inputName='AAOD443', folder='grasp'),
    GRASPColumn(outputName='AAOD490', inputName='AAOD490', folder='grasp'),
    GRASPColumn(outputName='AAOD565', inputName='AAOD565', folder='grasp'),
    GRASPColumn(outputName='AAOD670', inputName='AAOD670', folder='grasp'),
    GRASPColumn(outputName='AAOD865', inputName='AAOD865', folder='grasp'),
    GRASPColumn(outputName='AAOD1020', inputName='AAOD1020', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD443', inputName='CoarseAOD443', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD490', inputName='CoarseAOD490', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD565', inputName='CoarseAOD565', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD670', inputName='CoarseAOD670', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD865', inputName='CoarseAOD865', folder='grasp'),
    GRASPColumn(outputName='CoarseAOD1020', inputName='CoarseAOD1020', folder='grasp'),
    GRASPColumn(outputName='FineAOD443', inputName='FineAOD443', folder='grasp'),
    GRASPColumn(outputName='FineAOD490', inputName='FineAOD490', folder='grasp'),
    GRASPColumn(outputName='FineAOD565', inputName='FineAOD565', folder='grasp'),
    GRASPColumn(outputName='FineAOD670', inputName='FineAOD670', folder='grasp'),
    GRASPColumn(outputName='FineAOD865', inputName='FineAOD865', folder='grasp'),
    GRASPColumn(outputName='FineAOD1020', inputName='FineAOD1020', folder='grasp')
]
