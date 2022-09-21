from L1C_Conversion_Pipeline.icare import ICARESession
from load_dotenv import DATA_PATH, TMP_PATH
from other_pipelines.L1C_Formatting_Centered_adjusted import output_centered_l1c_file


def my_script():

    icare_file_name = 'POLDER3_L1B-BG1-080146M_2008-06-01T00-08-19_V1-00.h5'
    icare_file_folder = 'PARASOL/L1_B-HDF.v1.00/2008/2008_06_01/'

    box_folder = ''
    box_file_name = ''

    # Connect To iCare FTP
    icare = ICARESession(temp_dir=TMP_PATH)

    file_list = icare.listdir(icare_file_folder)

    # Download file to 'tmp_dir'
    for item in file_list:
        file_path = icare_file_folder + item
        # icare.get_file(filepath=file_path)

        write_path = DATA_PATH + icare_file_folder
        write_name = item[:-4] + '.nc'

        # Convert into L1C format
        output_centered_l1c_file(
            directory=TMP_PATH + icare_file_folder,
            file_path=file_path,
            write_path=write_path,
            write_name=write_name
        )

        # cls = ConvertHDFFile(3, TMP_PATH + icare_file_folder)
        # cls.convert_file()

        # Post to Box folder

    print('y')


if __name__ == '__main__':
    my_script()
