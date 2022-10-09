from L1C_Conversion_Pipeline.Caltrak_Pipeline.caltrak_file import CaltrakFile
from L1C_Conversion_Pipeline.Grasp_Pipeline.grasp_file import GraspFile
from L1C_Conversion_Pipeline.EPA_Pipeline.epa_file import *
from L1C_Conversion_Pipeline.icare import *
from load_dotenv import TMP_PATH


class L1CFile:
    GRASP_PATH = TMP_PATH + 'grasp_files/'
    ICARE_CALTRAK_PATH = '/SPACEBORNE/CALIOP/CALTRACK-333m_PAR-L1B.v1.00'
    CALTRAK_PATH = TMP_PATH + 'caltrak_files/'
    EPA_PATH = TMP_PATH + 'epa_files/'
    EPA_FILE_NAME = 'epa_data_2008-2015.csv'
    L1C_PATH = TMP_PATH + 'l1c_files/'

    def __init__(self):
        pass

    def run_group_of_caltrak_to_l1c(self, year_str, date_str, verbose=True):

        # need to 'extract' date from caltrak_file_name
        caltrak_file_date = date_str.replace('_', '-')

        # Filter EPA Data to just a particular date
        #   Only need to do this once per 'Day's data
        epa = EPAFile(self.EPA_FILE_NAME, self.EPA_PATH, caltrak_file_date)

        # pull list of caltrak files:
        caltrak_files = self.return_files_in_caltrak_folder(year_str, date_str, self.CALTRAK_PATH)
        caltrak_files = caltrak_files[0:3]

        if verbose:
            print(caltrak_files)
            print("\n\n")

        for file_name in caltrak_files:
            self.run_single_l1c_file(file_name, year_str, date_str, verbose, epa)

    def run_single_l1c_file(self, file_name, year_str, date_str, verbose, epa):

        # download caltrak file
        tic = time.perf_counter()
        if verbose:
            print(f"Start Download: {file_name}")

        self.download_caltrak_file(year_str, date_str, self.CALTRAK_PATH, file_name)
        toc = time.perf_counter()

        if verbose:
            print(f"Completed Download: {toc - tic:0.4f} seconds")

        #    Pull a Caltrak File
        #    Run Caltrak data extraction -> final_dict
        tic = time.perf_counter()
        if verbose:
            print(f"Start Build Caltrak Final Dict")

        caltrak = CaltrakFile(self.CALTRAK_PATH + file_name)
        caltrak.build_final_dict()

        toc = time.perf_counter()
        if verbose:
            print(f"Completed Caltrak Final Dict: {toc - tic:0.4f} seconds")

        #    Co-locate EPA to Caltrak
        #       Find assoicated 'indexes' in EPA (distance function)
        #       Extract all datapoints and merge to Caltrak
        tic = time.perf_counter()
        if verbose:
            print(f"Start EPA Match")

        epa.run_epa_matching_to_caltrak(caltrak)
        caltrak.final_dict['epa_data'] = {}
        caltrak.final_dict['epa_data']['pm25'] = epa.epa_dict

        toc = time.perf_counter()

        if verbose:
            print(f"Completed EPA Match: {toc - tic:0.4f} seconds")

        #    Co-locate Grasp to Caltrak
        #       Find assoicated 'indexes' in Grasp
        #       Extract all datapoints and merge to Caltrak

        #    Save Output File
        tic = time.perf_counter()
        write_name = self.L1C_PATH + 'Processed_' + file_name.replace('.hdf', '.nc')
        if verbose:
            print(f"Start File Write: {write_name}")

        caltrak.write_nc(caltrak.final_dict, write_name, verbose=verbose)

        toc = time.perf_counter()
        if verbose:
            print(f"Completed File Write: {toc - tic:0.4f} seconds")

        # delete caltrak file from TMP Folder
        self.delete_file(self.CALTRAK_PATH, file_name)

    def return_files_in_caltrak_folder(self, year_str, date_str, download_path):

        # Hard code CALTRACK data location
        ftp_path = self.ICARE_CALTRAK_PATH

        # Start an iCare Session
        sess = ICARESession(download_path)

        matching_files = sess.listdir(f"{ftp_path}/{year_str}/{date_str}")

        sess.ftp.close()

        return matching_files

    def download_caltrak_file(self, year_str, date_str, download_path, file_name):

        ftp_path = self.ICARE_CALTRAK_PATH

        # Start an iCare Session
        sess = ICARESession(download_path)

        ftp_full_path = f"{ftp_path}/{year_str}/{date_str}/{file_name}"

        sess.ftp.retrbinary('RETR %s' % ftp_full_path,
                            open(download_path + file_name, 'wb').write)

    @staticmethod
    def delete_file(file_dir, file_name):
        import os

        if os.path.exists(file_dir + file_name):
            os.remove(file_dir + file_name)
            print(f"{file_dir + file_name} deleted successfully")
        else:
            print(f"{file_dir + file_name} does not exist!")
