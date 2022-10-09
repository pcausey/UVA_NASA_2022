"""Utilities for interacting with FTP servers."""

import getpass
import os
import re
import shutil
import time
import subprocess

from datetime import datetime
from ftplib import FTP, error_perm, error_temp

from load_dotenv import ICARE_USER, ICARE_PASSWORD, ICARE_FTP_SITE, GRASP_KEY

import h5py
# from pyhdf.SD import SD, SDC

"""
Need to have .env file setup with filled in fields:
    icare_user=<USERNAME>
    icare_pw=<PASSWORD>
    icare_ftp=ftp.icare.univ-lille1.fr
"""


def datetime_to_subpath(dt: datetime) -> str:
    """Get the year/day folder ICARE subpath from a datetime."""
    return os.path.join(str(dt.year), f"{dt.year}_{dt.month:02d}_{dt.day:02d}") + "/"


class ICARESession:
    """ICARESession manages a session with the ICARE FTP server. Uses a cache to minimize requests.

    To use this, you need a login with ICARE. See: https://www.icare.univ-lille.fr/data-access/data-archive-access
    """

    SUBDIR_LOOKUP = {
        "SYNC": "CALIOP/CALTRACK-5km_PAR-RB2/",  # directory of CALTRACK / PARASOL merge, has time sync data
        "CLDCLASS": "CALIOP/CALTRACK-5km_CS-2B-CLDCLASS/",  # directory of CLDCLASS level 2B dataset
        "PAR": "PARASOL/L1_B-HDF/",  # directory of PARASOL level 1B dataset
    }

    def __init__(self, temp_dir: str, max_temp_files: int = 20) -> None:
        """Create an ICARESession.

        Args:
            temp_dir: Path to the temporary directory to use as a cache.
        """
        self.temp_dir = temp_dir
        self.max_temp_files = max_temp_files
        self.temp_files = []
        for dirpath, _, filenames in os.walk(self.temp_dir):
            self.temp_files += [os.path.join(dirpath, f) for f in filenames]
        self.ftp = None
        self.login()
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir, exist_ok=True)
        self.dir_tree = {}  # keep track of the directory tree of ICARE to cut down on FTP calls

    # def __del__(self):
    #     self.cleanup()

    def _get_rec(self, subdict, key_list):
        """Recursive get."""
        if key_list[0] not in subdict:
            return {}
        if len(key_list) == 1:
            return subdict[key_list[0]]
        return self._get_rec(subdict[key_list[0]], key_list[1:])

    def _set_rec(self, subdict, key_list, val):
        """Recursive set."""
        if len(key_list) == 1:
            subdict[key_list[0]] = val
            return subdict
        if key_list[0] not in subdict:
            subdict[key_list[0]] = {}
        subdict[key_list[0]] = self._set_rec(subdict[key_list[0]], key_list[1:], val)
        return subdict

    def _dump_temp_files(self) -> None:
        """If we already have too many temp files, delete the first one."""
        while len(self.temp_files) >= self.max_temp_files:
            os.remove(self.temp_files[0])
            self.temp_files = self.temp_files[1:]

    def login(self) -> None:
        """Log in to the ICARE FTP server, prompting user for credentials."""
        self.ftp = FTP(ICARE_FTP_SITE)
        logged_in = False
        attempts = 0
        while not logged_in:
            attempts += 1
            if attempts > 10:
                raise Exception("Too many failed login attempts!!")
            try:
                # if os.path.exists("icare_credentials.txt"):
                #     username, password = open("icare_credentials.txt").read().split("\n")
                # else:
                #     username = input("ICARE Username:")
                #     password = getpass.getpass("ICARE Password:")
                self.ftp.login(ICARE_USER, ICARE_PASSWORD)
                logged_in = True
                self.ftp.cwd("SPACEBORNE")
            except error_perm as e:
                print(e)

    def cleanup(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        # while os.path.exists(self.temp_dir):
        #     time.sleep(0.1)

    def listdir(self, dir_path: str) -> list:
        """List the contents of a directory, with a cache.

        Args:
            dir_path: Path to the directory

        Returns:
            listing: Directory listing
        """
        split_path = [s for s in dir_path.split("/") if s != ""]
        listing = self._get_rec(self.dir_tree, split_path)
        if listing == {}:
            err_code = None
            attempts = 0
            while err_code in [None, "421", "430", "434"]:
                attempts += 1
                if attempts > 10:
                    raise Exception("Too many failed listdir attempts!!")
                try:
                    nlst = self.ftp.nlst(dir_path)
                    break
                except error_temp as e:
                    print(e)
                    err_code = str(e)[:3]
                    if err_code in ["421", "430", "434"]:
                        self.login()
                    else:
                        raise FileNotFoundError(f"Could not find {dir_path} in ICARE server.")
            listing = sorted([f.split("/")[-1] for f in nlst])
            listing_dict = {}
            for l in listing:
                # check for a file extension
                if len(l) > 5 and "." in l[-5:-1]:
                    listing_dict[l] = l
                else:
                    listing_dict[l] = {}
            self.dir_tree = self._set_rec(self.dir_tree, split_path, listing_dict)
        return listing

    def get_file(self, filepath: str) -> str:
        # if the file doesn't exist, download it
        # Build output path based on tmp_dir passed in at object creation and filepath (dated folder)
        local_path = os.path.join(self.temp_dir, filepath)

        if not os.path.exists(local_path):
            self._dump_temp_files()
            self.temp_files.append(local_path)

            # recursively make directories to this file
            make_dir_path = os.path.join(self.temp_dir, os.path.split(filepath)[0])
            os.makedirs(make_dir_path, exist_ok=True)

            temp_file = open(local_path, "wb")

            err_code = None
            attempts = 0
            while err_code in [None, "421", "430", "434"]:
                attempts += 1
                if attempts > 10:
                    raise Exception("Too many get file attempts!!")
                try:
                    self.ftp.retrbinary("RETR " + filepath, temp_file.write)
                    break
                except error_temp as e:
                    err_code = str(e)[:3]
                    if err_code in ["421", "430", "434"]:
                        self.login()
                    else:
                        raise FileNotFoundError(f"Could not find {filepath} in ICARE server.")
                except error_perm as e:
                    err_code = str(e)[:3]
                    if err_code == "550":
                        raise FileNotFoundError(f"Could not find {filepath} in ICARE server.")
            temp_file.close()
        return local_path
    
    
def get_caltrack_files_from_grasp_day(grasp_date: str, download = False) -> list:
    """
    Takes a GRASP date ("2008-08-05") and returns all CALTRACK files that match
    that date
    
    Inputs:
        grasp_date (str) - date to match files on (YYYY-MM-DD)
        download (bool)  - if True: download all files in matching directory
    
    Outputs:
        matching_files (list(str)) - list of files that match date from FTP
        
    """
    # Create a download folder based on date
    download_path = f'CALTRACK_download_{grasp_date}'
    
    # Hard code CALTRACK data location
    caltrack_path = '/SPACEBORNE/CALIOP/CALTRACK-333m_PAR-L1B.v1.00'
    
    # Start an iCare Session
    sess = ICARESession(download_path)
    
    # Create objects for use in path
    grasp_date_repl = grasp_date.replace('-', '_')
    grasp_date_obj = datetime.strptime(grasp_date, '%Y-%m-%d')
    
    # Obtain all file names that match based on date
    matching_files = sess.listdir("{caltrack_path}/{grasp_year}/{grasp_date}".format(
                                        caltrack_path = caltrack_path,
                                        grasp_year = grasp_date_obj.year,
                                        grasp_date = grasp_date_repl))
    
    # Download all files if True
    if download == True:
        for file in matching_files:
            sess.get_file("{caltrack_path}/{grasp_year}/{grasp_date}/{caltrack_file}".format(
                                        caltrack_path = caltrack_path,
                                        grasp_year = grasp_date_obj.year,
                                        grasp_date = grasp_date_repl,
                                        caltrack_file = file))
        
    return matching_files


def return_file_names_in_dir(dir_path):
    from os import listdir
    from os.path import isfile, join

    only_files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]

    return only_files


def get_grasp_files(filter_string, download_path, verbose = False):
    """
    SUMMARY: downloads a daily L2 file from the grasp-open website given a 
             filter string and download path
             
             base website: https://download.grasp-cloud.com/download/polder/
             
             default path: https://download.grasp-cloud.com/download/polder/polder-3/models/v2.1/l2/daily/
             
             ** ENSURE YOU HAVE GRASP_KEY SET IN .ENV FILE **
             
    INPUTS: filter_string (str) - filter for the query, limits the files to retrieve
            download_path (str) - directory path for download 
            
    OUTPUTS: no objects, creates a file structure that mirrors the GRASP website 
             within the download_path path
    
    """
    # Change current directory to download_path
    os.chdir(download_path)
    query_string = ''
    
    # Create the query string for the wget method
    try:
        query_string = 'wget --recursive --no-parent "https://{GRASP_KEY}@download.grasp-cloud.com/basic/polder/polder-3/models/v2.1/l2/daily/?filter={filter_string}"'.format(
            GRASP_KEY = GRASP_KEY,
            filter_string = filter_string
        )
    
    except NameError:
        print('GRASP_KEY is not found in .env file, add to file and retry')
    
    if verbose: 
        print(query_string)
        print("Querying using filter: {filter_string}".format(filter_string = filter_string))
        
    # Use subprocess to execute the wget command 
    subprocess.Popen(query_string)
    
    if verbose: 
        print("Query complete")

    # Return no python object
    return return_file_names_in_dir(download_path)
    
    
                               
                               
                               
                               
                               
                               
                               
                               
