import pickle
import os

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import io
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import shutil
from tabulate import tabulate
from load_dotenv import CRED_PATH


class GoogleDrive:
    # If modifying these scopes, delete the file token.pickle.
    # SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self):
        self.service = self.get_gdrive_service()

    def get_gdrive_service(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(CRED_PATH + 'token.pickle'):
            with open(CRED_PATH + 'token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CRED_PATH + 'google-api-credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(CRED_PATH + 'token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        # return Google Drive API service
        return build('drive', 'v3', credentials=creds)

    def list_available_files(self):
        """Shows basic usage of the Drive v3 API.
        Prints the names and ids of the first 5 files the user has access to.
        """
        # Call the Drive v3 API
        results = self.service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="files(id, driveId, name, mimeType, size, parents, modifiedTime)",
            q="parents in '1E_OlVsKhnbovurUkSXcINN3A0Y5QaQlu'").execute()

        for item in results['files']:
            item['file_date'] = self.return_file_date(item['name'])

        return results

    @staticmethod
    def match_string(find, in_str):
        import re
        match = re.search(find, in_str)
        if match:
            return match

    def return_file_date(self, file_name):
        from datetime import datetime
        found_date = self.match_string("([0-9]{4}[0-9]{2}[0-9]{2})", file_name)
        if found_date:
            # found_date = datetime.strptime(found_date[0], '%Y%m%d')
            return found_date[0]

    def list_files(self, items):
        """given items returned by Google Drive API, prints them in a tabular way"""
        if not items:
            # empty drive
            print('No files found.')
        else:
            rows = []
            for item in items:
                # get the File ID
                id = item["id"]
                # get the name of file
                name = item["name"]
                try:
                    # parent directory ID
                    parents = item["parents"]
                except:
                    # has no parrents
                    parents = "N/A"
                try:
                    # get the size in nice bytes format (KB, MB, etc.)
                    size = self.get_size_format(int(item["size"]))
                except:
                    # not a file, may be a folder
                    size = "N/A"
                # get the Google Drive type of file
                mime_type = item["mimeType"]
                # get last modified date time
                modified_time = item["modifiedTime"]
                # append everything to the list
                rows.append((id, name, parents, size, mime_type, modified_time))
            print("Files:")
            # convert to a human readable table
            table = tabulate(rows, headers=["ID", "Name", "Parents", "Size", "Type", "Modified Time"])
            # print the table
            print(table)

    @staticmethod
    def get_size_format(b, factor=1024, suffix="B"):
        """
        Scale bytes to its proper byte format
        e.g:
            1253656 => '1.20MB'
            1253656678 => '1.17GB'
        """
        for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
            if b < factor:
                return f"{b:.2f}{unit}{suffix}"
            b /= factor
        return f"{b:.2f}Y{suffix}"

    def file_download(self, file_id, file_name):
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()

        # Initialise a downloader object to download the file , chunksize=204800
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        try:
            # Download the data in chunks
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)

            # Write the received data to the file
            with open(file_name, 'wb') as f:
                shutil.copyfileobj(fh, f)

            print("File Downloaded")
            # Return True if file Downloaded successfully
            return True
        except:

            # Return False if something went wrong
            print("Something went wrong.")
            return False

    @staticmethod
    def unzip_file(zip_path, unzip_path):
        import zipfile
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)

    @staticmethod
    def list_files_in_folder(folder_path):
        from os import listdir
        from os.path import isfile, join
        onlyfiles = [f for f in listdir(folder_path) if isfile(join(folder_path, f))]

        return onlyfiles
