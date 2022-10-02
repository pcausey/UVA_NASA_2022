import dotenv
import os


dotenv.load_dotenv()

ICARE_USER = os.environ.get("icare_user")
ICARE_PASSWORD = os.environ.get("icare_pw")
ICARE_FTP_SITE = os.environ.get("icare_ftp")

GRASP_KEY = os.environ.get("grasp_key")

DATA_PATH = os.environ.get("data_path")
TMP_PATH = os.environ.get("tmp_path")

