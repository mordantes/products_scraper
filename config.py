import os

from dotenv import load_dotenv


load_dotenv()

C_HOST = os.environ.get("C_HOST")
C_USER = os.environ.get("C_USER")
C_PWD = os.environ.get("C_PWD")
C_PORT = os.environ.get("C_PORT")
C_DB = os.environ.get("C_DB")
C_TABLE = os.environ.get("C_TABLE")
BASE_DIR = os.environ.get("BASE_DIR")
