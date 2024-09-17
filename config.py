import logging
from dotenv import load_dotenv
import os
from pathlib import Path

from utils.db_conn import DatabaseConnector

BASE_PATH = Path(os.getcwd())
ASSET_PATH = BASE_PATH / "assets"
EXCEL_PATH = ASSET_PATH / "data.xlsx"
LOGO_PATH = ASSET_PATH / "logo.txt"
IMAGE_PATH = ASSET_PATH / "product_images"

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
                    handlers=[logging.StreamHandler()])

load_dotenv()

DB_SERVER   = os.environ.get("DB_SERVER")
DB_DATABASE = os.environ.get("DB_DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

rsa_db = DatabaseConnector(
    server = DB_SERVER,
    database = DB_DATABASE,
    username = DB_USERNAME,
    password = DB_PASSWORD
)

if not os.path.exists(IMAGE_PATH):
    os.makedirs(IMAGE_PATH)