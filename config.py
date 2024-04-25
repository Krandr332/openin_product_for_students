
from os import getenv
import json

import pymongo


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SAMPLE_SPREADSHEET_ID = "*****"
SAMPLE_RANGE_NAME = "*******"

ROOT = getenv("****", "*******")
LOCAL_FILES = f"local_files"

username = "***"
password = "****"

_mongo_account = json.load(open(f'mongo_data.json'))
_login, _password = _mongo_account['login'], _mongo_account['password']
CA_FILE = f"CA.pem"
CONNECTION_STRING = f"mongodb://{_login}:{_password}@rc1b-okazhb06hqauc9ep.mdb.yandexcloud.net:*****8/?replicaSet=rs01&authSource=google_services"
DB_NAME = "google_services"

# Настройки Google Sheets
_collection = pymongo.MongoClient(CONNECTION_STRING, ssl=True, tlsCAFile=CA_FILE)["google_services"]["service_accounts"]
TABLE_CONFIG_T = _collection.find_one({"name": "counting_messages_bot_t"})
TABLE_KEY_LIST = "*****"

creds_info = {
        'token': TABLE_CONFIG_T['token'],
        'refresh_token': TABLE_CONFIG_T['refresh_token'],
        'client_id': TABLE_CONFIG_T['client_id'],
        'client_secret': TABLE_CONFIG_T['client_secret'],
        'scopes': TABLE_CONFIG_T['scopes']
    }
spreadsheet_id = "**********"
sheet_id = "**********"