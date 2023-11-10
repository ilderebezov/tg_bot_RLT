
import os
from datetime import datetime

import pandas as pd
import pymongo


def get_client() -> pymongo.mongo_client.MongoClient:
    """
    get mongodb client
    :return:
    """
    return pymongo.MongoClient(f"mongodb://{os.environ['DB_USER']}:{os.environ['DB_USER_PWD']}"
                               f"@{os.environ['DB_HOST']}:27017/")


def get_data_from_db(dt_iso_from: str, dt_iso_upto: str) -> pd.DataFrame:
    """
    Get data from mongodb
    :param dt_iso_from: from date
    :param dt_iso_upto: to date
    :return: DataFrame
    """
    collections = get_client()[os.environ['DB_NAME']][os.environ['COLLECTION']]
    find_collections = collections.find({"dt": {"$gte": datetime.fromisoformat(dt_iso_from),
                                                "$lte": datetime.fromisoformat(dt_iso_upto)}})
    return pd.DataFrame(list(find_collections))
