from pymongo import MongoClient
from dotenv import load_dotenv
from src.utils.constants import DbEnum
import os


class Database_configs:

    @staticmethod
    def create_connection_db():
        load_dotenv()
        url_mongodb = os.environ['BANCO_CREDENTIALS']
        cluster = MongoClient(url_mongodb)
        db = cluster[DbEnum.CLUSTER.value]
        return db

    @staticmethod
    def get_collection_db():
        collection = Database_configs.create_connection_db()
        return collection[os.environ['COLLECTION']]
