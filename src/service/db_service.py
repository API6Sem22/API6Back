import pymongo
from pymongo import MongoClient
from src.configs.database import Data_base_configs
from src.service.csv_service import Csv_service


class Db_service:

    @staticmethod   
    def insert_db():
        post = Csv_service.read_csv()
        print(post)
        collection_name = Data_base_configs.get_collection_db()
        collection_name.insert_many(post)

    @staticmethod
    def get_data_db():
        collection_name = Data_base_configs.get_collection_db()
        results = collection_name.find()
        for result in results:
            print(result)


Db_service.insert_db()
