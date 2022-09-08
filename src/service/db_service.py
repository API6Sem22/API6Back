import pymongo
from pymongo import MongoClient
from src.configs.database import Data_base_configs

class Db_service:

    @staticmethod
    def insert_db():
        post = {'Name':['Ramon'], 'Age':[21]}
        collection_name = Data_base_configs.get_collection_db()
        collection_name.insert_one(post)

    @staticmethod
    def get_data_db():
        collection_name = Data_base_configs.get_collection_db()
        results = collection_name.find()
        for result in results:
            print(result)



