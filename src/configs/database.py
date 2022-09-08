import pymongo
from pymongo import MongoClient

class Data_base_configs:

    @staticmethod
    def create_connection_db():
        url_mongodb = "mongodb+srv://d-end:six-d-end@api6semestre.aypedm0.mongodb.net/?retryWrites=true&w=majority"
        cluster = MongoClient(url_mongodb)
        db = cluster["Medical"]
        return db

    @staticmethod
    def get_collection_db():
        cluster = Data_base_configs.create_connection_db()
        collection = cluster["medical_insurance_amil_mensalidade"]
        return collection
