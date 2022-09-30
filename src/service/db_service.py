from src.configs.database import Database_configs
from src.service.csv_service import Csv_service


class Db_service:

    @staticmethod
    def insert_db():
        post = Csv_service.read_csv()
        collection_name = Database_configs.get_collection_db()
        collection_name.insert_many(post)

    @staticmethod
    def get_data_db():
        collection_name = Database_configs.get_collection_db()
        results = collection_name.find()
        for result in results:
            print(result)


Db_service.insert_db()
