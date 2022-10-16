import logging
from datetime import datetime
from src.configs.database import Database_configs
from src.service.csv_service import Csv_service


class Db_service:
    datetime_now = datetime.now()
    filename = datetime.now().strftime("%Y-%m-%d") + ' ' + datetime_now.strftime('%Hh%Mm%S') + '.log'
    format_log = '[%(asctime)s] %(name)s %(levelname)s: %(message)s'
    logging.basicConfig(filename=filename,
                        filemode='a',
                        level=logging.DEBUG,
                        format=format_log,
                        datefmt='%Y-%m-%d %H:%M:%S')

    global logger
    logger = logging.getLogger()

    @staticmethod
    def insert_into_db():
        post = Csv_service.read_csv()
        collection_name = Database_configs.get_collection_db()
        try:
            collection_name.insert_many(post)
        except:
            logger.error('Insert error')


    @staticmethod
    def get_data_db():
        collection_name = Database_configs.get_collection_db()
        results = collection_name.find()
        for result in results:
            print(result)


Db_service.insert_into_db()