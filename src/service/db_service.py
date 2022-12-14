import logging
from src.configs.database import Database_configs
from src.service.csv_service import Csv_service
from src.service.logs.loggers import LoggerConfiguration


class Db_service:

    global logger
    logger = logging.getLogger()

    @staticmethod
    def insert_into_db():
        post = Csv_service.read_csv()
        collection_name = Database_configs.get_collection_db(True)
        try:
            collection_name.insert_many(post)
            logger.info('Insert in database with success!')
        except Exception as e:
            logger.error('Insert error: ' + str(e))
        finally:
            logger.info('Persisti logs in database')
            LoggerConfiguration.save_logs()
