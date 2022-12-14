import logging

from pymongo import MongoClient
from dotenv import load_dotenv
import os


class Database_configs:
    global logger
    logger = logging.getLogger()

    @staticmethod
    def create_connection_db():
        load_dotenv()
        try:
            logger.info('Opening database connection')
            url_mongodb = os.environ['BANCO_CREDENTIALS']
            logging.info('Establishing connection')
            cluster = MongoClient(url_mongodb)

            db = cluster[os.environ['CLUSTER']]
            logging.info('Get a cluster')
            return db

        except Exception as e:
            logger.error('Connection error, bad credentials ')
            return Exception

    @staticmethod
    def get_collection_db(bool: bool):
        try:
            logger.info('Create a connection')
            collection = Database_configs.create_connection_db()

            logger.info('Get a collection name')
            if bool:
                return collection[os.environ['COLLECTION']]
            return collection[os.environ['COLLECTION_LOGS']]
        except Exception as e:
            logger.error('error while get a collection name')

