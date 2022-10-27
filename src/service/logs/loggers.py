from datetime import datetime
from src.configs.database import Database_configs
import logging
import os


class LoggerConfiguration:
    datetime_now = datetime.now()
    filename = datetime.now().strftime("%Y-%m-%d") + ' ' + datetime_now.strftime('%Hh%Mm%S') + '.log'
    format_log = '[%(asctime)s] %(name)s %(levelname)s: %(message)s'
    logging.basicConfig(filename=filename,
                        filemode='a',
                        level=logging.DEBUG,
                        format=format_log,
                        datefmt='%Y-%m-%d %H:%M:%S')

    @staticmethod
    def save_logs():
        log_path = logging.getLoggerClass().root.handlers[0].baseFilename
        log_open = open(log_path, "r")
        lines = log_open.readlines()
        log_post = {'Message': [lines],
                    'DateTime': datetime.now()}
        collection = Database_configs.get_collection_db(False)
        collection.insert_one(log_post)