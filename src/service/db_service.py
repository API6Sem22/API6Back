import logging
from src.configs.database import Database_configs
from src.service.csv_service import Csv_service
from src.service.logs.loggers import LoggerConfiguration
from crypto.crypto import crypto


class Db_service:

    global logger
    logger = logging.getLogger()

    @staticmethod
    def insert_into_db():
        post = Csv_service.read_csv()
        collection_name = Database_configs.get_collection_db(True)
        # for index, row in post.iterrows():
        #     enc_nome, enc_contrato = crypto.encrypt(row["marca_otica"], row["nXmX_bXnXfXWXXrXX"], row["cod_contrato"])
        #     post.loc[post["nXmX_bXnXfXWXXrXX"] == row["nXmX_bXnXfXWXXrXX"], "nXmX_bXnXfXWXXrXX"] = enc_nome.decode('utf-8')
        #     post.loc[post["cod_contrato"] == row["cod_contrato"], "cod_contrato"] = enc_contrato.decode('utf-8')
        try:
            collection_name.insert_many(post)
            logger.info('Insert in database with success!')
        except Exception as e:
            logger.error('Insert error: ' + str(e))
        finally:
            LoggerConfiguration.save_logs()



    @staticmethod
    def get_data_db():
        collection_name = Database_configs.get_collection_db()
        results = collection_name.find()
        for result in results:
            print(result)


