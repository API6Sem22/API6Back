import logging
import pandas as pd
import re

class Csv_service:

    global logger
    logger = logging.getLogger()

    @staticmethod
    def read_csv():
        logger.info('read a csv')
        try:
            url_csv = ""

            data = pd.read_csv(url_csv, sep=',',
                            low_memory=False)
            idFile = data['marca_otica']
            for id in idFile:
                logger.info('Get a file id: ' + str(id))
            return Csv_service.transform_fields(data)
        except FileNotFoundError as error:
            logger.info('Error file not found')
            return error
        except ValueError as error:
            logger.error('Parser error during convertion')
            return error

    @staticmethod
    def transform_fields(data):
        columns = pd.DataFrame(data)
        transform_columns_data = []
        transform_columns_value = []
        for col in columns:
            if 'dt_' in col:
                transform_columns_data.append(col)
        for item in transform_columns_data:
            if re.search("NaN", str(data[item])) == None:
                data[item] = pd.to_datetime(data[item])
    

        for col in columns:
            if 'valor' in col: 
                transform_columns_value.append(col)
        for item in transform_columns_value:
            data[item] = data[item].astype('double')
        return data
