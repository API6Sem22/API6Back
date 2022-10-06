import pandas as pd


class Csv_service:

    @staticmethod
    def read_csv():
        url_csv = "C:/Users/ramon/Desktop/Ramon/Faculdade/Api6semestre/API6Back/src/doc/affix_amil_header_bronze_csv.csv"
        data = pd.read_csv(url_csv, sep=';',
                           low_memory=False)
        return Csv_service.transform_fields(data)

    @staticmethod
    def transform_fields(data):
        columns = pd.DataFrame(data)
        transform_columns = []
        for col in columns:
            if 'dt_' in col:
                transform_columns.append(col)
        for item in transform_columns:
            data[item] = pd.to_datetime(data[item], format='%Y/%m/%d')
        return data
