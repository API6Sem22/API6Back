import pandas as pd


class Csv_service:

    @staticmethod
    def read_csv():
        parse_dates = ['competencia', 'dt_geracao', 'dt_nascimento', 'dt_situacao', 'inicio_vigencia']
        url_csv = "src/doc/medical_repasse_unique_csv.csv"

        data = pd.read_csv(url_csv, sep=';',
                           low_memory=False, parse_dates=parse_dates).to_dict(orient="records")
        return data
