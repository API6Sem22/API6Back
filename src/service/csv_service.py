import pandas as pd

class Csv_service:

    @staticmethod
    def read_csv():
        # DTYPES = {'dt_inclusao':str,
        #           '_id':str,
        #           '_idFile': str,
        #           '_idheader_bronze':str}
        url_csv = "C:/Users/gabsm/OneDrive/Documentos/6° semestre/git/API6Back/src/doc/amil_mensalidade_bronze_csv.csv"
        read = pd.read_csv(url_csv, sep=',', low_memory=False).to_dict(orient="records")
        print(read)
        return read