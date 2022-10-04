import pyodbc
import os
from dotenv import load_dotenv
import pymongo

def search_mongo():
    uri = os.environ['BANCO_CREDENTIALS']
    client = pymongo.MongoClient(uri)
    db_mongo = client.Medical
    repasse = db_mongo.medical_repasse_trat_unique
    cursor = repasse.find()

    return cursor

def insert_dw():
    server = os.environ['DW_SERVER']
    user = os.environ['DW_USER']
    pwd = os.environ['DW_PASS']
    db = "datawarriors-ho"
    driver = "SQL Server"

    dados_conexao = "Driver={"+driver+"}; Server="+server+"; Database="+db+"; ENCRYPT=yes; UID="+user+"; PWD="+pwd+";"
    conexao = pyodbc.connect(dados_conexao)

    cursor = conexao.cursor()
    comando = f"""SELECT cli_id_oli FROM Dim_Cliente"""

    cursor.execute(comando)

    row = cursor.fetchall()

    for item in cursor:
        if item["_id"] in row:
            pass
        else:
            id = 3
            cliente = "Lira Python"
            produto = "Carro"
            data = "25/08/2021"
            preco = 5000
            quantidade = 1

            comando = f"""INSERT INTO Vendas(id_venda, cliente, produto, data_venda, preco, quantidade)
            VALUES
                ({id}, '{cliente}', '{produto}', '{data}', {preco}, {quantidade})"""

            cursor.execute(comando)
            cursor.commit()


if __name__ == '__main__':
    try:
        load_dotenv()
        search_mongo()
    except Exception as e:
        print(e)
