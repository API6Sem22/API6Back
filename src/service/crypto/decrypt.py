import pyodbc
import pandas as pd
from cryptography.fernet import Fernet

def result_df():
    server = ""
    user = ""
    pwd = ""
    db = "dw_dend"
    driver = "SQL Server"

    dados_conexao = "Driver={"+driver+"}; Server="+server+"; Database="+db+"; ENCRYPT=yes; UID="+user+"; PWD="+pwd+";"
    conexao = pyodbc.connect(dados_conexao)

    cursor = conexao.cursor()
    comando = f"""SELECT * FROM TESTE"""

    cursor.execute(comando)

    row = cursor.fetchall()
    ids = []
    nomes = []
    for r in row:
        ids.append(r[0])
        nomes.append(r[1])

    data = {"id": ids, "nome": nomes}
    df_data= pd.DataFrame(data)

    comando = f"""SELECT * FROM KEY_CRYPTO"""

    cursor.execute(comando)

    row = cursor.fetchall()

    key = {}
    for r in row:
        key[r[1]] = r[0]

    conexao.close()

    return df_data, key


def decrypt_data(df_data, key):
    for index, row in df_data.iterrows():
        cipher_suite = Fernet(key[str(row["id"])])
        decoded_text = cipher_suite.decrypt(row["nome"])
        df_data = df_data.replace(to_replace=row["nome"], value=decoded_text.decode("utf-8"))
    
    return df_data


if __name__ == '__main__':
    try:
        df_data, key = result_df()
        df = decrypt_data(df_data, key)
        print(df)
    except Exception as e:
        print('Error: ' + str(e))
