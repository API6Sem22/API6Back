import pyodbc
import pandas as pd
from cryptography.fernet import Fernet

def result_df():
    server = ""
    user = ""
    pwd = ""
    db = "denddev"
    driver = "SQL Server"

    dados_conexao = "Driver={"+driver+"}; Server="+server+"; Database="+db+"; ENCRYPT=yes; UID="+user+"; PWD="+pwd+";"
    conexao = pyodbc.connect(dados_conexao)

    cursor = conexao.cursor()
    comando = f"""SELECT * FROM Dim_Cliente"""

    cursor.execute(comando)

    row = cursor.fetchall()

    ids = []
    id_orig = []
    nomes = []
    data_nascimento = []
    qtd_dependente = []
    marca_otica = []
    otica_odonto = []
    situacao = []
    cancelamento = []
    data_situacao = []
    cod_contrato = []
    for r in row:
        ids.append(r[0])
        id_orig.append(r[1])
        nomes.append(r[2])
        data_nascimento.append(r[3])
        qtd_dependente.append(r[4])
        marca_otica.append(r[5])
        otica_odonto.append(r[6])
        situacao.append(r[7])
        cancelamento.append(r[8])
        data_situacao.append(r[9])
        cod_contrato.append(r[10])  

    data = {"cli_id": ids, "cli_id_ori": id_orig, "cli_nome": nomes, "cli_data_nascimento": data_nascimento, 
    "cli_qtd_dependente": qtd_dependente, "cli_marca_otica": marca_otica, 
    "cli_marca_otica_odonto": otica_odonto, "cli_situacao": situacao, 
    "cli_data_cancelamento": cancelamento, "cli_data_situacao": data_situacao, 
    "cli_cod_contrato": cod_contrato}
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
        cipher_suite = Fernet(key[str(row["cli_marca_otica"])])
        decoded_nome = cipher_suite.decrypt(row["cli_nome"])
        decoded_contrato = cipher_suite.decrypt(str(row["cli_cod_contrato"]))
        df_data.loc[df_data["cli_nome"] == row["cli_nome"], "cli_nome"] = decoded_nome.decode('utf-8')
        df_data.loc[df_data["cli_cod_contrato"] == row["cli_cod_contrato"], "cli_cod_contrato"] = decoded_contrato.decode('utf-8')
    
    return df_data


if __name__ == '__main__':
    try:
        df_data, key = result_df()
        df = decrypt_data(df_data, key)
        print(df)
    except Exception as e:
        print('Error: ' + str(e))
