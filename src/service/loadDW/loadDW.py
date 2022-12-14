import pyodbc
import os
from dotenv import load_dotenv
import pymongo
import logging
from datetime import datetime

def search_mongo():
    uri = os.environ['BANCO_CREDENTIALS']
    client = pymongo.MongoClient(uri)
    db_mongo = client.Medical
    repasse = db_mongo.medical_final_repasse
    cursor = repasse.find()

    return cursor


def is_nan(field):
    if str(field) == 'nan':
        return True
    else:
        return False

def key_exists(key, dict):
    if key not in dict:
        return True
    else:
        return False


def insert_dw(c_mongo):
    server = os.environ['DW_SERVER']
    user = os.environ['DW_USER']
    pwd = os.environ['DW_PASS']
    db = "dw_dend"
    driver = "SQL Server"

    dados_conexao = "Driver={"+driver+"}; Server="+server+"; Database="+db+"; ENCRYPT=yes; UID="+user+"; PWD="+pwd+";"
    conexao = pyodbc.connect(dados_conexao)
    logger.info('Create a connection')

    cursor = conexao.cursor()
    comando = f"""SELECT cli_id_ori FROM Dim_Cliente"""

    cursor.execute(comando)

    row = cursor.fetchall()
    key_values = ["nome_completo", "dt_nascimento", "dependente", "marca_otica", "marca_otica_odonto",
    "situacao", "dt_cancelamento", "dt_situacao", "cod_contrato", "cod_convenio", "convenio",
    "dt_suspencao", "cod_plano", "plano", "operadora", "dt_inicio_vigencia", "saude_orig",
    "saude_net_orig", "dt_competencia", "dt_geracao", "tp_beneficiario", "dif_rep_mens", "rubrica",
    "valor_repasse", "condicao"]

    for item in c_mongo:
        if any(item["_id"] in r for r in row):
            pass
        else:   
            for k in key_values:
                if key_exists(k, item): item[k] = "NULL"            
                if is_nan(item[k]): item[k] = "NULL"
            print(item)
            # INSERT DIM_CLIENTE
            comando = f"""INSERT INTO Dim_Cliente(cli_id_ori,
                                                    cli_nome,
                                                    cli_data_nascimento,
                                                    cli_qtd_dependente,
                                                    cli_marca_otica,
                                                    cli_marca_otica_odonto,
                                                    cli_situacao,
                                                    cli_data_cancelamento,
                                                    cli_data_situacao,
                                                    cli_cod_contrato)
            VALUES
                ('{item["_id"]}', '{item["nome_completo"]}', '{item["dt_nascimento"]}',
                {item["dependente"]}, {item["marca_otica"]}, {item["marca_otica_odonto"]},
                '{item["situacao"]}', '{item["dt_cancelamento"]}', '{item["dt_situacao"]}',
                {item["cod_contrato"]})"""

            cursor.execute(comando)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            id_cliente = cursor.fetchone()[0]
            cursor.commit()

            # INSERT DIM_CONVENIO
            comando = f"""INSERT INTO Dim_Convenio(con_codigo,
                                                    con_nome,
                                                    con_data_suspensao)
            VALUES
                ({item["cod_convenio"]}, '{item["convenio"]}', '{item["dt_suspencao"]}')"""

            cursor.execute(comando)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            id_convenio = cursor.fetchone()[0]
            cursor.commit()

            # INSERT DIM_PLANO
            comando = f"""INSERT INTO Dim_Plano(pln_codigo,
                                                pln_nome,
                                                pln_operadora,
                                                pln_ini_vigencia)
            VALUES
                ('{item["cod_plano"]}', '{item["plano"]}', '{item["operadora"]}',
                '{item["dt_inicio_vigencia"]}')"""

            cursor.execute(comando)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            id_plano = cursor.fetchone()[0]
            cursor.commit()

            # INSERT DIM_REPASSE
            comando = f"""INSERT INTO Dim_Repasse(rep_saude_orig,
                                                    rep_competencia,
                                                    rep_data_geracao,
                                                    rep_valor,
                                                    rep_tipo_beneficiario,
                                                    rep_rubrica,
                                                    rep_condicao,
                                                    rep_diferenca)
            VALUES
                ({item["saude_orig"]}, '{item["dt_competencia"]}', '{item["dt_geracao"]}',
                {item["valor_repasse"]}, '{item["tp_beneficiario"]}', '{item["rubrica"]}', 
                '{item["condicao"]}', {item["dif_rep_mens"]})"""

            cursor.execute(comando)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            id_repasse = cursor.fetchone()[0]
            cursor.commit()

            # INSERT FATO_MEDICAL
            comando = f"""INSERT INTO Fato_Medical(Dim_Cliente_cli_id,
                                                    Dim_Plano_pln_id,
                                                    Dim_Repasse_rep_id,
                                                    Dim_Convenio_con_id)
            VALUES
                ({id_cliente}, {id_plano}, {id_repasse}, {id_convenio})"""

            cursor.execute(comando)
            cursor.commit()
            logger.info('Get a file id: ' + str(item["_id"]))

    conexao.close()
    logger.info('Close connection')


if __name__ == '__main__':
    datetime_now = datetime.now()
    filename = datetime.now().strftime("%Y-%m-%d") + ' ' + datetime_now.strftime('%Hh%Mm%S') + '-DW.log'
    format_log = '[%(asctime)s] %(name)s %(levelname)s: %(message)s'
    logging.basicConfig(filename=filename,
                        filemode='a',
                        level=logging.DEBUG,
                        format=format_log,
                        datefmt='%Y-%m-%d %H:%M:%S')

    global logger
    logger = logging.getLogger()
    try:
        load_dotenv()
        c_mongo = search_mongo()
        insert_dw(c_mongo)
        logger.info('Data imported into SQL Server successfully')
    except Exception as e:
        logger.error('Error: ' + str(e))
