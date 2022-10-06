import pyodbc
import os
from dotenv import load_dotenv
import pymongo
from bson.objectid import ObjectId


def search_mongo():
    uri = os.environ['BANCO_CREDENTIALS']
    client = pymongo.MongoClient(uri)
    db_mongo = client.Medical
    repasse = db_mongo.medical_repasse_trat_unique
    cursor = repasse.find()

    return cursor


def is_nan(field):
    if str(field) == 'nan':
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

    cursor = conexao.cursor()
    comando = f"""SELECT cli_id_ori FROM Dim_Cliente"""

    cursor.execute(comando)

    row = cursor.fetchall()

    for item in c_mongo:
        if item["_id"] in row:
            pass
        else:
            if is_nan(item["nome_completo"]): item["nome_completo"] = "-"
            if is_nan(item["dt_nascimento"]): item["dt_nascimento"] = "-"
            if is_nan(item["dependente"]): item["dependente"] = None
            if is_nan(item["marca_otica"]): item["marca_otica"] = None
            if is_nan(item["marca_otica_odonto"]): item["marca_otica_odonto"] = None
            if is_nan(item["situacao"]): item["situacao"] = "-"
            if is_nan(item["dt_cancelamento"]): item["dt_cancelamento"] = "-"
            if is_nan(item["dt_situacao"]): item["dt_situacao"] = "-"
            if is_nan(item["cod_contrato"]): item["cod_contrato"] = None
            
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
            if is_nan(item["cod_convenio"]): item["cod_convenio"] = None
            if is_nan(item["convenio"]): item["convenio"] = "-"
            if is_nan(item["dt_suspencao"]): item["dt_suspencao"] = "-"

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
            if is_nan(item["cod_plano"]): item["cod_plano"] = "-"
            if is_nan(item["plano"]): item["plano"] = "-"
            if is_nan(item["operadora"]): item["operadora"] = "-"
            if is_nan(item["dt_inicio_vigencia"]): item["dt_inicio_vigencia"] = "-"

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
            if is_nan(item["saude_orig"]): item["saude_orig"] = None
            if is_nan(item["saude_net_orig"]): item["saude_net_orig"] = None
            if is_nan(item["dt_competencia"]): item["dt_competencia"] = "-"
            if is_nan(item["dt_geracao"]): item["dt_geracao"] = "-"

            comando = f"""INSERT INTO Dim_Repasse(rep_saude_orig,
                                                    rep_saude_net_orig,
                                                    rep_competencia,
                                                    rep_data_geracao)
            VALUES
                ({item["saude_orig"]}, {item["saude_net_orig"]}, '{item["dt_competencia"]}',
                '{item["dt_geracao"]}')"""

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

    conexao.close()


if __name__ == '__main__':
    try:
        load_dotenv()
        c_mongo = search_mongo()
        insert_dw(c_mongo)
    except Exception as e:
        print(e)
