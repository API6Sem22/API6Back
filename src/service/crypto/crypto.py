from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os
import pyodbc

class crypto:

    @staticmethod
    def encrypt(marca_otica, cli_nome, cod_num):
        load_dotenv()
        server = os.environ['DW_SERVER']
        user = os.environ['DW_USER']
        pwd = os.environ['DW_PASS']
        db = "denddev"
        driver = "SQL Server"

        dados_conexao = "Driver={"+driver+"}; Server="+server+"; Database="+db+"; ENCRYPT=yes; UID="+user+"; PWD="+pwd+";"
        conexao = pyodbc.connect(dados_conexao)

        cursor = conexao.cursor()

        comando = f"""SELECT kcr_key FROM KEY_CRYPTO WHERE kcr_id = {marca_otica}"""

        cursor.execute(comando)

        row = cursor.fetchall()

        enc_nome     =  ""
        enc_contrato = ""
        for r in row:
            key = Fernet.generate_key()
            cipher_suite = Fernet(r[0])

            enc_nome     =  cipher_suite.encrypt(bytes(cli_nome, 'utf-8'))
            enc_contrato = cipher_suite.encrypt(bytes(str(cod_num), 'utf-8'))
            
        if enc_nome == "":
            key = Fernet.generate_key()
            cipher_suite = Fernet(key)

            enc_nome     =  cipher_suite.encrypt(bytes(cli_nome, 'utf-8'))
            enc_contrato = cipher_suite.encrypt(bytes(str(cod_num), 'utf-8'))

            comando = f"""INSERT INTO KEY_CRYPTO(kcr_key, kcr_id)
                            VALUES ('{key.decode('utf-8')}', '{marca_otica}')"""

            cursor.execute(comando)
            cursor.commit()

        conexao.close()

        return enc_nome, enc_contrato