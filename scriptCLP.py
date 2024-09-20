from pylogix import PLC
import oracledb
import time
from cred import PSSWD, USR, DSN, ORACLEPATH
from validation import validate
from data import dict_ep, statement


class fonte_clps():
    
    def __init__(self, dict_ep: dict, statement:str):
        self.dict_ep = dict_ep
        self.statement = statement

    def _inserir_dados(self):
        
        print("Iniciando conexão ao banco...")
        oracledb.init_oracle_client(config_dir=ORACLEPATH)
        try:
            conn = oracledb.connect(user=USR, password=PSSWD, dsn=DSN)
            cursor = conn.cursor()
            print("Conectado.")
        except:
            raise Exception("Falha ao conectar ao banco.")
        
        try:
            while True:
                for endpoint, tags in self.dict_ep.items():
                    print(f"Conectando ao endpoint {endpoint}")
                    plc = PLC(endpoint)
                    print("Conectado.")
                    updates = {}
                    
                    print("Realizando leitura de tags...")
                    for tag in tags:
                        leitura = plc.Read(tag)
                        
                        if leitura.Status != "Success":
                            print(f"AVISO: A tag {tag} não pôde ser obtida.")
                            print(f"STATUS: {leitura.Status}")
                            continue
                        
                        val = validate(leitura.Value)    
                        value = str(val).casefold()
                        updates[tag] = f"'{value}'" if value in ["false", "true"] else value
            
                    if updates:
                        print("Enviando ao banco de dados...")
                        set_clause = ", ".join([f"{tag} = {value}" for tag, value in updates.items()])
                        query = f"{self.statement} {set_clause}"
                        cursor.execute(query)
                        conn.commit()
                        print("Envio realizado com sucesso.")
                               
                time.sleep(60)
        except Exception as e:
            print(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute(self):
        self._inserir_dados()


prog = fonte_clps(dict_ep, statement)

prog.execute()
