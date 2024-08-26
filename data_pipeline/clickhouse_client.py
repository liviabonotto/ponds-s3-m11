import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

#configuração do cliente ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost') 
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '8123')       

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

def execute_sql_script(script_path):
    try:
        client = get_client()
        with open(script_path, 'r') as file:
            sql_script = file.read()
        client.command(sql_script)
    except Exception as e:
        print(f"Erro ao executar o script SQL {script_path}: {e}")
        raise

def insert_dataframe(client, table_name, df):
    try:
        client.insert_df(table_name, df)
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")
        raise
