from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import os
import logging

#configuração básica de log
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)

#criar bucket se não existir
create_bucket_if_not_exists("raw-data")

def ensure_table_exists(client, table_name, create_script_path):
    try:
        result = client.command(f"EXISTS TABLE {table_name}")
        if result == 0:
            app.logger.info(f"Tabela {table_name} não existe. Criando a tabela...")
            execute_sql_script(create_script_path)
            app.logger.info(f"Tabela {table_name} criada com sucesso.")
        else:
            app.logger.info(f"Tabela {table_name} já existe.")
    except Exception as e:
        app.logger.error(f"Erro ao verificar ou criar a tabela {table_name}: {str(e)}")
        raise

def ensure_view_exists(client, view_name, create_script_path):
    try:
        result = client.command(f"EXISTS VIEW {view_name}")
        if result == 0:
            app.logger.info(f"View {view_name} não existe. Criando a view...")
            execute_sql_script(create_script_path)
            app.logger.info(f"View {view_name} criada com sucesso.")
        else:
            app.logger.info(f"View {view_name} já existe.")
    except Exception as e:
        app.logger.error(f"Erro ao verificar ou criar a view {view_name}: {str(e)}")
        raise

@app.route('/')
def home():
    return "<h1>O servidor Flask está funcionando!</h1><p>Use a rota /upload-csv para fazer o upload.</p>"

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        app.logger.error("Nenhum arquivo foi enviado")
        return jsonify({"error": "Nenhum arquivo foi enviado"}), 400

    file = request.files['file']
    if file.filename == '':
        app.logger.error("Nome de arquivo inválido")
        return jsonify({"error": "Nome de arquivo inválido"}), 400

    try:
        app.logger.info("Lendo o arquivo CSV...")
        df = pd.read_csv(file, delimiter=';')
        if df.empty:
            app.logger.error("Arquivo CSV está vazio ou não possui colunas válidas.")
            return jsonify({"error": "Arquivo CSV está vazio ou não possui colunas válidas."}), 400

        app.logger.info(f"Dataframe lido com sucesso, tamanho: {df.shape}")

        df_processed = process_data(df)
        app.logger.info("Dataframe processado com sucesso!")

        df_prepared = prepare_dataframe_for_insert(df_processed)
        app.logger.info("Dataframe preparado para inserção no ClickHouse!")

        #salvar temporariamente para upload no MinIO
        temp_filename = f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(temp_filename, index=False)
        app.logger.info(f"Arquivo temporário salvo: {temp_filename}")

        #upload no MinIO
        upload_file("raw-data", temp_filename)
        app.logger.info("Arquivo enviado para MinIO")

        #verifica e garante que a tabela existe antes de inserir os dados
        client = get_client()
        ensure_table_exists(client, 'working_data', 'sql/create_table.sql')

        #verifica e garante que a view existe
        ensure_view_exists(client, 'working_view', 'sql/create_view.sql')

        #inserir no ClickHouse
        insert_dataframe(client, 'working_data', df_prepared)
        app.logger.info("Dados inseridos no ClickHouse")

        #remover o arquivo temporário
        os.remove(temp_filename)
        app.logger.info(f"Arquivo temporário removido: {temp_filename}")

        return jsonify({"message": "Arquivo processado e dados inseridos com sucesso!"}), 200

    except pd.errors.EmptyDataError:
        app.logger.error("Arquivo CSV está vazio.")
        return jsonify({"error": "Arquivo CSV está vazio."}), 400
    except pd.errors.ParserError as e:
        app.logger.error(f"Erro ao analisar o CSV: {str(e)}")
        return jsonify({"error": f"Erro ao analisar o CSV: {str(e)}"}), 400
    except Exception as e:
        app.logger.error(f"Erro ao processar o arquivo: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    #criar a tabela e a view na inicialização do servidor
    client = get_client()
    ensure_table_exists(client, 'working_data', 'sql/create_table.sql')
    ensure_view_exists(client, 'working_view', 'sql/create_view.sql')
    app.run(host='0.0.0.0', port=5000, debug=True)
