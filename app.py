from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import os
import logging
import csv

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
        dataset_name = os.path.splitext(file.filename)[0]
        app.logger.info(f"Dataset name extracted from filename: {dataset_name}")

  # Attempt to auto-detect the delimiter
        try:
            sample = file.read(1024).decode('utf-8')
            file.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            app.logger.info(f"Auto-detected delimiter: {delimiter}")
        except Exception as e:
            app.logger.error(f"Auto-detection of delimiter failed: {e}")
            # If auto-detection fails, fallback to manual
            if ';' in sample:
                delimiter = ';'
                app.logger.info("Fallback to semicolon delimiter")
            else:
                delimiter = ','
                app.logger.info("Fallback to comma delimiter")

        app.logger.info("Lendo o arquivo CSV...")
        df = pd.read_csv(file, delimiter=delimiter)
        
        app.logger.info(f"Shape of the DataFrame: {df.shape}")
        
        if df.empty:
            app.logger.error("Arquivo CSV está vazio ou não possui colunas válidas.")
            return jsonify({"error": "Arquivo CSV está vazio ou não possui colunas válidas."}), 400

        app.logger.info(f"Dataframe lido com sucesso, tamanho: {df.shape}")

        # Process DataFrame
        df_processed = process_data(df)
        app.logger.info("Dataframe processado com sucesso!")

        # Prepare DataFrame for insertion, passing the dataset_name argument
        df_prepared = prepare_dataframe_for_insert(df_processed, dataset_name)
        app.logger.info("Dataframe preparado para inserção no ClickHouse!")

        # Save temporarily for upload to MinIO
        temp_filename = f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(temp_filename, index=False)
        app.logger.info(f"Arquivo temporário salvo: {temp_filename}")

        # Upload to MinIO
        upload_file("raw-data", temp_filename)
        app.logger.info("Arquivo enviado para MinIO")

        # Ensure table exists
        client = get_client()
        ensure_table_exists(client, 'working_data', 'sql/create_table.sql')

        # Ensure view exists, handle missing SQL script
        view_script_path = f'sql/create_view_{dataset_name}.sql'
        if not os.path.exists(view_script_path):
            app.logger.error(f"O arquivo de script SQL para criar a view não foi encontrado: {view_script_path}")
            return jsonify({"error": f"O arquivo de script SQL para criar a view não foi encontrado: {view_script_path}"}), 500
        
        ensure_view_exists(client, f'{dataset_name}_view', view_script_path)

        # Insert into ClickHouse
        insert_dataframe(client, 'working_data', df_prepared)
        app.logger.info("Dados inseridos no ClickHouse")

        # Remove temporary file
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
