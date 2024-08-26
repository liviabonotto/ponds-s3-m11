import os
import streamlit as st
import pandas as pd
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from io import StringIO
import tempfile

#streamlit UI
st.title('Upload de CSV para MinIO e Clickhouse')

uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

if uploaded_file is not None:
    # Process the uploaded CSV file
    df = pd.read_csv(uploaded_file, delimiter=';')

    # Ensure correct data types
    df['cod_prod'] = df['cod_prod'].astype(str)  # Convert product code to string
    df['conteudo_valor'] = pd.to_numeric(df['conteudo_valor'], errors='coerce')  # Ensure content value is numeric

    # Display the DataFrame
    st.write("Pré-visualização do dataframe")
    st.dataframe(df)

    # Save the file temporarily
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    # Upload to MinIO
    create_bucket_if_not_exists("raw-data")
    upload_file("raw-data", tmp_path)

    # Execute SQL script to create the table (if it doesn't exist)
    client = get_client()
    script_dir = os.path.dirname(__file__)
    sql_path = os.path.join(script_dir, 'data_pipeline', 'sql', 'create_table.sql')
    execute_sql_script('sql\create_table.sql')

    # Prepare and insert data into ClickHouse
    insert_dataframe(client, 'working_data', df)

    st.success("O arquivo CSV foi carregado e os dados inseridos no Clickhouse!")
