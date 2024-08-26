import streamlit as st
import pandas as pd
import requests

st.title('Ponderada - upload de CSV para MinIO e Clickhouse')

uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, delimiter=';')
        st.write("Pré-visualização do CSV")
        st.dataframe(df)

        if st.button('Enviar para processamento'):
            with st.spinner('Enviando dados...'):
                #ler o conteúdo do arquivo
                file_content = uploaded_file.getvalue()
                
                #enviar o arquivo para o backend Flask
                response = requests.post(
                    "http://localhost:5000/upload-csv",  #URL da API Flask
                    files={'file': (uploaded_file.name, file_content, 'text/csv')}
                )
                
                if response.status_code == 200:
                    st.success("Dados enviados e inseridos com sucesso no ClickHouse! Você já pode visualizá-los no DBeaver!")
                else:
                    st.error(f"Erro: {response.json().get('error', 'Desconhecido')}")

    except pd.errors.ParserError as e:
        st.error(f"Erro ao processar o CSV: {e}")
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
