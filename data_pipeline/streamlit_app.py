# import streamlit as st
# import pandas as pd
# import requests 
# import matplotlib.pyplot as plt

# st.title('Ponderada - upload de CSV para MinIO e Clickhouse')

# uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

# if uploaded_file is not None:
#     try:
#         # Attempt to detect delimiter by looking for ';' in the file
#         uploaded_file.seek(0)
#         content = uploaded_file.read(1024).decode('utf-8')
#         uploaded_file.seek(0)
#         delimiter = ';' if ';' in content else ','

#         df = pd.read_csv(uploaded_file, delimiter=delimiter)
#         st.write("Pré-visualização do CSV")
#         st.dataframe(df)

#         if st.button('Enviar para processamento'):
#             with st.spinner('Enviando dados...'):
#                 # Ler o conteúdo do arquivo
#                 file_content = uploaded_file.getvalue()
                
#                 # Enviar o arquivo para o backend Flask
#                 response = requests.post(
#                     "http://localhost:5000/upload-csv",  # URL da API Flask
#                     files={'file': (uploaded_file.name, file_content, 'text/csv')}
#                 )
                
#                 if response.status_code == 200:
#                     st.success("Dados enviados e inseridos com sucesso no ClickHouse! Você já pode visualizá-los no DBeaver!")

#                     # Determine the dataset type
#                     dataset_name = uploaded_file.name.split('.')[0]

#                     if dataset_name == "sku_dataset":
#                         st.write("Gerando gráficos para SKU Dataset...")

#                         # Total de produtos por categoria
#                         categoria_count = df['categoria'].value_counts().reset_index()
#                         categoria_count.columns = ['categoria', 'total_produtos']
                        
#                         # Generate a bar chart for total products by category
#                         plt.figure(figsize=(10, 6))
#                         plt.bar(categoria_count['categoria'], categoria_count['total_produtos'], color='skyblue')
#                         plt.xlabel('Categoria')
#                         plt.ylabel('Total de produtos')
#                         plt.title('Total de produtos por categoria')
#                         plt.xticks(rotation=45, ha='right')
#                         st.pyplot(plt)

#                         # Total de produtos por subcategoria
#                         subcategoria_count = df['sub_categoria'].value_counts().reset_index()
#                         subcategoria_count.columns = ['sub_categoria', 'total_produtos']
                        
#                         # Generate a bar chart for total products by subcategory
#                         plt.figure(figsize=(10, 6))
#                         plt.bar(subcategoria_count['sub_categoria'], subcategoria_count['total_produtos'], color='lightgreen')
#                         plt.xlabel('Subcategoria')
#                         plt.ylabel('Total de Produtos')
#                         plt.title('Total de produtos por subcategoria')
#                         plt.xticks(rotation=45, ha='right')
#                         st.pyplot(plt)

#                     elif dataset_name == "sku_price":
#                         st.write("Gerando gráfico para SKU Price...")
#                         # Convert 'data_inicio' to datetime
#                         df['data_inicio'] = pd.to_datetime(df['data_inicio'])
#                         df['preco'] = df['preco'].astype(float)

#                         # Aggregate by date to reduce clutter
#                         df_aggregated = df.groupby('data_inicio')['preco'].mean().reset_index()

#                         # Generate a line chart for the aggregated price changes over time
#                         plt.figure(figsize=(10, 6))
#                         plt.plot(df_aggregated['data_inicio'], df_aggregated['preco'], marker='o', color='b')
                        
#                         plt.xlabel('Data de início')
#                         plt.ylabel('Preço médio')
#                         plt.title('Variação média de preço ao longo do tempo')
#                         st.pyplot(plt)

#                 else:
#                     st.error(f"Erro: {response.json().get('error', 'Dataset desconhecido')}")

#     except pd.errors.ParserError as e:
#         st.error(f"Erro ao processar o CSV: {e}")
#     except Exception as e:
#         st.error(f"Erro inesperado: {e}")



import streamlit as st
import pandas as pd
import requests 

st.title('Vizion Dashboard')
st.subheader("Olá, Maria! Comece subindo um novo arquivo.")


uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

if uploaded_file is not None:
    try:
        #identificando o delimitador
        uploaded_file.seek(0)
        content = uploaded_file.read(1024).decode('utf-8')
        uploaded_file.seek(0)
        delimiter = ';' if ';' in content else ','

        df = pd.read_csv(uploaded_file, delimiter=delimiter)
        st.write("Pré-visualização do CSV")
        st.dataframe(df)

        if st.button('Enviar para processamento'):
            with st.spinner('Enviando dados...'):
                #ler o conteúdo do arquivo
                file_content = uploaded_file.getvalue()
                
                #enviar o arquivo para o backend Flask
                response = requests.post(
                    "http://localhost:5000/upload-csv",
                    files={'file': (uploaded_file.name, file_content, 'text/csv')}
                )
                
                if response.status_code == 200:
                    st.success("Dados enviados e inseridos com sucesso no ClickHouse! Você já pode visualizá-los no DBeaver!")

                    #dataset type
                    dataset_name = uploaded_file.name.split('.')[0]

                    if dataset_name == "sku_dataset":
                        st.write("Gerando gráficos para SKU Dataset...")

                        #total de produtos por categoria
                        categoria_count = df['categoria'].value_counts().reset_index()
                        categoria_count.columns = ['categoria', 'total_produtos']
                        
                        st.subheader("Total de produtos por categoria")
                        st.bar_chart(categoria_count.set_index('categoria'))

                        #total de produtos por subcategoria
                        subcategoria_count = df['sub_categoria'].value_counts().reset_index()
                        subcategoria_count.columns = ['sub_categoria', 'total_produtos']
                        
                        st.subheader("Total de produtos por subcategoria")
                        st.bar_chart(subcategoria_count.set_index('sub_categoria'))

                    elif dataset_name == "sku_price":
                        st.write("Gerando gráfico para SKU Price...")
                        #convertendo para date time
                        df['data_inicio'] = pd.to_datetime(df['data_inicio'])
                        df['preco'] = df['preco'].astype(float)

                        #agregando
                        df_aggregated = df.groupby('data_inicio')['preco'].mean().reset_index()

                        #variação media de preço ao longo do tempo
                        st.subheader("Variação média de preço ao longo do tempo")
                        st.line_chart(df_aggregated.set_index('data_inicio')['preco'])

                else:
                    st.error(f"Erro: {response.json().get('error', 'Dataset desconhecido')}")

    except pd.errors.ParserError as e:
        st.error(f"Erro ao processar o CSV: {e}")
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
