# # import streamlit as st
# # import pandas as pd
# # import requests

# # st.title('Ponderada - upload de CSV para MinIO e Clickhouse')

# # uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

# # if uploaded_file is not None:
# #     try:
# #         df = pd.read_csv(uploaded_file, delimiter=';')
# #         st.write("Pré-visualização do CSV")
# #         st.dataframe(df)

# #         if st.button('Enviar para processamento'):
# #             with st.spinner('Enviando dados...'):
# #                 #ler o conteúdo do arquivo
# #                 file_content = uploaded_file.getvalue()
                
# #                 #enviar o arquivo para o backend Flask
# #                 response = requests.post(
# #                     "http://localhost:5000/upload-csv",  #URL da API Flask
# #                     files={'file': (uploaded_file.name, file_content, 'text/csv')}
# #                 )
                
# #                 if response.status_code == 200:
# #                     st.success("Dados enviados e inseridos com sucesso no ClickHouse! Você já pode visualizá-los no DBeaver!")
# #                 else:
# #                     st.error(f"Erro: {response.json().get('error', 'Desconhecido')}")

# #     except pd.errors.ParserError as e:
# #         st.error(f"Erro ao processar o CSV: {e}")
# #     except Exception as e:
# #         st.error(f"Erro inesperado: {e}")


# import streamlit as st
# import pandas as pd
# import json
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
#                         st.write("Gerando gráfico para SKU Dataset...")
#                         # Assuming the dataset has columns for 'marca', 'conteudo_valor', etc.
#                         df['conteudo_valor'] = df['conteudo_valor'].astype(float)
#                         marca_volume = df.groupby('marca')['conteudo_valor'].sum().reset_index()
                        
#                         # Generate a bar chart using matplotlib
#                         plt.figure(figsize=(10, 6))
#                         plt.bar(marca_volume['marca'], marca_volume['conteudo_valor'], color='skyblue')
#                         plt.xlabel('Marca')
#                         plt.ylabel('Volume Total')
#                         plt.title('Total Volume por Marca')
#                         plt.xticks(rotation=45, ha='right')
#                         st.pyplot(plt)

#                     elif dataset_name == "sku_price":
#                         st.write("Gerando gráfico para SKU Price...")
#                         # Assuming the dataset has columns for 'cod_prod', 'data_inicio', 'preco', etc.
#                         df['data_inicio'] = pd.to_datetime(df['data_inicio'])
#                         df['preco'] = df['preco'].astype(float)

#                         # Generate a line chart for price changes over time
#                         plt.figure(figsize=(10, 6))
#                         for cod_prod in df['cod_prod'].unique():
#                             product_data = df[df['cod_prod'] == cod_prod]
#                             plt.plot(product_data['data_inicio'], product_data['preco'], marker='o', label=f'Produto {cod_prod}')
                        
#                         plt.xlabel('Data de Início')
#                         plt.ylabel('Preço')
#                         plt.title('Variação de Preço ao longo do Tempo')
#                         plt.legend()
#                         st.pyplot(plt)

#                 else:
#                     st.error(f"Erro: {response.json().get('error', 'Desconhecido')}")

#     except pd.errors.ParserError as e:
#         st.error(f"Erro ao processar o CSV: {e}")
#     except Exception as e:
#         st.error(f"Erro inesperado: {e}")



import streamlit as st
import pandas as pd
import requests 
import matplotlib.pyplot as plt

st.title('Ponderada - upload de CSV para MinIO e Clickhouse')

uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

if uploaded_file is not None:
    try:
        # Attempt to detect delimiter by looking for ';' in the file
        uploaded_file.seek(0)
        content = uploaded_file.read(1024).decode('utf-8')
        uploaded_file.seek(0)
        delimiter = ';' if ';' in content else ','

        df = pd.read_csv(uploaded_file, delimiter=delimiter)
        st.write("Pré-visualização do CSV")
        st.dataframe(df)

        if st.button('Enviar para processamento'):
            with st.spinner('Enviando dados...'):
                # Ler o conteúdo do arquivo
                file_content = uploaded_file.getvalue()
                
                # Enviar o arquivo para o backend Flask
                response = requests.post(
                    "http://localhost:5000/upload-csv",  # URL da API Flask
                    files={'file': (uploaded_file.name, file_content, 'text/csv')}
                )
                
                if response.status_code == 200:
                    st.success("Dados enviados e inseridos com sucesso no ClickHouse! Você já pode visualizá-los no DBeaver!")

                    # Determine the dataset type
                    dataset_name = uploaded_file.name.split('.')[0]

                    if dataset_name == "sku_dataset":
                        st.write("Gerando gráficos para SKU Dataset...")

                        # Total de produtos por categoria
                        categoria_count = df['categoria'].value_counts().reset_index()
                        categoria_count.columns = ['categoria', 'total_produtos']
                        
                        # Generate a bar chart for total products by category
                        plt.figure(figsize=(10, 6))
                        plt.bar(categoria_count['categoria'], categoria_count['total_produtos'], color='skyblue')
                        plt.xlabel('Categoria')
                        plt.ylabel('Total de produtos')
                        plt.title('Total de produtos por categoria')
                        plt.xticks(rotation=45, ha='right')
                        st.pyplot(plt)

                        # Total de produtos por subcategoria
                        subcategoria_count = df['sub_categoria'].value_counts().reset_index()
                        subcategoria_count.columns = ['sub_categoria', 'total_produtos']
                        
                        # Generate a bar chart for total products by subcategory
                        plt.figure(figsize=(10, 6))
                        plt.bar(subcategoria_count['sub_categoria'], subcategoria_count['total_produtos'], color='lightgreen')
                        plt.xlabel('Subcategoria')
                        plt.ylabel('Total de Produtos')
                        plt.title('Total de produtos por subcategoria')
                        plt.xticks(rotation=45, ha='right')
                        st.pyplot(plt)

                    elif dataset_name == "sku_price":
                        st.write("Gerando gráfico para SKU Price...")
                        # Convert 'data_inicio' to datetime
                        df['data_inicio'] = pd.to_datetime(df['data_inicio'])
                        df['preco'] = df['preco'].astype(float)

                        # Aggregate by date to reduce clutter
                        df_aggregated = df.groupby('data_inicio')['preco'].mean().reset_index()

                        # Generate a line chart for the aggregated price changes over time
                        plt.figure(figsize=(10, 6))
                        plt.plot(df_aggregated['data_inicio'], df_aggregated['preco'], marker='o', color='b')
                        
                        plt.xlabel('Data de início')
                        plt.ylabel('Preço médio')
                        plt.title('Variação média de preço ao longo do tempo')
                        st.pyplot(plt)

                else:
                    st.error(f"Erro: {response.json().get('error', 'Dataset desconhecido')}")

    except pd.errors.ParserError as e:
        st.error(f"Erro ao processar o CSV: {e}")
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
