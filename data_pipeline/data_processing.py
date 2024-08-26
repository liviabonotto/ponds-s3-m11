import pandas as pd
from datetime import datetime

#função para processar o dataframe, se necessario implementamos algo adicional
def process_data(df):
    return df

#função para preparar o dataframe, transformando as colunas em uma única columa com json, e adicionamos colunas com a data da ingestão e tag
def prepare_dataframe_for_insert(df):
    df['data_ingestao'] = datetime.now()
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    df['tag'] = 'sku_dataset'
    return df[['data_ingestao', 'dado_linha', 'tag']]
