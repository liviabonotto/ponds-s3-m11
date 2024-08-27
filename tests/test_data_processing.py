import unittest
import pandas as pd
from datetime import datetime
from unittest.mock import patch
from io import StringIO 
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../data_pipeline')))
from data_processing import process_data, prepare_dataframe_for_insert

class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        #criando um datafrane de exemplo para os testes
        data = """sku,name,price
        1,Product A,10.0
        2,Product B,15.5
        3,Product C,7.99"""
        self.df = pd.read_csv(StringIO(data))

    def test_process_data(self):
        #teste simples para garantir que a função de processamento retorne o mesmo dataframe
        processed_df = process_data(self.df)
        pd.testing.assert_frame_equal(processed_df, self.df)

    def test_prepare_dataframe_for_insert(self):
        #testando a função prepare_dataframe_for_insert
        result_df = prepare_dataframe_for_insert(self.df)

        #verificando se as colunas esperadas estão presentes
        self.assertIn('data_ingestao', result_df.columns)
        self.assertIn('dado_linha', result_df.columns)
        self.assertIn('tag', result_df.columns)

        #verificando se a coluna 'tag' contém o valor correto
        self.assertTrue(all(result_df['tag'] == 'sku_dataset'))

        #verificando se a coluna 'dado_linha' contém JSON
        for json_data in result_df['dado_linha']:
            self.assertTrue('"sku":' in json_data)
            self.assertTrue('"name":' in json_data)
            self.assertTrue('"price":' in json_data)

        #verificando se 'data_ingestao' está na forma de datetime
        self.assertTrue(isinstance(result_df['data_ingestao'].iloc[0], datetime))

if __name__ == '__main__':
    unittest.main()