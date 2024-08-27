import unittest
from unittest.mock import patch, mock_open, MagicMock
from data_pipeline.clickhouse_client import get_client, execute_sql_script, insert_dataframe

class TestClickhouseClient(unittest.TestCase):

    #teste para verificar o comportamento do método execute_sql_script em caso de exceção
    @patch('builtins.open', new_callable=mock_open, read_data='SELECT 1')  
    #mocka a função open para simular que o arquivo 'fake_path.sql' existe e contém o script SQL 'SELECT 1'
    @patch('data_pipeline.clickhouse_client.get_client')
    #mocka a função get_client para simular a criação de um cliente do ClickHouse
    def test_execute_sql_script_exception(self, mock_get_client, mock_open_file):
        #cria um mock para o cliente do ClickHouse
        mock_client = MagicMock()
        #define que o mock de get_client retorna o mock do cliente criado
        mock_get_client.return_value = mock_client
        #configura o mock do cliente para levantar uma exceção ao executar o comando SQL
        mock_client.command.side_effect = Exception('Error')

        #testa se a função execute_sql_script levanta uma exceção corretamente
        with self.assertRaises(Exception) as context:
            execute_sql_script('fake_path.sql')
        
        #verifica se a mensagem de erro 'Error' está presente na exceção levantada
        self.assertIn('Error', str(context.exception))

if __name__ == '__main__':
    unittest.main()
