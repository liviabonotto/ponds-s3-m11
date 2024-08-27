import unittest
from unittest.mock import patch, MagicMock
from minio import Minio
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
import tempfile
import os

class TestMinioClient(unittest.TestCase):

    @patch('data_pipeline.minio_client.minio_client')
    def test_create_bucket_if_not_exists(self, mock_minio_client):
        #configura o mock para retornar False, simulando que o bucket não existe
        mock_minio_client.bucket_exists.return_value = False

        #chama a função que deve criar o bucket
        create_bucket_if_not_exists('test-bucket')

        #verifica se as funções foram chamadas conforme esperado
        mock_minio_client.bucket_exists.assert_called_with('test-bucket')
        mock_minio_client.make_bucket.assert_called_with('test-bucket')

    @patch('data_pipeline.minio_client.minio_client')
    def test_upload_file(self, mock_minio_client):
        #cria um arquivo temporário para o teste
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write("Conteúdo de teste".encode('utf-8'))
            tmp_file_name = tmp_file.name

        #chama a função que deve fazer o upload do arquivo
        upload_file('test-bucket', tmp_file_name)

        #verifica se a função fput_object foi chamada corretamente
        mock_minio_client.fput_object.assert_called_with('test-bucket', os.path.basename(tmp_file_name), tmp_file_name)

        #remove o arquivo temporário
        os.remove(tmp_file_name)

    @patch('data_pipeline.minio_client.minio_client')
    def test_download_file(self, mock_minio_client):
        #simula o comportamento de fget_object
        mock_minio_client.fget_object.return_value = None

        #chama a função que deve fazer o download do arquivo
        download_file('test-bucket', 'test-file.txt', '/path/to/downloaded-test-file.txt')

        #verifica se a função fget_object foi chamada corretamente
        mock_minio_client.fget_object.assert_called_with('test-bucket', 'test-file.txt', '/path/to/downloaded-test-file.txt')


if __name__ == '__main__':
    unittest.main()
