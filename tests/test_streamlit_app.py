# tests/test_streamlit_app.py
import pytest
from data_pipeline.clickhouse_client import get_client, insert_dataframe
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
import pandas as pd
import tempfile

@pytest.fixture
def sample_data():
    data = {
        "cod_prod": ["603078352615", "131384024501"],
        "nome_abrev": ["Acne Solutions Cleansing Bar", "Pep Start Daily UV"],
        "nome_completo": ["Acne Solutions Cleansing Bar for Face & Body", "Pep Start Daily UV Protector Broad Spectrum SPF 50"],
        "descricao": ["Sabonete líquido Clinique Acne Solutions Cleansing Bar for Face & Body", "Protetor solar Clinique Pep Start Daily UV Protector Broad Spectrum SPF 50"],
        "categoria": ["Corpo", "Corpo"],
        "sub_categoria": ["Sabonete líquido", "Protetor solar"],
        "marca": ["Clinique", "Clinique"],
        "conteudo_valor": [156.0, 30.0],
        "conteudo_medida": ["ml", "ml"]
    }
    return pd.DataFrame(data)

def test_minio_upload_download(sample_data):
    # Save the sample data as CSV temporarily
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        sample_data.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    # Test MinIO upload and download
    create_bucket_if_not_exists("test-bucket")
    upload_file("test-bucket", tmp_path)
    
    # Download and compare
    download_file("test-bucket", tmp_path.split('/')[-1], f"downloaded_{tmp_path.split('/')[-1]}")
    df_downloaded = pd.read_csv(f"downloaded_{tmp_path.split('/')[-1]}")
    
    pd.testing.assert_frame_equal(sample_data, df_downloaded)

def test_clickhouse_insert(sample_data):
    client = get_client()

    # Insert data into ClickHouse
    insert_dataframe(client, 'working_data', sample_data)

    # Fetch the data back
    result = client.query('SELECT * FROM working_data')

    assert len(result) == len(sample_data)
