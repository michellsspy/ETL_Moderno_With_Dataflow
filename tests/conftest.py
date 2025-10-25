# Testes do python
import pytest
from apache_beam.options.pipeline_options import PipelineOptions

@pytest.fixture(scope="module") # Melhor usar scope="module" para eficiência
def pipeline_options():
    """Fornece um objeto PipelineOptions configurado para testes com DirectRunner."""
    # Define os argumentos como uma lista de strings
    args = [
        '--runner=DirectRunner',
        '--project_id=test-project', # Usando project_id que funcionou no DirectRunner
        '--dataset_id_raw=test_raw',
        '--dataset_id_trusted=test_trusted',
        #'--dataset_id_refined=test_refined',
        '--bucket_name=test-bucket',
        '--pk_map_json={"raw_test": ["id"]}', # Schema simples para teste
        '--processing_date=2025-01-01',
        # Adicione quaisquer outras opções que seus testes possam precisar
        # '--temp_location=...' # Geralmente não necessário para DirectRunner
    ]
    # Cria e retorna o objeto PipelineOptions parseado a partir dos args
    return PipelineOptions(flags=args, save_main_session=False) # save_main_session=False é bom para testes