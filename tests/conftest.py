import pytest

@pytest.fixture
def pipeline_options():
    """Fornece opções básicas para rodar testes com o DirectRunner."""
    options = [
        '--runner=DirectRunner',
        '--project_id=test-project',
        '--dataset_id_raw=test_raw',
        '--dataset_id_trusted=test_trusted',
        '--dataset_id_refined=test_refined',
        '--bucket_name=test-bucket',
        '--pk_map_json={"raw_test": ["id"]}',
        '--processing_date=2025-01-01'
    ]
    return options