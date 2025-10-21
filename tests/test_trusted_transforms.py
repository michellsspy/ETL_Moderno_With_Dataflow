import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from hotelaria_pipeline.trusted.transforms import StandardizeAndValidate
from datetime import date

# Dados de entrada mockados (PCollection de entrada)
MOCK_INPUT_DATA = [
    {
        'id_reserva': 'R100', 'id_hospede': 'H200',
        'status_reserva': ' confirmed ', 'email_hospede': ' TEST@Email.com ',
        'valor_total_reserva': '150.75'
    },
    {
        'id_reserva': None, 'id_hospede': 'H201', # Deve falhar
        'status_reserva': 'pending', 'email_hospede': 'outro@email.com',
        'valor_total_reserva': '200.00'
    }
]

# Dados de saída esperados (PCollection 'success')
EXPECTED_VALID_OUTPUT = [
    {
        'id_reserva': 'R100', 'id_hospede': 'H200',
        'status_reserva': 'CONFIRMED', 'email_hospede': 'test@email.com',
        'valor_total_reserva': 150.75,
        'dt_processamento': '2025-01-01'
    }
]

def _strip_timestamp(element):
    """Remove o dt_insert para facilitar a comparação no teste."""
    if 'dt_insert' in element:
        del element['dt_insert']
    return element

def test_standardize_and_validate(pipeline_options):
    
    processing_date = '2025-01-01'

    with TestPipeline(options=pipeline_options) as p:
        
        # PCollection de entrada
        input_data = p | "Create Input" >> beam.Create(MOCK_INPUT_DATA)

        # Aplicar a transformação
        results = (
            input_data
            | "Test Validate" >> beam.ParDo(
                StandardizeAndValidate(),
                processing_date=processing_date
            ).with_outputs(StandardizeAndValidate.TAG_FAILED, main='success')
        )
        
        pcoll_validos = results['success']
        pcoll_invalidos = results[StandardizeAndValidate.TAG_FAILED]

        # Assert 1: Verificar se os dados válidos estão corretos
        assert_that(
            pcoll_validos | "Strip TS" >> beam.Map(_strip_timestamp),
            equal_to(EXPECTED_VALID_OUTPUT),
            label="Check Valid Output"
        )
        
        # Assert 2: Verificar se 1 registro foi para a 'failed' tag
        assert_that(
            pcoll_invalidos | "Count Failed" >> beam.combiners.Count.Globally(),
            equal_to([1]),
            label="Check Failed Count"
        )