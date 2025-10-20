import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from hotelaria_pipeline.refined.transforms import ExtractDataParaAgregacao, AggregateAndFormat
from datetime import date

# Dados de entrada (mock da tabela trusted)
MOCK_TRUSTED_DATA = [
    {
        'id_hotel': 'H1', 'data_checkin': date(2025, 1, 1),
        'valor_total_reserva': 100.0, 'nome_hotel': 'Hotel A', 'cidade_hotel': 'SP'
    },
    {
        'id_hotel': 'H1', 'data_checkin': date(2025, 1, 1),
        'valor_total_reserva': 150.0, 'nome_hotel': 'Hotel A', 'cidade_hotel': 'SP'
    },
    {
        'id_hotel': 'H2', 'data_checkin': date(2025, 1, 1),
        'valor_total_reserva': 300.0, 'nome_hotel': 'Hotel B', 'cidade_hotel': 'RJ'
    }
]

# Saída final esperada
EXPECTED_REFINED_OUTPUT = [
    {
        'id_hotel': 'H1', 'nome_hotel': 'Hotel A', 'cidade_hotel': 'SP',
        'data_referencia': '2025-01-01',
        'receita_total_dia': 250.0,
        'reservas_confirmadas_dia': 2,
        'taxa_ocupacao_dia': 0.85, # Valor fixo do placeholder
        'dt_processamento': '2025-01-10'
    },
    {
        'id_hotel': 'H2', 'nome_hotel': 'Hotel B', 'cidade_hotel': 'RJ',
        'data_referencia': '2025-01-01',
        'receita_total_dia': 300.0,
        'reservas_confirmadas_dia': 1,
        'taxa_ocupacao_dia': 0.85, # Valor fixo do placeholder
        'dt_processamento': '2025-01-10'
    }
]


def test_refined_aggregation(pipeline_options):
    
    processing_date = '2025-01-10'

    with TestPipeline(options=pipeline_options) as p:
        
        input_data = p | "Create Trusted" >> beam.Create(MOCK_TRUSTED_DATA)
        
        # Pipeline completo de refined
        output = (
            input_data
            | "Extract KV" >> beam.ParDo(ExtractDataParaAgregacao(), processing_date)
            | "Aggregate and Format" >> AggregateAndFormat(processing_date)
        )
        
        # Assert: Verificar se a agregação está correta
        assert_that(
            output,
            equal_to(EXPECTED_REFINED_OUTPUT),
            label="Check Aggregated Output"
        )