import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from hotelaria_pipeline.pipeline_options import RefinedPipelineOptions
from hotelaria_pipeline.schemas import SCHEMA_REFINED_OCUPACAO_DIARIA
from hotelaria_pipeline.refined.transforms import ExtractDataParaAgregacao, AggregateAndFormat
from hotelaria_pipeline.utils import setup_logging
import logging

def run():
    """Ponto de entrada principal para o pipeline da camada REFINED."""
    
    # 1. Configuração e Parse de Opções
    options = PipelineOptions(flags=None)
    refined_options = options.view_as(RefinedPipelineOptions)
    
    logger = setup_logging()
    logger.info("Iniciando pipeline REFINED (BQ Trusted -> BQ Refined)")
    
    processing_date = refined_options.processing_date

    # 2. Definição do Pipeline
    with beam.Pipeline(options=options) as p:
        
        # --- Fonte de Leitura (Query) ---
        
        # Lê a tabela trusted enriquecida
        query_trusted = f"""
            SELECT * FROM `{refined_options.project_id}.{refined_options.dataset_id_trusted}.trusted_reservas_enriquecidas`
            WHERE dt_processamento = '{processing_date}'
        """
        
        pcoll_trusted = (
            p 
            | "Read Trusted Data" >> beam.io.ReadFromBigQuery(query=query_trusted, use_standard_sql=True)
        )
        
        # --- Transformação (Agregação) ---
        
        pcoll_agregado = (
            pcoll_trusted
            | "Extract KV" >> beam.ParDo(ExtractDataParaAgregacao(), processing_date)
            | "Aggregate and Format" >> AggregateAndFormat(processing_date)
        )
        
        # --- Escrita (Destino) ---
        
        (
            pcoll_agregado
            | "Write Refined Table" >> beam.io.WriteToBigQuery(
                table=f"{refined_options.project_id}.{refined_options.dataset_id_refined}.refined_ocupacao_diaria",
                schema=SCHEMA_REFINED_OCUPACAO_DIARIA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Ou WRITE_TRUNCATE
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()