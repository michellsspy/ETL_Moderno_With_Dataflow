import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from hotelaria_pipeline.schemas import SCHEMA_TRUSTED_RESERVAS
from hotelaria_pipeline.trusted.transforms import StandardizeAndValidate, LeftJoin
from hotelaria_pipeline.utils import setup_logging
import logging

def run():
    """Ponto de entrada principal para o pipeline da camada TRUSTED."""
    
    # 1. Configuração e Parse de Opções
    options = PipelineOptions(flags=None)
    trusted_options = options
    
    logger = setup_logging()
    logger.info("Iniciando pipeline TRUSTED (BQ Raw -> BQ Trusted)")

    # 2. Definição do Pipeline
    with beam.Pipeline(options=options) as p:
        
        # --- Fontes de Leitura (Queries) ---
        
        # Tabela FATO principal
        query_reservas = f"""
            SELECT * FROM `{trusted_options.project_id}.{trusted_options.dataset_id_raw}.raw_hotel_reservas`
        """
        
        # Tabelas de DIMENSÃO
        query_hospedes = f"""
            SELECT id_hospede, nome AS nome_hospede, email AS email_hospede
            FROM `{trusted_options.project_id}.{trusted_options.dataset_id_raw}.raw_hotel_hospedes`
        """
        
        query_hoteis = f"""
            SELECT id_hotel, nome AS nome_hotel, cidade AS cidade_hotel
            FROM `{trusted_options.project_id}.{trusted_options.dataset_id_raw}.raw_hotel_hoteis`
        """

        # --- Leitura das PCollections ---
        
        pcoll_reservas = (
            p 
            | "Read Raw Reservas" >> beam.io.ReadFromBigQuery(query=query_reservas, use_standard_sql=True)
        )
        
        pcoll_hospedes = (
            p 
            | "Read Raw Hospedes" >> beam.io.ReadFromBigQuery(query=query_hospedes, use_standard_sql=True)
        )
        
        pcoll_hoteis = (
            p 
            | "Read Raw Hoteis" >> beam.io.ReadFromBigQuery(query=query_hoteis, use_standard_sql=True)
        )

        # --- Transformação (Enriquecimento/Join) ---
        
        # Exemplo: Left Join (Reservas + Hóspedes)
        reservas_com_hospedes = LeftJoin(
            pcoll_left=pcoll_reservas,
            pcoll_right=pcoll_hospedes,
            on_key_left='id_hospede',
            on_key_right='id_hospede'
        )

        # Exemplo: Left Join (Resultado anterior + Hotéis)
        reservas_enriquecidas = LeftJoin(
            pcoll_left=reservas_com_hospedes,
            pcoll_right=pcoll_hoteis,
            on_key_left='id_hotel',
            on_key_right='id_hotel'
        )

        # --- Validação e Padronização ---
        
        # Passa a data de processamento como um argumento extra para o DoFn
        validacao_results = (
            reservas_enriquecidas
            | "Validar e Padronizar" >> beam.ParDo(
                StandardizeAndValidate(),
                processing_date=trusted_options.processing_date
            ).with_outputs(StandardizeAndValidate.TAG_FAILED, main='success')
        )
        
        pcoll_validos = validacao_results['success']
        pcoll_invalidos = validacao_results[StandardizeAndValidate.TAG_FAILED]

        # --- Escrita (Destinos) ---
        
        # Destino 1: Tabela Principal (Trusted)
        (
            pcoll_validos
            | "Write Trusted" >> beam.io.WriteToBigQuery(
                table=f"{trusted_options.project_id}.{trusted_options.dataset_id_trusted}.trusted_reservas_enriquecidas",
                schema=SCHEMA_TRUSTED_RESERVAS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Ou WRITE_TRUNCATE
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Destino 2: Tabela de Erros (Dead-letter)
        (
            pcoll_invalidos
            | "Write Dead-Letter" >> beam.io.WriteToBigQuery(
                table=f"{trusted_options.project_id}.{trusted_options.dataset_id_trusted}.errors_trusted_reservas",
                # O schema para erros é geralmente 'error:STRING', 'data:STRING'
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()