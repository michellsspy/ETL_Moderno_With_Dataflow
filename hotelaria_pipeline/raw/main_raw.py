import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict

from hotelaria_pipeline.pipeline_options import RawPipelineOptions
from hotelaria_pipeline.raw.transforms import GetFolderNames, GcsCsvToBqUpsert
from hotelaria_pipeline.utils import setup_logging, parse_pk_map_json
import logging

def run():
    """Ponto de entrada principal para o pipeline da camada RAW."""
    
    # 1. Configuração e Parse de Opções
    options = PipelineOptions(flags=None)
    raw_options = options.view_as(RawPipelineOptions)
    
    logger = setup_logging()
    logger.info("Iniciando pipeline RAW (GCS Transient -> BQ Raw Upsert)")
    
    # 2. Parsear o mapa de PKs
    try:
        pk_map = parse_pk_map_json(raw_options.pk_map_json)
        logger.info(f"Mapa de chaves primárias carregado: {pk_map}")
    except Exception as e:
        logger.error(f"Falha ao parsear pk_map_json. Encerrando. Erro: {e}")
        return

    # 3. Instanciar PTransforms com estado (clientes, etc)
    get_folder_names_transform = GetFolderNames(
        bucket_name=raw_options.bucket_name,
        prefix_to_search=raw_options.gcs_transient_prefix
    )
    
    gcs_to_bq_upsert_transform = GcsCsvToBqUpsert(
        projeto=raw_options.project_id,
        dataset_id=raw_options.dataset_id_raw,
        bucket_name=raw_options.bucket_name,
        gcs_transient_prefix=raw_options.gcs_transient_prefix,
        pk_map=pk_map
    )

    # 4. Definição do Pipeline
    with beam.Pipeline(options=options) as p:
        
        # Passo 1: Impulso inicial e listagem de pastas no GCS
        folder_names = (
            p
            | "Iniciar Pipeline" >> beam.Create([None])
            | "Listar Pastas no GCS" >> beam.ParDo(get_folder_names_transform)
        )

        # Passo 2: Executar o Upsert para cada pasta (seu script)
        # Usamos with_outputs para capturar falhas
        results = (
            folder_names
            | "Executar GCS-BQ Upsert" >> beam.ParDo(
                gcs_to_bq_upsert_transform
            ).with_outputs(
                GcsCsvToBqUpsert.TAG_FAILED, 
                main=GcsCsvToBqUpsert.TAG_SUCCESS
            )
        )
        
        # 5. Coleta e Log de Resultados (Opcional, bom para monitoramento)
        (
            results[GcsCsvToBqUpsert.TAG_SUCCESS]
            | "Log Sucessos" >> beam.Map(lambda msg: logger.info(f"SUCESSO: {msg}"))
        )
        
        (
            results[GcsCsvToBqUpsert.TAG_FAILED]
            | "Log Falhas" >> beam.Map(lambda msg: logger.error(f"FALHA: {msg}"))
            # TODO: Escrever falhas em uma tabela de log/PubSub para alertas
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()