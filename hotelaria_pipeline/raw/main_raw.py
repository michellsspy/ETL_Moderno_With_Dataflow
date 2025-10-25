import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from hotelaria_pipeline import schemas
from hotelaria_pipeline.raw.transforms import GetFolderNames, GcsCsvToBqUpsert
from hotelaria_pipeline.utils import setup_logging, parse_pk_map_json
import logging
import sys
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict

from hotelaria_pipeline import schemas
from hotelaria_pipeline.raw.transforms import GetFolderNames, GcsCsvToBqUpsert
from hotelaria_pipeline.utils import setup_logging


def run_main_raw(argv=None):
    """Ponto de entrada principal para o pipeline da camada RAW."""
    data_now = datetime.now().strftime("%Y%m%d%H%M")

    # 1. Configuração base das opções do Dataflow
    base_options_dict = {
        'project': 'etl-hoteis',
        'runner': 'DataflowRunner',
        'streaming': False,
        'job_name': f"etl-transient-to-raw-{data_now}",
        'temp_location': 'gs://bk-etl-hotelaria/temp',
        'staging_location': 'gs://bk-etl-hotelaria/staging_location',
        'template_location': f'gs://bk-etl-hotelaria/templates/template-etl-hotel-{data_now}',
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
        'worker_machine_type': 'n1-standard-4',
        'num_workers': 1,
        'max_num_workers': 3,
        'disk_size_gb': 25,
        'region': 'us-central1',
        'save_main_session': False,
        'prebuild_sdk_container_engine': 'cloud_build',
        'docker_registry_push_url': 'us-central1-docker.pkg.dev/etl-hotel/etl-transient-to-raw/etl-transient-dev',
        'sdk_container_image': 'us-central1-docker.pkg.dev/etl-hotel/etl-transient-to-raw/etl-transient-dev:latest',
        'sdk_location': 'container',
        'requirements_file': './requirements.txt',
        'setup_file': './setup.py',
        'metabase_file': './metadata_raw.json',
        'service_account_email': 'etl-hoteis@etl-hoteis.iam.gserviceaccount.com'
    }

    # 2. Combinar as opções fixas com as opções de linha de comando (RawPipelineOptions)
    options = PipelineOptions(flags=argv, **base_options_dict)
    raw_options = options

    # 3. Configuração de logs
    logger = setup_logging()
    logger.info("Iniciando pipeline RAW (GCS Transient -> BQ Raw Upsert)")


    # 5. Mapa de schemas
    SCHEMA_MAP = {
        "raw_consumos": schemas.SCHEMA_RAW_CONSUMOS,
        "raw_faturas": schemas.SCHEMA_RAW_FATURAS,
        "raw_hospedes": schemas.SCHEMA_RAW_HOSPEDES,
        "raw_hoteis": schemas.SCHEMA_RAW_HOTEIS,
        "raw_quartos": schemas.SCHEMA_RAW_QUARTOS,
        "raw_reservas": schemas.SCHEMA_RAW_RESERVAS,
        "raw_reservas_ota": schemas.SCHEMA_RAW_RESERVAS_OTA,
    }
    logger.info("Mapa de Schemas carregado.")

    # 6. Transforms principais
    get_folder_names_transform = GetFolderNames(
        bucket_name='transient',
        prefix_to_search='source'
    )

    gcs_to_bq_upsert_transform = GcsCsvToBqUpsert(
        projeto='etl-hoteis',
        dataset_id='raw_hotelaria',
        bucket_name='transient',
        gcs_transient_prefix='source',
        schema_map=SCHEMA_MAP
    )

    # 7. Definição da pipeline
    with beam.Pipeline(options=options) as p:
        folder_names = (
            p
            | "Iniciar Pipeline" >> beam.Create([None])
            | "Listar Pastas no GCS" >> beam.ParDo(get_folder_names_transform)
        )

        results = (
            folder_names
            | "Executar GCS-BQ Upsert" >> beam.ParDo(
                gcs_to_bq_upsert_transform
            ).with_outputs(
                GcsCsvToBqUpsert.TAG_FAILED,
                main=GcsCsvToBqUpsert.TAG_SUCCESS
            )
        )

        (
            results[GcsCsvToBqUpsert.TAG_SUCCESS]
            | "Log Sucessos" >> beam.Map(lambda msg: logger.info(f"SUCESSO: {msg}"))
        )

        (
            results[GcsCsvToBqUpsert.TAG_FAILED]
            | "Log Falhas" >> beam.Map(lambda msg: logger.error(f"FALHA: {msg}"))
        )


