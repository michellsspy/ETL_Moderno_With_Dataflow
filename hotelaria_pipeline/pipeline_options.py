from apache_beam.options.pipeline_options import PipelineOptions

class HotelariaPipelineOptions(PipelineOptions):
    """Opções de pipeline base com parâmetros comuns a todas as camadas."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            required=True,
            help='ID do Projeto GCP'
        )
        parser.add_argument(
            '--bucket_name',
            default='bk-etl-hotelaria',
            help='Nome do bucket GCS para transient e staging.'
        )

class RawPipelineOptions(HotelariaPipelineOptions):
    """Opções específicas para o pipeline da camada RAW (GCS Transient -> BQ Raw)."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--dataset_id_raw',
            required=True,
            help='Dataset do BigQuery para a camada RAW (ex: raw_hotelaria)'
        )
        parser.add_argument(
            '--gcs_transient_prefix',
            default='transient/',
            help='Prefixo no GCS onde as pastas das tabelas estão (ex: transient/)'
        )
        parser.add_argument(
            '--pk_map_json',
            required=True,
            help='String JSON mapeando tabelas para suas chaves primárias. '
                 'Ex: \'{"raw_hotel_reservas": ["id_reserva"]}\''
        )

class TrustedPipelineOptions(HotelariaPipelineOptions):
    """Opções específicas para o pipeline da camada TRUSTED (Raw -> Trusted)."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--dataset_id_raw',
            required=True,
            help='Dataset do BigQuery de origem (camada RAW)'
        )
        parser.add_argument(
            '--dataset_id_trusted',
            required=True,
            help='Dataset do BigQuery de destino (camada TRUSTED)'
        )
        parser.add_argument(
            '--processing_date',
            required=True,
            help='Data de processamento (YYYY-MM-DD) para pipelines batch.'
        )

class RefinedPipelineOptions(HotelariaPipelineOptions):
    """Opções específicas para o pipeline da camada REFINED (Trusted -> Refined)."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--dataset_id_trusted',
            required=True,
            help='Dataset do BigQuery de origem (camada TRUSTED)'
        )
        parser.add_argument(
            '--dataset_id_refined',
            required=True,
            help='Dataset do BigQuery de destino (camada REFINED)'
        )
        parser.add_argument(
            '--processing_date',
            required=True,
            help='Data de processamento (YYYY-MM-DD) para pipelines batch.'
        )