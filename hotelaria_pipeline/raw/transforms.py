import logging
import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from apache_beam.pvalue import TaggedOutput
import apache_beam as beam

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class GetFolderNames(beam.DoFn):
    """
    Lista os nomes das "pastas" (prefixos) encontradas
    em um caminho específico do Google Cloud Storage (GCS).
    """
    
    def __init__(self, bucket_name, prefix_to_search):
        self.bucket_name = bucket_name
        self.prefix_to_search = prefix_to_search
        self.storage_client = None

    def setup(self):
        try:
            self.storage_client = storage.Client()
            logger.info("Cliente GCS (GetFolderNames) inicializado.")
        except Exception as e:
            logger.error(f"Falha ao inicializar cliente GCS: {e}")
            raise e

    def process(self, element, *args, **kwargs):
        """
        Execução Principal do DoFn.
        """
        logger.info(f"Iniciando busca por pastas em gs://{self.bucket_name}/{self.prefix_to_search}")

        try:
            # Usamos list_blobs com um delimitador '/'
            iterator = self.storage_client.list_blobs(
                self.bucket_name, 
                prefix=self.prefix_to_search, 
                delimiter='/'
            )
            
            # Força a iteração para popular .prefixes
            list(iterator) 
            
            pastas_encontradas = []
            
            if iterator.prefixes:
                for folder_prefix in iterator.prefixes:
                    # Retorno: "transient/nome_da_pasta/"
                    # 1. Remove o prefixo inicial
                    nome_pasta = folder_prefix[len(self.prefix_to_search):] 
                    # 2. Remove a barra final
                    nome_pasta_limpo = nome_pasta.strip('/')
                    
                    if nome_pasta_limpo:
                        pastas_encontradas.append(nome_pasta_limpo)
            
            nomes_unicos = list(set(pastas_encontradas))
            logger.info(f"Pastas encontradas: {nomes_unicos}")
            
            for nome_pasta in nomes_unicos:
                yield nome_pasta  # Retorna os nomes das pastas

        except Exception as e:
            logger.error(f"Erro ao conectar ou listar pastas no GCS: {e}")
            raise e


class GcsCsvToBqUpsert(beam.DoFn):
    """
    Carrega CSV do GCS para uma tabela de staging e, em seguida,
    executa um MERGE (Upsert) na tabela de destino final.
    (Script fornecido por você, adaptado para esta classe)
    """
    TAG_FAILED = 'failed'
    TAG_SUCCESS = 'success'

    def __init__(self, projeto, dataset_id, bucket_name, gcs_transient_prefix, pk_map: dict):
        self.projeto = projeto
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.gcs_transient_prefix = gcs_transient_prefix
        self.pk_map = pk_map
        self.storage_client = None
        self.bq_client = None

    def setup(self):
        try:
            self.storage_client = storage.Client()
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Clientes GCS e BigQuery (GcsCsvToBqUpsert) inicializados.")
        except Exception as e:
            logger.error(f"Falha ao inicializar clientes: {e}")
            raise e

    def process(self, folder_name, *args, **kwargs):
        now = datetime.now()
        timestamp_suffix = now.strftime('%Y%m%d%H%M%S_%f') 

        try:
            # 1. Montar Nomes e Caminhos
            blob_path = f"{self.gcs_transient_prefix}{folder_name}/{folder_name}.csv"
            gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
            
            # Remove o prefixo "cl_" (se existir) para gerar nome da tabela
            nome_base_tabela = folder_name
            if folder_name.startswith('cl_'):
                 nome_base_tabela = folder_name[3:] # Remove 'cl_'
                 
            table_name = f"raw_{nome_base_tabela}"
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            
            temp_table_name = f"_staging_{table_name}_{timestamp_suffix}"
            temp_table_id = f"{self.projeto}.{self.dataset_id}.{temp_table_name}"

            logger.info(f"Iniciando UPSERT para: {table_id} (via staging: {temp_table_id})")
            logger.info(f"Lendo dados de: {gcs_uri}")

            # 2. Obter Chave Primária
            pk_cols = self.pk_map.get(table_name)
            if not pk_cols:
                error_msg = f"Nenhuma chave primária definida no pk_map para a tabela: {table_name}"
                logger.error(error_msg)
                yield TaggedOutput(self.TAG_FAILED, f"{folder_name}: {error_msg}")
                return

            # 3. Verificar existência do Blob
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            if not blob.exists():
                error_msg = f"Arquivo não encontrado: {gcs_uri}. Pulando este item."
                logger.warning(error_msg)
                yield TaggedOutput(self.TAG_FAILED, error_msg)
                return

            # 4. PASSO 1: Carregar na Tabela de Staging (Direto do GCS)
            job_config_load = bigquery.LoadJobConfig(
                autodetect=True, # Como no seu script
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1, 
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            
            job_load = self.bq_client.load_table_from_uri(
                gcs_uri, temp_table_id, job_config=job_config_load
            )
            job_load.result() 
            
            staging_table = self.bq_client.get_table(temp_table_id)
            staging_cols = [field.name for field in staging_table.schema]
            
            logger.info(f"Carga direta para staging {temp_table_id} concluída.")

            # 5. PASSO 2: Construir e Executar a Query MERGE
            on_clause = " AND ".join([f"T.{col} = S.{col}" for col in pk_cols])
            
            update_cols = [col for col in staging_cols if col not in pk_cols]
            if not update_cols:
                 # Caso a tabela só tenha Pks, não há o que atualizar
                 update_clause = "T.update_date = @current_timestamp"
            else:
                update_clause = ",\n".join([f"T.{col} = S.{col}" for col in update_cols])
                update_clause += f",\nT.update_date = @current_timestamp" 

            insert_cols_list = staging_cols + ["insert_date", "update_date"]
            insert_cols = ", ".join([f"`{col}`" for col in insert_cols_list])
            
            values_cols_list = [f"S.`{col}`" for col in staging_cols] \
                               + ["@current_timestamp", "NULL"]
            values_cols = ", ".join(values_cols_list)

            merge_query = f"""
            MERGE `{table_id}` T
            USING `{temp_table_id}` S
            ON {on_clause}
            
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({values_cols})
            """

            query_params = [
                bigquery.ScalarQueryParameter("current_timestamp", "TIMESTAMP", now),
            ]
            job_config_query = bigquery.QueryJobConfig(query_parameters=query_params)
            
            logger.info(f"Executando MERGE em {table_id}...")
            job_merge = self.bq_client.query(merge_query, job_config=job_config_query)
            job_merge.result() 

            logger.info(f"MERGE para {table_id} concluído. (Linhas afetadas: {job_merge.num_dml_affected_rows})")

            # 6. PASSO 3: Limpar (Excluir) Tabela de Staging
            self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"Tabela de staging {temp_table_id} excluída.")

            # 7. Emitir sucesso
            success_msg = f"Upsert (via GCS-Load) para {table_id} concluído (Linhas afetadas: {job_merge.num_dml_affected_rows})."
            yield TaggedOutput(self.TAG_SUCCESS, success_msg)

        except Exception as e:
            error_msg = f"Erro no processo de UPSERT para {folder_name}: {e}"
            logger.error(error_msg, exc_info=True)
            yield TaggedOutput(self.TAG_FAILED, error_msg)
            
            # Tenta limpar o staging se ele foi criado
            try:
                if 'temp_table_id' in locals() and self.bq_client:
                    self.bq_client.delete_table(temp_table_id, not_found_ok=True)
                    logger.warning(f"Tabela de staging {temp_table_id} de falha foi limpa.")
            except Exception as ce:
                logger.error(f"Erro ao limpar tabela de staging de falha {temp_table_id}: {ce}")