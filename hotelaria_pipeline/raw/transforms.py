# hotelaria_pipeline/raw/transforms.py (Versão FINAL CORRIGIDA)

import logging
import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
# --- CORREÇÃO 1: Importar NotFound do lugar correto ---
from google.api_core.exceptions import NotFound
# --- FIM CORREÇÃO 1 ---
from apache_beam.pvalue import TaggedOutput
import apache_beam as beam

# Configuração de logging (Mantida)
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class GetFolderNames(beam.DoFn):
    """Lista os nomes das pastas no GCS."""
    def __init__(self, bucket_name, prefix_to_search):
        self.bucket_name = bucket_name
        self.prefix_to_search = prefix_to_search
        self.storage_client = None

    def setup(self):
        try:
            self.storage_client = storage.Client()
            logger.info("Cliente GCS (GetFolderNames) inicializado.")
        except Exception as e: logger.error(f"Falha ao inicializar cliente GCS: {e}"); raise e

    def process(self, element, *args, **kwargs):
        logger.info(f"Iniciando busca por pastas em gs://{self.bucket_name}/{self.prefix_to_search}")
        try:
            if not self.storage_client: self.storage_client = storage.Client()
            iterator = self.storage_client.list_blobs(self.bucket_name, prefix=self.prefix_to_search, delimiter='/')
            list(iterator)
            pastas_encontradas = [p[len(self.prefix_to_search):].strip('/') for p in iterator.prefixes if p]
            nomes_unicos = list(set(pastas_encontradas))
            logger.info(f"Pastas encontradas: {nomes_unicos}")
            for nome_pasta in nomes_unicos: yield nome_pasta
        except Exception as e: logger.error(f"Erro ao conectar ou listar pastas no GCS: {e}")


class GcsCsvToBqUpsert(beam.DoFn):
    """
    Carrega CSV para staging, CRIA tabela final se não existir, e executa MERGE condicional.
    """
    TAG_FAILED = 'failed'
    TAG_SUCCESS = 'success'

    def __init__(self, projeto, dataset_id, bucket_name, gcs_transient_prefix, pk_map: dict, schema_map: dict):
        self.projeto = projeto
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.gcs_transient_prefix = gcs_transient_prefix
        self.pk_map = pk_map
        self.schema_map = schema_map
        self.storage_client = None
        self.bq_client = None

    def setup(self):
        try:
            self.storage_client = storage.Client(project=self.projeto)
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info(f"Clientes GCS e BigQuery inicializados para o projeto: {self.projeto}.")
        except Exception as e: logger.error(f"Falha ao inicializar clientes: {e}"); raise e

    def process(self, folder_name, *args, **kwargs):
        now = datetime.now()
        timestamp_suffix = now.strftime('%Y%m%d%H%M%S_%f')
        table_id = None
        temp_table_id = None

        try:
            # 1. Nomes e Caminhos
            blob_path = f"{self.gcs_transient_prefix}{folder_name}/{folder_name}.csv"
            gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
            nome_base_tabela = folder_name[7:] if folder_name.startswith('source_') else folder_name[3:] if folder_name.startswith('cl_') else folder_name
            table_name = f"raw_{nome_base_tabela}"
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            temp_table_name = f"_staging_{nome_base_tabela}_{timestamp_suffix}"
            temp_table_id = f"{self.projeto}.{self.dataset_id}.{temp_table_name}"
            logger.info(f"[{folder_name}] Iniciando UPSERT para: {table_id} (via staging: {temp_table_id})")

            # 2. Obter PK
            pk_cols = self.pk_map.get(table_name);
            if not pk_cols: raise ValueError(f"Nenhuma PK definida para a tabela: {table_name}")

            # 3. Obter Schemas (Final e Staging)
            table_schema_final = self.schema_map.get(table_name);
            if not table_schema_final: raise ValueError(f"Nenhum schema definido para a tabela: {table_name}")
            staging_schema = [f for f in table_schema_final if f.name not in ('insert_date', 'update_date')]

            # 4. Verificar Blob
            if not self.storage_client: self.storage_client = storage.Client(project=self.projeto)
            bucket = self.storage_client.bucket(self.bucket_name); blob = bucket.blob(blob_path)
            if not blob.exists(): logger.warning(f"[{folder_name}] Arquivo não encontrado: {gcs_uri}. Pulando."); return

            # 5. Carregar Staging
            job_config_load = bigquery.LoadJobConfig(
                schema=staging_schema, autodetect=False, source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                allow_quoted_newlines=True, quote_character='"', max_bad_records=100
            )
            if not self.bq_client: self.bq_client = bigquery.Client(project=self.projeto)
            logger.info(f"[{folder_name}] Iniciando job de LOAD do GCS para {temp_table_id}...")
            job_load = self.bq_client.load_table_from_uri(gcs_uri, temp_table_id, job_config=job_config_load)
            try:
                job_load.result()
                logger.info(f"[{folder_name}] Carga para staging {temp_table_id} concluída. Status: {job_load.state}")
                if job_load.errors: logger.warning(f"[{folder_name}] Job de LOAD concluído com erros tolerados.")
                if job_load.error_result: raise RuntimeError(f"Job de LOAD falhou fatalmente. Erro: {job_load.error_result}")
            except Exception as load_error:
                logger.error(f"[{folder_name}] Erro EXCEPCIONAL durante job_load.result(): {load_error}", exc_info=False)
                if hasattr(job_load, 'errors') and job_load.errors: logger.error(f"[{folder_name}] Detalhes dos erros do job de LOAD: {job_load.errors}")
                raise load_error

            # Obter colunas reais da staging
            try:
                staging_table = self.bq_client.get_table(temp_table_id); staging_cols = [f.name for f in staging_table.schema]; staging_cols_set = set(staging_cols)
            except Exception as get_staging_error: logger.error(f"[{folder_name}] Falha ao obter metadados da staging {temp_table_id}: {get_staging_error}", exc_info=True); raise get_staging_error

            # 5.5 Garantir Tabela Final
            #logger.info(f"[{folder_name}] Verificando/Criando tabela final: {table_id}")
            try:
                self.bq_client.get_table(table_id)
                #logger.info(f"[{folder_name}] Tabela final {table_id} já existe.")
            # --- CORREÇÃO 1: Usar a exceção NotFound importada ---
            except NotFound:
            # --- FIM CORREÇÃO 1 ---
                logger.info(f"[{folder_name}] Tabela final {table_id} não encontrada. Criando...")
                table_ref = bigquery.Table(table_id, schema=table_schema_final)
                self.bq_client.create_table(table_ref)
                logger.info(f"[{folder_name}] Tabela final {table_id} criada.")
            except Exception as check_create_error: logger.error(f"[{folder_name}] Erro ao verificar/criar tabela final {table_id}: {check_create_error}", exc_info=True); raise check_create_error

            # 6. Construir e Executar MERGE Condicional
            on_clause = " AND ".join([f"T.`{col}` = S.`{col}`" for col in pk_cols if col in staging_cols_set])
            if not on_clause: raise ValueError(f"Nenhuma PK válida na staging para {table_name}")
            update_cols = [col for col in staging_cols if col not in pk_cols]
            if not update_cols: update_set_clause = "T.update_date = @current_timestamp"
            else: update_set_clause = ",\n".join([f"T.`{col}` = S.`{col}`" for col in update_cols]) + f",\nT.update_date = @current_timestamp"
            match_condition_string = ""
            if update_cols: comparisons = [f"(T.`{col}` IS DISTINCT FROM S.`{col}`)" for col in update_cols]; match_condition_string = f"AND ({' OR '.join(comparisons)})"
            final_schema_cols_set = {f.name for f in table_schema_final}; insert_audit_cols = []
            if "insert_date" in final_schema_cols_set and "insert_date" not in staging_cols_set: insert_audit_cols.append("insert_date")
            if "update_date" in final_schema_cols_set and "update_date" not in staging_cols_set: insert_audit_cols.append("update_date")
            insert_cols_list = staging_cols + insert_audit_cols; insert_cols = ", ".join([f"`{col}`" for col in insert_cols_list])
            values_cols_list = [f"S.`{col}`" for col in staging_cols]
            if "insert_date" in final_schema_cols_set and "insert_date" not in staging_cols_set: values_cols_list.append("@current_timestamp")
            if "update_date" in final_schema_cols_set and "update_date" not in staging_cols_set: values_cols_list.append("NULL")
            values_cols = ", ".join(values_cols_list)

            merge_query = f"""
            MERGE `{table_id}` T USING `{temp_table_id}` S ON {on_clause}
            WHEN MATCHED {match_condition_string} THEN UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({values_cols})
            """
            query_params = [bigquery.ScalarQueryParameter("current_timestamp", "DATETIME", now)]
            job_config_query = bigquery.QueryJobConfig(query_parameters=query_params)

            logger.info(f"[{folder_name}] Executando MERGE (condicional) em {table_id}...")
            # --- CORREÇÃO 2: Passar job_config_query para job_config ---
            job_merge = self.bq_client.query(merge_query, job_config=job_config_query)
            # --- FIM CORREÇÃO 2 ---
            job_merge.result()

            affected_rows = job_merge.num_dml_affected_rows if job_merge.num_dml_affected_rows is not None else 0
            log_msg = f"MERGE para {table_id} concluído. (Linhas afetadas: {affected_rows})" if affected_rows > 0 else f"MERGE para {table_id} concluído. Nenhuma linha precisou ser atualizada."
            logger.info(f"[{folder_name}] {log_msg}")

            # 7. Limpar Staging
            self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            #logger.info(f"[{folder_name}] Tabela de staging {temp_table_id} excluída.")

            # 8. Emitir sucesso
            success_msg = f"Upsert para {table_id} concluído (Linhas afetadas: {affected_rows}). Origem: {gcs_uri}"
            yield TaggedOutput(self.TAG_SUCCESS, success_msg)

        except Exception as e:
            error_msg = f"Erro GERAL no processo de UPSERT para {folder_name}: {e}"
            logger.error(f"[{folder_name}] {error_msg}", exc_info=True)
            yield TaggedOutput(self.TAG_FAILED, f"{folder_name}: {error_msg}")
            try:
                if temp_table_id and self.bq_client: self.bq_client.delete_table(temp_table_id, not_found_ok=True); logger.warning(f"[{folder_name}] Staging {temp_table_id} de falha limpa.")
            except Exception as ce: logger.error(f"[{folder_name}] Erro ao limpar staging de falha {temp_table_id}: {ce}")