# hotelaria_pipeline/raw/transforms.py

import logging
import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound # Importar exceptions
from apache_beam.pvalue import TaggedOutput
import apache_beam as beam

# Configuração de logging (Mantida como antes)
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
        logger.info(f"Iniciando busca por pastas em gs://{self.bucket_name}/{self.prefix_to_search}")
        try:
            # Re-inicializa se necessário (robustez)
            if not self.storage_client: self.storage_client = storage.Client()

            iterator = self.storage_client.list_blobs(self.bucket_name, prefix=self.prefix_to_search, delimiter='/')
            list(iterator) # Força a iteração

            pastas_encontradas = []
            if iterator.prefixes:
                for folder_prefix in iterator.prefixes:
                    nome_pasta = folder_prefix[len(self.prefix_to_search):].strip('/')
                    if nome_pasta: pastas_encontradas.append(nome_pasta)

            nomes_unicos = list(set(pastas_encontradas))
            logger.info(f"Pastas encontradas: {nomes_unicos}")
            for nome_pasta in nomes_unicos: yield nome_pasta
        except Exception as e:
            logger.error(f"Erro ao conectar ou listar pastas no GCS: {e}")
            # Em um cenário real, emitir para TAG_FAILED pode ser útil aqui


class GcsCsvToBqUpsert(beam.DoFn):
    """
    Carrega CSV do GCS para staging, CRIA a tabela final se não existir,
    e executa MERGE (Upsert).
    """
    TAG_FAILED = 'failed'
    TAG_SUCCESS = 'success'

    # Recebe 'projeto' (vindo de --project_id) e 'schema_map'
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
            # Inicializa clientes com o projeto correto
            # Importante para garantir que as operações BQ/GCS usem o projeto passado
            self.storage_client = storage.Client(project=self.projeto)
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info(f"Clientes GCS e BigQuery inicializados para o projeto: {self.projeto}.")
        except Exception as e:
            logger.error(f"Falha ao inicializar clientes: {e}")
            raise e

    def process(self, folder_name, *args, **kwargs):
        now = datetime.now()
        timestamp_suffix = now.strftime('%Y%m%d%H%M%S_%f')
        table_id = None # Tabela final (ex: raw_hospedes)
        temp_table_id = None # Tabela de staging (ex: _staging_hospedes_...)

        try:
            # --- 1. Montar Nomes e Caminhos ---
            blob_path = f"{self.gcs_transient_prefix}{folder_name}/{folder_name}.csv"
            gcs_uri = f"gs://{self.bucket_name}/{blob_path}"

            # Mapeamento GCS -> BQ (Remove 'source_' ou 'cl_')
            nome_base_tabela = folder_name
            if folder_name.startswith('source_'): nome_base_tabela = folder_name[7:]
            elif folder_name.startswith('cl_'): nome_base_tabela = folder_name[3:]

            table_name = f"raw_{nome_base_tabela}" # Ex: raw_hospedes
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            temp_table_name = f"_staging_{nome_base_tabela}_{timestamp_suffix}"
            temp_table_id = f"{self.projeto}.{self.dataset_id}.{temp_table_name}"

            logger.info(f"[{folder_name}] Iniciando UPSERT para: {table_id} (via staging: {temp_table_id})")
            logger.info(f"[{folder_name}] Lendo dados de: {gcs_uri}")

            # --- 2. Obter Chave Primária ---
            pk_cols = self.pk_map.get(table_name)
            if not pk_cols: raise ValueError(f"Nenhuma PK definida no pk_map para a tabela: {table_name}")

            # --- 3. Obter Schema Final e Criar Schema de Staging ---
            table_schema_final = self.schema_map.get(table_name)
            if not table_schema_final: raise ValueError(f"Nenhum schema definido no schema_map para a tabela: {table_name}")
            # Cria schema para staging SEM as colunas de auditoria que NÃO estão no CSV
            staging_schema = [f for f in table_schema_final if f.name not in ('insert_date', 'update_date')]
            logger.info(f"[{folder_name}] Schema de staging ({len(staging_schema)} cols) e final ({len(table_schema_final)} cols) definidos.")

            # --- 4. Verificar existência do Blob ---
            if not self.storage_client: self.storage_client = storage.Client(project=self.projeto)
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            if not blob.exists():
                logger.warning(f"[{folder_name}] Arquivo não encontrado: {gcs_uri}. Pulando.")
                return # Pula este folder_name

            # --- 5. PASSO 1: Carregar na Tabela de Staging ---
            job_config_load = bigquery.LoadJobConfig(
                schema=staging_schema, # Usa o schema SEM auditoria
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Sempre recria staging
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                allow_quoted_newlines=True, # Tenta lidar com CSVs mal formatados
                quote_character='"',
                max_bad_records=100 # Permite até 100 linhas ruins no CSV (ajuste!)
            )
            # Re-inicializa cliente BQ se necessário
            if not self.bq_client: self.bq_client = bigquery.Client(project=self.projeto)

            logger.info(f"[{folder_name}] Iniciando job de LOAD do GCS para {temp_table_id}...")
            job_load = self.bq_client.load_table_from_uri(gcs_uri, temp_table_id, job_config=job_config_load)
            try:
                job_load.result() # Espera terminar
                logger.info(f"[{folder_name}] Carga para staging {temp_table_id} concluída. Status: {job_load.state}")
                # Loga erros não fatais (se max_bad_records > 0)
                if job_load.errors:
                    logger.warning(f"[{folder_name}] Job de LOAD concluído com erros tolerados:")
                    for error in job_load.errors[:5]: logger.warning(f"  - {error.get('reason','N/A')}: {error.get('message','N/A')}")
                # Verifica erro fatal
                if job_load.error_result: raise RuntimeError(f"Job de LOAD falhou fatalmente. Erro: {job_load.error_result}")
            except Exception as load_error:
                # Log detalhado em caso de falha no LOAD
                logger.error(f"[{folder_name}] Erro EXCEPCIONAL durante job_load.result(): {load_error}", exc_info=False)
                if hasattr(job_load, 'errors') and job_load.errors:
                    logger.error(f"[{folder_name}] Detalhes dos erros do job de LOAD:")
                    for error in job_load.errors: logger.error(f"  - {error}")
                raise load_error # Importante re-lançar para falhar o DoFn

            # --- Obter colunas DA STAGING ---
            # Necessário pois a carga pode ter pulado linhas/colunas se max_bad_records > 0
            try:
                staging_table = self.bq_client.get_table(temp_table_id)
                staging_cols = [field.name for field in staging_table.schema]
                staging_cols_set = set(staging_cols)
                logger.info(f"[{folder_name}] Staging table {temp_table_id} possui {len(staging_cols)} colunas após carga.")
            except Exception as get_staging_error:
                 logger.error(f"[{folder_name}] Falha ao obter metadados da staging {temp_table_id}: {get_staging_error}", exc_info=True)
                 raise get_staging_error

            # --- PASSO 5.5: Garantir que a Tabela Final Exista ---
            logger.info(f"[{folder_name}] Verificando/Criando tabela final: {table_id}")
            try:
                self.bq_client.get_table(table_id) # Tenta buscar
                logger.info(f"[{folder_name}] Tabela final {table_id} já existe.")
            except NotFound:
                # Se não encontrar, cria usando o schema FINAL (com auditoria)
                logger.info(f"[{folder_name}] Tabela final {table_id} não encontrada. Criando...")
                table_ref = bigquery.Table(table_id, schema=table_schema_final)
                # Aqui você pode adicionar particionamento/clusterização se desejar
                # Ex: table_ref.time_partitioning = bigquery.TimePartitioning(field="data_referencia")
                self.bq_client.create_table(table_ref)
                logger.info(f"[{folder_name}] Tabela final {table_id} criada com sucesso.")
            except Exception as check_create_error:
                 logger.error(f"[{folder_name}] Erro ao verificar/criar tabela final {table_id}: {check_create_error}", exc_info=True)
                 raise check_create_error
            # --- FIM DO PASSO 5.5 ---

            # --- 6. PASSO 2: Construir e Executar a Query MERGE ---
            # Junta apenas com colunas PK que existem na staging
            on_clause = " AND ".join([f"T.`{col}` = S.`{col}`" for col in pk_cols if col in staging_cols_set])
            if not on_clause: raise ValueError(f"Nenhuma coluna PK válida encontrada na staging para {table_name}")

            # Atualiza apenas colunas que existem na staging e não são PKs
            update_cols = [col for col in staging_cols if col not in pk_cols]
            if not update_cols: update_clause = "T.update_date = @current_timestamp"
            else:
                update_clause = ",\n".join([f"T.`{col}` = S.`{col}`" for col in update_cols])
                update_clause += f",\nT.update_date = @current_timestamp"

            # Prepara colunas para o INSERT (colunas da staging + auditoria ausente)
            final_schema_cols_set = {field.name for field in table_schema_final}
            insert_audit_cols = []
            if "insert_date" in final_schema_cols_set and "insert_date" not in staging_cols_set: insert_audit_cols.append("insert_date")
            if "update_date" in final_schema_cols_set and "update_date" not in staging_cols_set: insert_audit_cols.append("update_date")

            insert_cols_list = staging_cols + insert_audit_cols
            insert_cols = ", ".join([f"`{col}`" for col in insert_cols_list])

            values_cols_list = [f"S.`{col}`" for col in staging_cols]
            if "insert_date" in final_schema_cols_set and "insert_date" not in staging_cols_set: values_cols_list.append("@current_timestamp")
            if "update_date" in final_schema_cols_set and "update_date" not in staging_cols_set: values_cols_list.append("NULL")
            values_cols = ", ".join(values_cols_list)

            # Monta a query MERGE
            merge_query = f"""
            MERGE `{table_id}` T
            USING `{temp_table_id}` S
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({values_cols})
            """

            # Define o parâmetro de tempo (agora como DATETIME)
            query_params = [bigquery.ScalarQueryParameter("current_timestamp", "DATETIME", now)]
            job_config_query = bigquery.QueryJobConfig(query_parameters=query_params)

            # Executa o MERGE
            logger.info(f"[{folder_name}] Executando MERGE em {table_id}...")
            job_merge = self.bq_client.query(merge_query, job_config=job_config_query)
            job_merge.result() # Espera terminar

            affected_rows = job_merge.num_dml_affected_rows if job_merge.num_dml_affected_rows is not None else 0
            logger.info(f"[{folder_name}] MERGE para {table_id} concluído. (Linhas afetadas: {affected_rows})")

            # --- 7. PASSO 3: Limpar Tabela de Staging ---
            self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"[{folder_name}] Tabela de staging {temp_table_id} excluída.")

            # --- 8. Emitir sucesso ---
            success_msg = f"Upsert para {table_id} concluído (Linhas afetadas: {affected_rows}). Origem: {gcs_uri}"
            yield TaggedOutput(self.TAG_SUCCESS, success_msg)

        # --- Tratamento Geral de Erros ---
        except Exception as e:
            error_msg = f"Erro GERAL no processo de UPSERT para {folder_name}: {e}"
            logger.error(f"[{folder_name}] {error_msg}", exc_info=True) # Log completo
            yield TaggedOutput(self.TAG_FAILED, f"{folder_name}: {error_msg}")
            # Tenta limpar a staging mesmo em caso de erro
            try:
                if temp_table_id and self.bq_client:
                    self.bq_client.delete_table(temp_table_id, not_found_ok=True)
                    logger.warning(f"[{folder_name}] Tabela de staging {temp_table_id} de falha foi limpa.")
            except Exception as ce:
                logger.error(f"[{folder_name}] Erro ao limpar tabela de staging de falha {temp_table_id}: {ce}")
