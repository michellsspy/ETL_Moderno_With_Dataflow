from google.cloud import storage
import logging
import sys
import json
from google.cloud import secretmanager
import tempfile
import os

def setup_logging():
    """Configura o logger padrão do Beam para printar no stdout."""
    logger = logging.getLogger(__name__)
    
    # Evita reconfiguração se já estiver configurado
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    logging.getLogger().setLevel(logging.INFO)
    logger.info("Configuração de logging aplicada.")
    return logger

def parse_pk_map_json(pk_map_json_str: str) -> dict:
    """Converte a string JSON de pk_map em um dicionário Python."""
    try:
        pk_map = json.loads(pk_map_json_str)
        if not isinstance(pk_map, dict):
            raise ValueError("pk_map_json não é um dicionário JSON válido.")
        return pk_map
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar pk_map_json: {e}")
        raise e
    

def upload_log_to_gcs(logger,log_stream,data_now):
    """Salva os logs no GCS."""
    log_file = f"log_{data_now}.log"

    try:
        client = storage.Client()
        bucket_name = "bk-etl-hotelaria"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"log/{log_file}")

        # Obtém os logs do buffer em memória
        log_content = log_stream.getvalue()

        # Faz o upload do conteúdo para o GCS
        blob.upload_from_string(log_content)
        logger.info(f"Log salvo em gs://{bucket_name}/log/{log_file}")
    except Exception as e:
        logger.error(f"Erro ao salvar log no GCS: {e}", exc_info=True)


def get_secret():
    # Recuperando a secret
    PROJECT_ID = "992325149095"
    SECRET_NAME = "etl-hoteis"  # Nome do Secret salvo no Secret Manager

    """Recupera o secret do Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"

        response = client.access_secret_version(name=secret_path)
        secret_data = response.payload.data.decode("UTF-8")

        logging.info("\nSecret recuperado com sucesso.\n")
        return secret_data
    except Exception as e:
        logging.error(f"\nErro ao acessar o Secret Manager: {e}\n", exc_info=True)
        raise

def save_secret_to_temp_file(secret_data):
    """Salva o secret em um arquivo temporário com permissões restritas."""
    try:
        # Cria um arquivo temporário com um nome único
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as temp_file:
            temp_file.write(secret_data)
            temp_path = temp_file.name

        # Aplica permissões restritas ao arquivo
        os.chmod(temp_path, 0o600)
        logging.info(f"Secret salvo temporariamente em {temp_path} com permissões 0o600.")
        return temp_path
    except Exception as e:
        logging.error(f"Erro ao salvar o secret em arquivo temporário: {e}", exc_info=True)
        raise