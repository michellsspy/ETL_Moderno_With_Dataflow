from apache_beam.options.pipeline_options import PipelineOptions
from hotelaria_pipeline.utils import setup_logging, parse_pk_map_json
from google.cloud import secretmanager
from google.cloud import storage
from datetime import datetime, timedelta
import apache_beam as beam
import tempfile
import logging
import sys
import os
import io  
 
# Buffer para armazenar os logs em memória
log_stream = io.StringIO()

# Configuração do logging para capturar logs INFO e ERROR
logging.basicConfig(
    level=logging.INFO,  # Captura logs INFO e ERROR
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Exibe logs no console
        logging.StreamHandler(log_stream)  # Salva logs no buffer
    ],
)
logger = logging.getLogger(__name__)

# Importando as Funções
from hotelaria_pipeline.raw.main_raw import run_main_raw
from hotelaria_pipeline.utils import upload_log_to_gcs, get_secret, save_secret_to_temp_file

# Recupera a chave e salva temporariamente
key_data = get_secret()
key_path = save_secret_to_temp_file(key_data)

# Configura as credenciais do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Pegando a data do dia atual
data_now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# Lista de funções main a serem executadas
list_main = [run_main_raw]  # Adicione mais funções aqui

if __name__ == "__main__":
    for main_func in list_main:
        try:
            logging.getLogger().setLevel(logging.INFO)
            run_main_raw(sys.argv)
        except Exception as e:
            logger.error(f"Erro na execução de {main_func.__name__}: {e}", exc_info=True)
    
    # Salva os logs no GCS após todas as execuções
    upload_log_to_gcs(logger,log_stream,data_now)