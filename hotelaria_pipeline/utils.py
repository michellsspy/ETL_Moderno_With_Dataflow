import logging
import sys
import json

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