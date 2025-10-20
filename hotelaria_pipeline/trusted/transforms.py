import apache_beam as beam
import logging
from apache_beam.pvalue import TaggedOutput
from datetime import datetime

logger = logging.getLogger(__name__)

class StandardizeAndValidate(beam.DoFn):
    """
    Padroniza os dados (ex: uppercase) e valida tipos.
    Usa TaggedOutput para enviar registros inválidos para uma "dead-letter".
    """
    TAG_FAILED = 'failed'

    def process(self, element, processing_date):
        try:
            # 1. Padronização (Exemplos)
            element['status_reserva'] = element['status_reserva'].upper().strip()
            element['email_hospede'] = element['email_hospede'].lower().strip()
            
            # 2. Validação (Exemplo)
            if not element['id_reserva'] or not element['id_hospede']:
                raise ValueError("id_reserva ou id_hospede estão nulos ou vazios.")
                
            # Tenta converter valores (exemplo)
            element['valor_total_reserva'] = float(element['valor_total_reserva'])

            # 3. Adicionar campos de auditoria
            element['dt_processamento'] = processing_date
            element['dt_insert'] = datetime.utcnow().isoformat()
            
            yield element # Emite o elemento válido

        except Exception as e:
            error_msg = f"Falha na validação: {e} | Dados: {element}"
            logger.warning(error_msg)
            # Emite o registro falho + a mensagem de erro
            yield TaggedOutput(self.TAG_FAILED, {'error': error_msg, 'data': element})

class LeftJoin(beam.PTransform):
    """
    Um PTransform reutilizável para realizar um Left Join entre duas PCollections.
    """
    def __init__(self, pcoll_left, pcoll_right, on_key_left, on_key_right):
        self.pcoll_left = pcoll_left
        self.pcoll_right = pcoll_right
        self.on_key_left = on_key_left
        self.on_key_right = on_key_right

    def expand(self, p):
        def _make_kv_tuple(element, key_name):
            return (element[key_name], element)

        def _join_and_flatten(kv_tuple):
            (key, data) = kv_tuple
            left_data = data['left']
            right_data = data['right']
            
            # Se não houver correspondência no right (Left Join)
            if not right_data:
                for row_left in left_data:
                    yield row_left # Retorna apenas os dados da esquerda
                return

            # Se houver correspondência
            for row_left in left_data:
                for row_right in right_data:
                    # Cria um novo dicionário combinado
                    joined_row = {**row_left, **row_right}
                    yield joined_row

        left_kv = self.pcoll_left | f"Prepare KV Left ({self.on_key_left})" >> beam.Map(_make_kv_tuple, self.on_key_left)
        right_kv = self.pcoll_right | f"Prepare KV Right ({self.on_key_right})" >> beam.Map(_make_kv_tuple, self.on_key_right)

        joined = (
            {'left': left_kv, 'right': right_kv}
            | "CoGroupByKey (Join)" >> beam.CoGroupByKey()
            | "Flatten Join" >> beam.FlatMap(_join_and_flatten)
        )
        return joined