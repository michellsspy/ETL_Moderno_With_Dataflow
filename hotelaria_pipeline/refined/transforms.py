import apache_beam as beam
import logging

logger = logging.getLogger(__name__)

class ExtractDataParaAgregacao(beam.DoFn):
    """Prepara os dados para a agregação, criando a chave (id_hotel, data)."""

    def process(self, element, processing_date):
        try:
            # --- Adicionar verificação e logs ---
            if not isinstance(element, dict):
                logger.warning(f"Elemento inesperado (não é dict): {element}")
                return # Pular elemento não-dicionário

            id_hotel = element.get('id_hotel')
            data_checkin = element.get('data_checkin') # Assumindo que é um objeto Date ou string ISO
            receita_val = element.get('valor_total_reserva')
            nome_hotel_val = element.get('nome_hotel')
            cidade_hotel_val = element.get('cidade_hotel')

            # Verificar se os campos essenciais existem
            if id_hotel is None or data_checkin is None or receita_val is None:
                logger.warning(f"Campos essenciais ausentes para agregação: {element}")
                return # Pular registro incompleto

            # Tentar converter receita para float
            try:
                receita_float = float(receita_val)
            except (ValueError, TypeError):
                logger.warning(f"Não foi possível converter 'valor_total_reserva' para float: {receita_val} em {element}")
                return # Pular registro com receita inválida

            # Garantir que data_checkin seja string ISO para a chave
            if hasattr(data_checkin, 'isoformat'): # Se for objeto Date/Datetime
                 data_str = data_checkin.isoformat()
            else: # Se já for string (ou outro tipo)
                 data_str = str(data_checkin)

            # Chave de agregação
            key = f"{id_hotel}|{data_str}"

            # Dados a serem agregados
            value = {
                "receita": receita_float,
                "reservas": 1,
                "nome_hotel": nome_hotel_val if nome_hotel_val is not None else "N/A",
                "cidade_hotel": cidade_hotel_val if cidade_hotel_val is not None else "N/A"
            }
            # --- Fim das adições/verificações ---

            yield (key, value)

        except Exception as e:
            # Logar o erro mas não parar o pipeline inteiro por causa de um registro
            logger.error(f"Erro ao extrair dados para agregação: {e} | Dados: {element}", exc_info=True)


class AggregateAndFormat(beam.PTransform):
    """
    Agrega os dados por chave e formata a saída para o BQ.
    """
    
    def __init__(self, processing_date):
        self.processing_date = processing_date

    # Dentro da classe AggregateAndFormat
# Dentro da classe AggregateAndFormat
    def _sum_metrics(self, elements):
        receita_total = 0.0
        reservas_totais = 0
        nome_hotel = "N/A"
        cidade_hotel = "N/A"
        first_element_processed = False

        for item in elements:
            if not first_element_processed:
                nome_hotel = item.get('nome_hotel', "N/A")
                cidade_hotel = item.get('cidade_hotel', "N/A")
                first_element_processed = True

            # ERRO ESTAVA AQUI: As somas estavam fora do loop no código anterior
            # Esta versão (m-96) já está correta, mas vamos confirmar.
            receita_total += item.get('receita', 0.0)
            reservas_totais += item.get('reservas', 0)

        return {
            "receita_total_dia": receita_total,
            "reservas_confirmadas_dia": reservas_totais,
            "nome_hotel": nome_hotel,
            "cidade_hotel": cidade_hotel
        }

    def _format_output(self, kv_tuple, processing_date):
        (key, metrics) = kv_tuple
        (id_hotel, data_referencia) = key.split('|')
        
        # TODO: Lógica para calcular taxa de ocupação (exigiria side input com total de quartos)
        taxa_ocupacao = 0.85 # Valor Fixo (placeholder)

        return {
            "id_hotel": id_hotel,
            "nome_hotel": metrics['nome_hotel'],
            "cidade_hotel": metrics['cidade_hotel'],
            "data_referencia": data_referencia,
            "receita_total_dia": metrics['receita_total_dia'],
            "reservas_confirmadas_dia": metrics['reservas_confirmadas_dia'],
            "taxa_ocupacao_dia": taxa_ocupacao,
            "dt_processamento": processing_date
        }

    def expand(self, pcoll):
        return (
            pcoll
            # Agrega por chave (id_hotel|data)
            | "Combine (Sum Metrics)" >> beam.CombinePerKey(self._sum_metrics)
            # Formata a saída para o schema do BQ
            | "Format BQ Output" >> beam.Map(self._format_output, self.processing_date)
        )