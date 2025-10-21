import apache_beam as beam
import logging

logger = logging.getLogger(__name__)

class ExtractDataParaAgregacao(beam.DoFn):
    """Prepara os dados para a agregação, criando a chave (id_hotel, data)."""
    
    def process(self, element, processing_date):
        try:
            # Exemplo de lógica de negócios:
            # Queremos agregar receita por hotel, por dia.
            
            id_hotel = element['id_hotel']
            data_checkin = element['data_checkin'] # Assumindo que é um objeto Date
            receita = element['valor_total_reserva']
            
            # Chave de agregação
            key = f"{id_hotel}|{data_checkin.isoformat()}"
            
            # Dados a serem agregados
            value = {
                "receita": float(receita),
                "reservas": 1,
                # Salva os dados do hotel para não precisar de outro join
                "nome_hotel": element['nome_hotel'],
                "cidade_hotel": element['cidade_hotel']
            }
            
            yield (key, value)
            
        except Exception as e:
            logger.warning(f"Erro ao extrair dados para agregação: {e} | Dados: {element}")
            # Ignora o registro falho na agregação


class AggregateAndFormat(beam.PTransform):
    """
    Agrega os dados por chave e formata a saída para o BQ.
    """
    
    def __init__(self, processing_date):
        self.processing_date = processing_date

    # Dentro da classe AggregateAndFormat
    def _sum_metrics(self, elements):
        # elements é um iterável (ex: _ReiterableChain) de dicionários 'value'
        # logger.debug(f"[_sum_metrics] Aggregating elements...") # Log pode ser útil

        receita_total = 0.0
        reservas_totais = 0
        nome_hotel = "N/A" # Valores padrão caso 'elements' seja vazio
        cidade_hotel = "N/A"
        first_element_processed = False

        # --- CORREÇÃO: Iterar para calcular e pegar dados ---
        for item in elements:
            # Pega os dados do hotel APENAS do primeiro item
            if not first_element_processed:
                nome_hotel = item.get('nome_hotel', "N/A")
                cidade_hotel = item.get('cidade_hotel', "N/A")
                first_element_processed = True

            # Acumula as métricas
            receita_total += item.get('receita', 0.0)
            reservas_totais += item.get('reservas', 0)
        # --- FIM DA CORREÇÃO ---

        # O tratamento de caso vazio não é estritamente necessário
        # pois CombinePerKey geralmente não chama o combiner para chaves sem elementos,
        # mas os valores padrão acima garantem robustez.

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