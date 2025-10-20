from google.cloud.bigquery import SchemaField

"""
Este módulo centraliza a definição de todos os esquemas de tabela do BigQuery
usados nos pipelines de dados.

- Esquemas RAW: Refletem a estrutura dos arquivos CSV de origem, incluindo
  as colunas de auditoria 'insert_date' e 'update_date' que o pipeline
  de UPSERT irá popular.
  
- Esquemas TRUSTED: Refletem as tabelas limpas, enriquecidas e modeladas.
- Esquemas REFINED: Refletem as tabelas agregadas, prontas para BI.
"""

# --- Camada RAW ---
# Esquemas explícitos para as tabelas de origem.

SCHEMA_RAW_CONSUMOS = [
    SchemaField("consumo_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_servico", "STRING", "NULLABLE"),
    SchemaField("data_consumo", "STRING", "NULLABLE"),
    SchemaField("quantidade", "INTEGER", "NULLABLE"),
    SchemaField("valor_total_consumo", "FLOAT", "NULLABLE"),
    SchemaField("hora_consumo", "STRING", "NULLABLE"),
    SchemaField("local_consumo", "STRING", "NULLABLE"),
    SchemaField("funcionario_responsavel", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_FATURAS = [
    SchemaField("fatura_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("data_emissao", "STRING", "NULLABLE"),
    SchemaField("data_vencimento", "STRING", "NULLABLE"),
    SchemaField("status_pagamento", "STRING", "NULLABLE"),
    SchemaField("forma_pagamento", "STRING", "NULLABLE"),
    SchemaField("subtotal_estadia", "FLOAT", "NULLABLE"),
    SchemaField("subtotal_consumos", "FLOAT", "NULLABLE"),
    SchemaField("descontos", "FLOAT", "NULLABLE"),
    SchemaField("impostos", "FLOAT", "NULLABLE"),
    SchemaField("valor_total", "FLOAT", "NULLABLE"),
    SchemaField("data_pagamento", "STRING", "NULLABLE"),
    SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"),
    SchemaField("taxa_turismo", "FLOAT", "NULLABLE"),
    SchemaField("taxa_servico", "FLOAT", "NULLABLE"),
    SchemaField("numero_transacao", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_HOSPEDES = [
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_completo", "STRING", "NULLABLE"),
    SchemaField("cpf", "STRING", "NULLABLE"),
    SchemaField("data_nascimento", "STRING", "NULLABLE"),
    SchemaField("email", "STRING", "NULLABLE"),
    SchemaField("telefone", "STRING", "NULLABLE"),
    SchemaField("estado", "STRING", "NULLABLE"),
    SchemaField("nacionalidade", "STRING", "NULLABLE"),
    SchemaField("data_cadastro", "STRING", "NULLABLE"),
    SchemaField("programa_fidelidade", "STRING", "NULLABLE"),
    SchemaField("profissao", "STRING", "NULLABLE"),
    SchemaField("tipo_documento", "STRING", "NULLABLE"),
    SchemaField("numero_documento", "STRING", "NULLABLE"),
    SchemaField("empresa", "STRING", "NULLABLE"),
    SchemaField("eh_viajante_frequente", "BOOLEAN", "NULLABLE"),
    SchemaField("preferencias_hospede", "STRING", "NULLABLE"),
    SchemaField("restricoes_alimentares", "STRING", "NULLABLE"),
    SchemaField("data_ultima_hospedagem", "STRING", "NULLABLE"),
    SchemaField("total_hospedagens", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_HOTEIS = [
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_hotel", "STRING", "NULLABLE"),
    SchemaField("endereco", "STRING", "NULLABLE"),
    SchemaField("cidade", "STRING", "NULLABLE"),
    SchemaField("estado", "STRING", "NULLABLE"),
    SchemaField("estrelas", "INTEGER", "NULLABLE"),
    SchemaField("numero_quartos", "INTEGER", "NULLABLE"),
    SchemaField("comodidades", "STRING", "NULLABLE"),
    SchemaField("telefone", "STRING", "NULLABLE"),
    SchemaField("email_contato", "STRING", "NULLABLE"),
    SchemaField("data_abertura", "STRING", "NULLABLE"),
    SchemaField("horario_checkin", "STRING", "NULLABLE"),
    SchemaField("horario_checkout", "STRING", "NULLABLE"),
    SchemaField("categoria_hotel", "STRING", "NULLABLE"),
    SchemaField("tipo_hotel", "STRING", "NULLABLE"),
    SchemaField("ano_fundacao", "INTEGER", "NULLABLE"),
    SchemaField("capacidade_total", "INTEGER", "NULLABLE"),
    SchemaField("possui_acessibilidade", "BOOLEAN", "NULLABLE"),
    SchemaField("certificacoes", "STRING", "NULLABLE"),
    SchemaField("latitude", "FLOAT", "NULLABLE"),
    SchemaField("longitude", "FLOAT", "NULLABLE"),
    SchemaField("descricao_hotel", "STRING", "NULLABLE"),
    SchemaField("numero_funcionarios", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_QUARTOS = [
    SchemaField("quarto_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("numero_quarto", "INTEGER", "NULLABLE"),
    SchemaField("tipo_quarto", "STRING", "NULLABLE"),
    SchemaField("capacidade_maxima", "INTEGER", "NULLABLE"),
    SchemaField("preco_diaria_base", "FLOAT", "NULLABLE"),
    SchemaField("andar", "INTEGER", "NULLABLE"),
    SchemaField("vista", "STRING", "NULLABLE"),
    SchemaField("comodidades_quarto", "STRING", "NULLABLE"),
    SchemaField("possui_ar_condicionado", "BOOLEAN", "NULLABLE"),
    SchemaField("tamanho_quarto", "STRING", "NULLABLE"),
    SchemaField("status_manutencao", "STRING", "NULLABLE"),
    SchemaField("ultima_manutencao", "STRING", "NULLABLE"),
    SchemaField("eh_smoke_free", "BOOLEAN", "NULLABLE"),
    SchemaField("possui_kit_boas_vindas", "BOOLEAN", "NULLABLE"),
    SchemaField("numero_camas", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_RESERVAS = [
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("quarto_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("data_reserva", "STRING", "NULLABLE"),
    SchemaField("data_checkin", "STRING", "NULLABLE"),
    SchemaField("data_checkout", "STRING", "NULLABLE"),
    SchemaField("numero_noites", "INTEGER", "NULLABLE"),
    SchemaField("numero_adultos", "INTEGER", "NULLABLE"),
    SchemaField("numero_criancas", "INTEGER", "NULLABLE"),
    SchemaField("canal_reserva", "STRING", "NULLABLE"),
    SchemaField("status_reserva", "STRING", "NULLABLE"),
    SchemaField("data_cancelamento", "STRING", "NULLABLE"), 
    SchemaField("solicitacoes_especiais", "STRING", "NULLABLE"),
    SchemaField("valor_total_estadia", "FLOAT", "NULLABLE"),
    SchemaField("motivo_viagem", "STRING", "NULLABLE"),
    SchemaField("motivo_cancelamento", "STRING", "NULLABLE"), 
    SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"),
    SchemaField("taxa_turismo", "FLOAT", "NULLABLE"),
    SchemaField("avaliacao_hospede", "FLOAT", "NULLABLE"),
    SchemaField("comentarios_hospede", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_RAW_RESERVAS_OTA = [
    SchemaField("ota_reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("ota_codigo_confirmacao", "STRING", "NULLABLE"),
    SchemaField("ota_nome_convidado", "STRING", "NULLABLE"),
    SchemaField("total_pago_ota", "FLOAT", "NULLABLE"),
    SchemaField("taxa_comissao", "FLOAT", "NULLABLE"),
    SchemaField("valor_liquido_recebido", "FLOAT", "NULLABLE"),
    SchemaField("ota_solicitacoes_especificas", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]


# --- Camada TRUSTED ---
# Tabela de FATO na camada Trusted, unindo reservas, hóspedes e hotéis.
# (Conforme definido anteriormente)
SCHEMA_TRUSTED_CONSUMOS = [
    SchemaField("consumo_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_servico", "STRING", "NULLABLE"),
    SchemaField("data_consumo", "STRING", "NULLABLE"),
    SchemaField("quantidade", "INTEGER", "NULLABLE"),
    SchemaField("valor_total_consumo", "FLOAT", "NULLABLE"),
    SchemaField("hora_consumo", "STRING", "NULLABLE"),
    SchemaField("local_consumo", "STRING", "NULLABLE"),
    SchemaField("funcionario_responsavel", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_FATURAS = [
    SchemaField("fatura_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("data_emissao", "STRING", "NULLABLE"),
    SchemaField("data_vencimento", "STRING", "NULLABLE"),
    SchemaField("status_pagamento", "STRING", "NULLABLE"),
    SchemaField("forma_pagamento", "STRING", "NULLABLE"),
    SchemaField("subtotal_estadia", "FLOAT", "NULLABLE"),
    SchemaField("subtotal_consumos", "FLOAT", "NULLABLE"),
    SchemaField("descontos", "FLOAT", "NULLABLE"),
    SchemaField("impostos", "FLOAT", "NULLABLE"),
    SchemaField("valor_total", "FLOAT", "NULLABLE"),
    SchemaField("data_pagamento", "STRING", "NULLABLE"),
    SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"),
    SchemaField("taxa_turismo", "FLOAT", "NULLABLE"),
    SchemaField("taxa_servico", "FLOAT", "NULLABLE"),
    SchemaField("numero_transacao", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_HOSPEDES = [
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_completo", "STRING", "NULLABLE"),
    SchemaField("cpf", "STRING", "NULLABLE"),
    SchemaField("data_nascimento", "STRING", "NULLABLE"),
    SchemaField("email", "STRING", "NULLABLE"),
    SchemaField("telefone", "STRING", "NULLABLE"),
    SchemaField("estado", "STRING", "NULLABLE"),
    SchemaField("nacionalidade", "STRING", "NULLABLE"),
    SchemaField("data_cadastro", "STRING", "NULLABLE"),
    SchemaField("programa_fidelidade", "STRING", "NULLABLE"),
    SchemaField("profissao", "STRING", "NULLABLE"),
    SchemaField("tipo_documento", "STRING", "NULLABLE"),
    SchemaField("numero_documento", "STRING", "NULLABLE"),
    SchemaField("empresa", "STRING", "NULLABLE"),
    SchemaField("eh_viajante_frequente", "BOOLEAN", "NULLABLE"),
    SchemaField("preferencias_hospede", "STRING", "NULLABLE"),
    SchemaField("restricoes_alimentares", "STRING", "NULLABLE"),
    SchemaField("data_ultima_hospedagem", "STRING", "NULLABLE"),
    SchemaField("total_hospedagens", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_HOTEIS = [
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("nome_hotel", "STRING", "NULLABLE"),
    SchemaField("endereco", "STRING", "NULLABLE"),
    SchemaField("cidade", "STRING", "NULLABLE"),
    SchemaField("estado", "STRING", "NULLABLE"),
    SchemaField("estrelas", "INTEGER", "NULLABLE"),
    SchemaField("numero_quartos", "INTEGER", "NULLABLE"),
    SchemaField("comodidades", "STRING", "NULLABLE"),
    SchemaField("telefone", "STRING", "NULLABLE"),
    SchemaField("email_contato", "STRING", "NULLABLE"),
    SchemaField("data_abertura", "STRING", "NULLABLE"),
    SchemaField("horario_checkin", "STRING", "NULLABLE"),
    SchemaField("horario_checkout", "STRING", "NULLABLE"),
    SchemaField("categoria_hotel", "STRING", "NULLABLE"),
    SchemaField("tipo_hotel", "STRING", "NULLABLE"),
    SchemaField("ano_fundacao", "INTEGER", "NULLABLE"),
    SchemaField("capacidade_total", "INTEGER", "NULLABLE"),
    SchemaField("possui_acessibilidade", "BOOLEAN", "NULLABLE"),
    SchemaField("certificacoes", "STRING", "NULLABLE"),
    SchemaField("latitude", "FLOAT", "NULLABLE"),
    SchemaField("longitude", "FLOAT", "NULLABLE"),
    SchemaField("descricao_hotel", "STRING", "NULLABLE"),
    SchemaField("numero_funcionarios", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_QUARTOS = [
    SchemaField("quarto_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("numero_quarto", "INTEGER", "NULLABLE"),
    SchemaField("tipo_quarto", "STRING", "NULLABLE"),
    SchemaField("capacidade_maxima", "INTEGER", "NULLABLE"),
    SchemaField("preco_diaria_base", "FLOAT", "NULLABLE"),
    SchemaField("andar", "INTEGER", "NULLABLE"),
    SchemaField("vista", "STRING", "NULLABLE"),
    SchemaField("comodidades_quarto", "STRING", "NULLABLE"),
    SchemaField("possui_ar_condicionado", "BOOLEAN", "NULLABLE"),
    SchemaField("tamanho_quarto", "STRING", "NULLABLE"),
    SchemaField("status_manutencao", "STRING", "NULLABLE"),
    SchemaField("ultima_manutencao", "STRING", "NULLABLE"),
    SchemaField("eh_smoke_free", "BOOLEAN", "NULLABLE"),
    SchemaField("possui_kit_boas_vindas", "BOOLEAN", "NULLABLE"),
    SchemaField("numero_camas", "INTEGER", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_RESERVAS = [
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("hospede_id", "INTEGER", "NULLABLE"),
    SchemaField("quarto_id", "INTEGER", "NULLABLE"),
    SchemaField("hotel_id", "INTEGER", "NULLABLE"),
    SchemaField("data_reserva", "STRING", "NULLABLE"),
    SchemaField("data_checkin", "STRING", "NULLABLE"),
    SchemaField("data_checkout", "STRING", "NULLABLE"),
    SchemaField("numero_noites", "INTEGER", "NULLABLE"),
    SchemaField("numero_adultos", "INTEGER", "NULLABLE"),
    SchemaField("numero_criancas", "INTEGER", "NULLABLE"),
    SchemaField("canal_reserva", "STRING", "NULLABLE"),
    SchemaField("status_reserva", "STRING", "NULLABLE"),
    SchemaField("data_cancelamento", "STRING", "NULLABLE"), # CORRIGIDO: Era FLOAT
    SchemaField("solicitacoes_especiais", "STRING", "NULLABLE"),
    SchemaField("valor_total_estadia", "FLOAT", "NULLABLE"),
    SchemaField("motivo_viagem", "STRING", "NULLABLE"),
    SchemaField("motivo_cancelamento", "STRING", "NULLABLE"), # CORRIGIDO: Era FLOAT
    SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"),
    SchemaField("taxa_turismo", "FLOAT", "NULLABLE"),
    SchemaField("avaliacao_hospede", "FLOAT", "NULLABLE"),
    SchemaField("comentarios_hospede", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

SCHEMA_TRUSTED_RESERVAS_OTA = [
    SchemaField("ota_reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("reserva_id", "INTEGER", "NULLABLE"),
    SchemaField("ota_codigo_confirmacao", "STRING", "NULLABLE"),
    SchemaField("ota_nome_convidado", "STRING", "NULLABLE"),
    SchemaField("total_pago_ota", "FLOAT", "NULLABLE"),
    SchemaField("taxa_comissao", "FLOAT", "NULLABLE"),
    SchemaField("valor_liquido_recebido", "FLOAT", "NULLABLE"),
    SchemaField("ota_solicitacoes_especificas", "STRING", "NULLABLE"),
    SchemaField("insert_date", "DATETIME", "NULLABLE"),
    SchemaField("update_date", "DATETIME", "NULLABLE"),
]

# --- Camada REFINED ---
# Tabela agregada para um dashboard de BI.
# (Conforme definido anteriormente)
SCHEMA_REFINED_OCUPACAO_DIARIA = [
    SchemaField("id_hotel", "STRING", "REQUIRED"),
    SchemaField("nome_hotel", "STRING"),
    SchemaField("cidade_hotel", "STRING"),
    SchemaField("data_referencia", "DATE", "REQUIRED"),
    SchemaField("receita_total_dia", "NUMERIC", "REQUIRED"), # Corrigido de SchemaMessageField
    SchemaField("reservas_confirmadas_dia", "INTEGER", "REQUIRED"),
    SchemaField("taxa_ocupacao_dia", "NUMERIC"), 
    SchemaField("dt_processamento", "DATE", "REQUIRED"),
]