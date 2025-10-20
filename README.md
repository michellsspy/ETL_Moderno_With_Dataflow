# Pipeline de Dados de Hotelaria (Dataflow)

Este projeto implementa um pipeline de dados ETL em 3 camadas (Raw, Trusted, Refined) usando Apache Beam e Google Cloud Dataflow, empacotado como Flex Templates.

## Arquitetura de Camadas

O pipeline é orquestrado via Cloud Composer, que dispara Flex Templates do Dataflow.

### 1. Camada RAW (Batch)
* **Template:** `hotelaria_raw_pipeline`
* **Entrypoint:** `hotelaria_pipeline.raw.main_raw`
* **Lógica:**
    1.  Escaneia o GCS em `gs://{bucket_name}/transient/`.
    2.  Para cada pasta (ex: `cl_hotel_reservas`), encontra o CSV correspondente.
    3.  Carrega o CSV em uma tabela de *staging* no BigQuery.
    4.  Executa uma query `MERGE` (Upsert) da *staging* para a tabela final (ex: `raw_hotelaria.raw_hotel_reservas`) usando chaves primárias.
    5.  Exclui a tabela de *staging*.

### 2. Camada TRUSTED (Batch)
* **Template:** `hotelaria_trusted_pipeline`
* **Entrypoint:** `hotelaria_pipeline.trusted.main_trusted`
* **Lógica:**
    1.  Lê dados das tabelas `raw_` (ex: `raw_hotel_reservas`, `raw_hotel_hospedes`, `raw_hotel_hoteis`).
    2.  Realiza `LeftJoins` para enriquecer a fato (reservas) com as dimensões (hóspedes, hotéis).
    3.  Padroniza e valida os dados (ex: `UPPER()`, `LOWER()`, checagem de nulos).
    4.  Registros inválidos são enviados para uma tabela *dead-letter*.
    5.  Escreve os dados limpos e enriquecidos em `trusted_hotelaria.trusted_reservas_enriquecidas`.

### 3. Camada REFINED (Batch)
* **Template:** `hotelaria_refined_pipeline`
* **Entrypoint:** `hotelaria_pipeline.refined.main_refined`
* **Lógica:**
    1.  Lê a tabela `trusted_reservas_enriquecidas`.
    2.  Realiza agregações de negócios (ex: `SUM(receita)`, `COUNT(reservas)` por hotel, por dia).
    3.  Calcula KPIs (ex: Taxa de Ocupação - *placeholder*).
    4.  Escreve os dados agregados em `refined_hotelaria.refined_ocupacao_diaria`, prontos para consumo por BI (Looker).

## Como Buildar e Deployar (CI/CD)

O processo é automatizado pelo `cloudbuild.yaml`.

1.  **Testar:** `pytest tests/` é executado.
2.  **Buildar:** Uma imagem Docker (`gcr.io/$PROJECT_ID/hotelaria-pipeline:latest`) é criada com todo o código e dependências.
3.  **Deployar:** O Cloud Build executa `gcloud dataflow flex-template build` três vezes, publicando os três templates (`raw_`, `trusted_`, `refined_`) no GCS. Todos apontam para a mesma imagem, mas especificam `FLEX_TEMPLATE_PYTHON_PY_MODULE` diferentes.

## Como Executar (Orquestração)

Use o Cloud Composer (Airflow) com o `DataflowStartFlexTemplateOperator` para chamar os arquivos `.json` de template na ordem correta (Raw -> Trusted -> Refined).