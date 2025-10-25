# 1. Usar a imagem base oficial do Dataflow para Flex Templates
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# 2. Definir o diretório de trabalho
ENV WORKDIR=/app
WORKDIR ${WORKDIR}

# 3. (OPCIONAL) Instalar dependências de S.O.
# Ex: RUN apt-get update && apt-get install -y libpq-dev

# 4. Copiar e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar TODO o código-fonte do pipeline
COPY . .

# 6. Instalar o pacote (hotelaria_pipeline)
# Isso torna 'hotelaria_pipeline.raw.main_raw' etc. importáveis
RUN pip install .
RUN pip install -e .

# NENHUM 'ENV FLEX_TEMPLATE_PYTHON_PY_MODULE' aqui!
# Isso é definido pelo 'gcloud flex-template build' no cloudbuild.yaml