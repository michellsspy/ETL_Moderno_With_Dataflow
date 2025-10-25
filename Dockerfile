FROM python:3.12-slim

# Copiando as dependências Beam e Template Launcher
COPY --from=apache/beam_python3.12_sdk:2.64.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# 2. Definir o diretório de trabalho
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

#ENV WORKDIR=/app
#WORKDIR ${WORKDIR}

# 3. (OPCIONAL) Instalar dependências de S.O.
RUN apt-get update && apt-get install -y libpq-dev
RUN pip install build setuptools wheel

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

# Entrypoint
ENTRYPOINT ["/opt/apache/beam/boot"]