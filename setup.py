import setuptools

setuptools.setup(
    name='dataflow-hotelaria-pipeline',
    version='1.0.0',
    description='Pipeline de dados em camadas (Raw, Trusted, Refined) para Hotelaria.',
    author='Parceiro de Desenvolvimento de Dados',
    
    # Encontrar automaticamente os pacotes
    packages=setuptools.find_packages(),
    
    install_requires=[
        # As dependências são gerenciadas pelo requirements.txt
        # e instaladas no Dockerfile, mas é boa prática listar aqui também.
        'apache-beam[gcp]>=2.64.0',
        'google-cloud-storage>=2.0.0',
        'google-cloud-bigquery>=3.0.0',
    ],
    
    # Informa ao Dataflow para não tentar instalar o pacote
    # no modo "clássico", já que usaremos --sdk_container_image
    py_modules=[],
)