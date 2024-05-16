
FROM apache/spark-py:latest

USER root

# Ajustar permissões
RUN chmod -R 755 /var/lib/apt/lists

# Remover arquivos de cache antigos
RUN rm -rf /var/lib/apt/lists/*

# Instalar Python 3.12
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-dev python3.12-distutils && \
    apt-get install -y curl && \
    apt-get install -y python3-pip && \
    apt-get install -y python3.12-venv && \
    apt-get install -y python3.12-distutils

# fazer o python3.12 o padrão
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# instalar requirements.txt usando o pip do python3.12
RUN python3 -m ensurepip --upgrade && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade setuptools

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt

WORKDIR /workspace

# dar permissao de leitura e escrita para a pasta de trabalho
RUN chmod -R 777 /workspace

# Expor a porta padrão do Jupyter Lab e portas do UI do Spark
EXPOSE 8888 4040 8080 18080

# Comando para iniciar o Jupyter Lab na pasta de trabalho /workspace
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--notebook-dir=/workspace"]
